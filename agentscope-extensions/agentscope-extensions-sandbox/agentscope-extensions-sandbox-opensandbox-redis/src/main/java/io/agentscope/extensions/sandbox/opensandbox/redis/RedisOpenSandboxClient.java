/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.agentscope.extensions.sandbox.opensandbox.redis;

import io.agentscope.extensions.sandbox.opensandbox.OpenSandbox;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClient;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClientOptions;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxState;
import io.agentscope.harness.agent.sandbox.Sandbox;
import io.agentscope.harness.agent.sandbox.SandboxClient;
import io.agentscope.harness.agent.sandbox.SandboxException;
import io.agentscope.harness.agent.sandbox.SandboxIsolationKey;
import io.agentscope.harness.agent.sandbox.SandboxState;
import io.agentscope.harness.agent.sandbox.WorkspaceSpec;
import io.agentscope.harness.agent.sandbox.snapshot.NoopSnapshotSpec;
import io.agentscope.harness.agent.sandbox.snapshot.SandboxSnapshotSpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** OpenSandbox client with Redis-coordinated workspace reuse across JVM instances. */
public final class RedisOpenSandboxClient
        implements SandboxClient<OpenSandboxClientOptions>, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RedisOpenSandboxClient.class);
    private static final int SCHEMA_VERSION = 1;

    private final OpenSandboxClient delegate;
    private final OpenSandboxWorkspaceStore store;
    private final OpenSandboxClientOptions defaultOptions;
    private final OpenSandboxRedisLifecycleOptions lifecycle;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final String instanceId;
    private final boolean ownsScheduler;

    public RedisOpenSandboxClient(
            RedissonClient redisson,
            OpenSandboxClientOptions options,
            OpenSandboxRedisLifecycleOptions lifecycle) {
        this(
                new OpenSandboxClient(optionsOrDefault(options), null),
                new OpenSandboxWorkspaceStore(redisson),
                optionsOrDefault(options),
                lifecycle == null ? new OpenSandboxRedisLifecycleOptions() : lifecycle,
                Clock.systemUTC(),
                newScheduler(),
                UUID.randomUUID().toString(),
                true);
    }

    public RedisOpenSandboxClient(RedissonClient redisson, OpenSandboxClientOptions options) {
        this(redisson, options, new OpenSandboxRedisLifecycleOptions());
    }

    RedisOpenSandboxClient(
            OpenSandboxClient delegate,
            OpenSandboxWorkspaceStore store,
            OpenSandboxClientOptions options,
            OpenSandboxRedisLifecycleOptions lifecycle,
            Clock clock,
            ScheduledExecutorService scheduler,
            String instanceId) {
        this(delegate, store, options, lifecycle, clock, scheduler, instanceId, false);
    }

    private RedisOpenSandboxClient(
            OpenSandboxClient delegate,
            OpenSandboxWorkspaceStore store,
            OpenSandboxClientOptions options,
            OpenSandboxRedisLifecycleOptions lifecycle,
            Clock clock,
            ScheduledExecutorService scheduler,
            String instanceId,
            boolean ownsScheduler) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.store = Objects.requireNonNull(store, "store");
        this.defaultOptions = OpenSandboxClientOptions.copyOf(optionsOrDefault(options));
        this.lifecycle = OpenSandboxRedisLifecycleOptions.copyOf(lifecycle);
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.instanceId = requireText(instanceId, "instanceId");
        this.ownsScheduler = ownsScheduler;
    }

    @Override
    public Sandbox create(
            WorkspaceSpec workspaceSpec,
            SandboxSnapshotSpec snapshotSpec,
            OpenSandboxClientOptions options) {
        throw missingIdentity();
    }

    @Override
    public Sandbox create(
            WorkspaceSpec workspaceSpec,
            SandboxSnapshotSpec snapshotSpec,
            OpenSandboxClientOptions options,
            SandboxIsolationKey isolationKey,
            String agentId) {
        rejectTarSnapshot(snapshotSpec);
        return borrow(
                workspaceSpec == null ? new WorkspaceSpec() : workspaceSpec.copy(),
                options == null ? defaultOptions : options,
                Objects.requireNonNull(isolationKey, "isolationKey"),
                requireText(agentId, "agentId"));
    }

    @Override
    public Sandbox resume(SandboxState state) {
        throw missingIdentity();
    }

    @Override
    public Sandbox resume(SandboxState state, SandboxIsolationKey isolationKey, String agentId) {
        if (!(state instanceof OpenSandboxState openSandboxState)) {
            throw new SandboxException.SandboxConfigurationException(
                    "Redis OpenSandbox requires OpenSandboxState on resume");
        }
        WorkspaceSpec spec =
                openSandboxState.getWorkspaceSpec() == null
                        ? new WorkspaceSpec()
                        : openSandboxState.getWorkspaceSpec().copy();
        return borrow(
                spec,
                defaultOptions,
                Objects.requireNonNull(isolationKey, "isolationKey"),
                requireText(agentId, "agentId"));
    }

    @Override
    public void delete(Sandbox sandbox) {
        if (!(sandbox instanceof RedisManagedOpenSandbox managed) || managed.owner() != this) {
            throw new SandboxException.SandboxConfigurationException(
                    "Sandbox was not created by this RedisOpenSandboxClient");
        }
        Exception failure = null;
        try {
            managed.stop();
        } catch (Exception error) {
            failure = error;
        }
        try {
            withLock(
                    managed.workspaceId(),
                    () -> {
                        OpenSandboxWorkspaceRecord current =
                                store.load(managed.workspaceId()).orElse(null);
                        delegate.delete(managed.delegate());
                        if (current != null && !store.compareAndSet(current, null)) {
                            throw new SandboxException.SandboxRuntimeException(
                                    "OpenSandbox record changed while deleting", null);
                        }
                        store.clearLeases(managed.workspaceId());
                        store.cancelIdle(managed.workspaceId());
                        return null;
                    });
        } catch (Exception error) {
            if (failure == null) {
                failure = error;
            } else {
                failure.addSuppressed(error);
            }
        }
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure != null) {
            throw new SandboxException.SandboxRuntimeException(
                    "Failed to delete Redis-managed OpenSandbox", failure);
        }
    }

    @Override
    public String serializeState(SandboxState state) {
        return delegate.serializeState(state);
    }

    @Override
    public SandboxState deserializeState(String json) {
        return delegate.deserializeState(json);
    }

    ScheduledFuture<?> startHeartbeat(
            OpenSandboxActiveLease lease, Object monitor, BooleanSupplier active) {
        Objects.requireNonNull(monitor, "monitor");
        Objects.requireNonNull(active, "active");
        synchronized (monitor) {
            if (!active.getAsBoolean()) {
                return null;
            }
            heartbeat(lease);
            long interval = Math.max(1, lifecycle.getHeartbeatInterval().toMillis());
            return scheduler.scheduleAtFixedRate(
                    () -> {
                        synchronized (monitor) {
                            if (!active.getAsBoolean()) {
                                return;
                            }
                            try {
                                heartbeat(lease);
                            } catch (RuntimeException error) {
                                log.warn(
                                        "[sandbox-opensandbox-redis] Lease heartbeat failed for {}",
                                        lease.workspaceId(),
                                        error);
                            }
                        }
                    },
                    interval,
                    interval,
                    TimeUnit.MILLISECONDS);
        }
    }

    void release(RedisManagedOpenSandbox managed) throws Exception {
        OpenSandboxActiveLease lease = managed.lease();
        withLock(
                lease.workspaceId(),
                () -> {
                    OpenSandboxWorkspaceRecord current =
                            store.load(lease.workspaceId()).orElse(null);
                    store.removeLease(lease.workspaceId(), lease.leaseId());
                    if (current == null || current.getGeneration() != lease.generation()) {
                        return null;
                    }
                    OpenSandboxWorkspaceRecord update = current.copy();
                    update.setLastAccessAt(clock.instant());
                    update.setUpdatedAt(clock.instant());
                    update.setDirty(true);
                    if (!store.compareAndSet(current, update)) {
                        throw new SandboxException.SandboxRuntimeException(
                                "OpenSandbox record changed while releasing", null);
                    }
                    if (store.activeLeases(lease.workspaceId(), lease.generation()).isEmpty()) {
                        store.scheduleIdle(
                                lease.workspaceId(), clock.instant().plus(lifecycle.getIdleTtl()));
                    } else {
                        store.cancelIdle(lease.workspaceId());
                    }
                    return null;
                });
    }

    static String workspaceId(SandboxIsolationKey isolationKey, String agentId) {
        Objects.requireNonNull(isolationKey, "isolationKey");
        String canonical =
                switch (isolationKey.getScope()) {
                    case USER ->
                            "v1\0user\0"
                                    + requireText(agentId, "agentId")
                                    + "\0"
                                    + requireText(isolationKey.getValue(), "isolation value");
                    case SESSION ->
                            "v1\0session\0"
                                    + requireText(agentId, "agentId")
                                    + "\0"
                                    + requireText(isolationKey.getValue(), "isolation value");
                    case AGENT -> "v1\0agent\0" + requireText(agentId, "agentId");
                    case GLOBAL -> "v1\0global";
                };
        return digest(canonical);
    }

    static String runtimeProfileHash(OpenSandboxClientOptions options) {
        Objects.requireNonNull(options, "options");
        StringBuilder canonical = new StringBuilder("v1\0image\0").append(options.getImage());
        for (String entry : options.getEntrypoint()) {
            canonical.append("\0entrypoint\0").append(entry);
        }
        return digest(canonical.toString());
    }

    @Override
    public void close() {
        if (ownsScheduler) {
            scheduler.shutdownNow();
        }
    }

    private RedisManagedOpenSandbox borrow(
            WorkspaceSpec spec,
            OpenSandboxClientOptions requestedOptions,
            SandboxIsolationKey isolationKey,
            String agentId) {
        OpenSandboxClientOptions options = OpenSandboxClientOptions.copyOf(requestedOptions);
        ensureControlPlaneMatchesDefaults(options);
        String workspaceId = workspaceId(isolationKey, agentId);
        try {
            return withLock(
                    workspaceId,
                    () -> {
                        OpenSandboxWorkspaceRecord current = store.load(workspaceId).orElse(null);
                        OpenSandbox remote;
                        long generation;
                        if (current == null) {
                            generation = 1;
                            remote = createRemote(spec, options, workspaceId, generation, null);
                        } else {
                            ensureProfileMatches(current, options);
                            generation = current.getGeneration();
                            remote = connectOrRebuild(current, spec, options, workspaceId);
                            if (!Objects.equals(
                                    current.getSandboxId(),
                                    ((OpenSandboxState) remote.getState()).getSandboxId())) {
                                generation = current.getGeneration() + 1;
                            }
                        }
                        OpenSandboxWorkspaceRecord update =
                                runningRecord(
                                        current,
                                        remote,
                                        workspaceId,
                                        isolationKey,
                                        agentId,
                                        options,
                                        generation);
                        if (!store.compareAndSet(current, update)) {
                            remote.disconnect();
                            throw new SandboxException.SandboxRuntimeException(
                                    "OpenSandbox record changed while borrowing", null);
                        }
                        OpenSandboxActiveLease lease =
                                new OpenSandboxActiveLease(
                                        UUID.randomUUID().toString(),
                                        workspaceId,
                                        generation,
                                        instanceId,
                                        clock.instant(),
                                        clock.instant());
                        try {
                            store.putLease(lease, lifecycle.getActiveLeaseTtl());
                            store.cancelIdle(workspaceId);
                        } catch (RuntimeException error) {
                            remote.disconnect();
                            throw error;
                        }
                        return new RedisManagedOpenSandbox(this, remote, lease);
                    });
        } catch (RuntimeException error) {
            throw error;
        } catch (Exception error) {
            throw new SandboxException.SandboxRuntimeException(
                    "Failed to borrow Redis-managed OpenSandbox", error);
        }
    }

    private OpenSandbox connectOrRebuild(
            OpenSandboxWorkspaceRecord current,
            WorkspaceSpec spec,
            OpenSandboxClientOptions options,
            String workspaceId)
            throws Exception {
        if (current.getSandboxId() == null || current.getSandboxId().isBlank()) {
            return rebuild(current, spec, options, workspaceId, true);
        }
        OpenSandboxState info;
        try {
            info = delegate.describe(current.getSandboxId());
        } catch (RuntimeException error) {
            if (!delegate.isNotFound(error)) {
                throw error;
            }
            return rebuild(current, spec, options, workspaceId, true);
        }
        if ("PAUSED".equalsIgnoreCase(info.getRemoteStatus())) {
            try {
                delegate.resumeRemote(current.getSandboxId());
            } catch (RuntimeException error) {
                if (!delegate.isNotFound(error)) {
                    throw error;
                }
                return rebuild(current, spec, options, workspaceId, false);
            }
        }
        SandboxState deserialized = delegate.deserializeState(current.getSerializedSandboxState());
        if (!(deserialized instanceof OpenSandboxState persisted)) {
            throw new SandboxException.SandboxConfigurationException(
                    "Redis record does not contain OpenSandboxState");
        }
        persisted.setWorkspaceSpec(spec.copy());
        OpenSandbox remote = requireOpenSandbox(delegate.resume(persisted));
        String expectedId = persisted.getSandboxId();
        try {
            remote.startExisting();
        } catch (Exception error) {
            if (!delegate.isNotFound(error)) {
                throw error;
            }
            return rebuild(current, spec, options, workspaceId, false);
        }
        if (!Objects.equals(expectedId, ((OpenSandboxState) remote.getState()).getSandboxId())) {
            remote.disconnect();
            throw new SandboxException.SandboxRuntimeException(
                    "OpenSandbox disappeared during borrow; retry reconciliation", null);
        }
        return remote;
    }

    private OpenSandbox rebuild(
            OpenSandboxWorkspaceRecord current,
            WorkspaceSpec spec,
            OpenSandboxClientOptions options,
            String workspaceId,
            boolean allowImageCreate)
            throws Exception {
        long generation = current.getGeneration() + 1;
        List<String> snapshots = new ArrayList<>();
        if (current.getNativeSnapshotId() != null) {
            snapshots.add(current.getNativeSnapshotId());
        }
        if (current.getPreviousNativeSnapshotId() != null
                && !snapshots.contains(current.getPreviousNativeSnapshotId())) {
            snapshots.add(current.getPreviousNativeSnapshotId());
        }
        if (snapshots.isEmpty()) {
            if (!allowImageCreate) {
                throw new SandboxException.SandboxRuntimeException(
                        "OpenSandbox disappeared during borrow and no native snapshot is available",
                        null);
            }
            return createRemote(spec, options, workspaceId, generation, null);
        }
        Exception failure = null;
        for (String snapshotId : snapshots) {
            try {
                return createRemote(spec, options, workspaceId, generation, snapshotId);
            } catch (Exception error) {
                if (failure == null) {
                    failure = error;
                } else {
                    failure.addSuppressed(error);
                }
            }
        }
        throw new SandboxException.SandboxRuntimeException(
                "All native snapshot restore attempts failed; refusing empty workspace", failure);
    }

    private OpenSandbox createRemote(
            WorkspaceSpec spec,
            OpenSandboxClientOptions requested,
            String workspaceId,
            long generation,
            String snapshotId)
            throws Exception {
        OpenSandboxClientOptions options = OpenSandboxClientOptions.copyOf(requested);
        options.setRestoreSnapshotId(snapshotId);
        Map<String, String> metadata = new LinkedHashMap<>(options.getMetadata());
        metadata.put("agentscope.owner", "opensandbox-redis");
        metadata.put("agentscope.workspace-id", workspaceId);
        metadata.put("agentscope.generation", Long.toString(generation));
        metadata.put("agentscope.schema-version", Integer.toString(SCHEMA_VERSION));
        options.setMetadata(metadata);
        OpenSandbox remote = requireOpenSandbox(delegate.create(spec, null, options));
        try {
            remote.start();
            return remote;
        } catch (Exception error) {
            try {
                delegate.delete(remote);
            } catch (Exception cleanup) {
                if (cleanup != error) {
                    error.addSuppressed(cleanup);
                }
            }
            throw error;
        }
    }

    private OpenSandboxWorkspaceRecord runningRecord(
            OpenSandboxWorkspaceRecord current,
            OpenSandbox remote,
            String workspaceId,
            SandboxIsolationKey isolationKey,
            String agentId,
            OpenSandboxClientOptions options,
            long generation) {
        OpenSandboxWorkspaceRecord update =
                current == null ? new OpenSandboxWorkspaceRecord() : current.copy();
        OpenSandboxState state = (OpenSandboxState) remote.getState();
        update.setSchemaVersion(SCHEMA_VERSION);
        update.setWorkspaceId(workspaceId);
        update.setIsolationScope(isolationKey.getScope().name());
        update.setAgentId(agentId);
        update.setSandboxId(state.getSandboxId());
        update.setSerializedSandboxState(delegate.serializeState(state));
        update.setRuntimeImage(options.getImage());
        update.setRuntimeProfileHash(runtimeProfileHash(options));
        update.setLifecycleState(OpenSandboxWorkspaceRecord.LifecycleState.RUNNING);
        update.setGeneration(generation);
        update.setLastAccessAt(clock.instant());
        update.setPausedAt(null);
        update.setExpiresAt(state.getRemoteExpiresAt());
        update.setEvictionCandidateAt(null);
        update.setDirty(true);
        update.setLastError(null);
        update.setUpdatedAt(clock.instant());
        return update;
    }

    private void heartbeat(OpenSandboxActiveLease lease) {
        try {
            withLock(
                    lease.workspaceId(),
                    () -> {
                        OpenSandboxWorkspaceRecord current =
                                store.load(lease.workspaceId()).orElse(null);
                        if (current == null || current.getGeneration() != lease.generation()) {
                            store.removeLease(lease.workspaceId(), lease.leaseId());
                            return null;
                        }
                        Instant now = clock.instant();
                        store.putLease(
                                new OpenSandboxActiveLease(
                                        lease.leaseId(),
                                        lease.workspaceId(),
                                        lease.generation(),
                                        lease.ownerInstanceId(),
                                        lease.startedAt(),
                                        now),
                                lifecycle.getActiveLeaseTtl());
                        store.cancelIdle(lease.workspaceId());
                        return null;
                    });
        } catch (RuntimeException error) {
            throw error;
        } catch (Exception error) {
            throw new SandboxException.SandboxRuntimeException(
                    "Failed to heartbeat Redis-managed OpenSandbox", error);
        }
    }

    private void ensureProfileMatches(
            OpenSandboxWorkspaceRecord record, OpenSandboxClientOptions options) {
        if (record.getRuntimeProfileHash() != null
                && !record.getRuntimeProfileHash().equals(runtimeProfileHash(options))) {
            throw new SandboxException.SandboxConfigurationException(
                    "OpenSandbox runtime profile differs from the existing workspace; delete the"
                            + " workspace before changing image or entrypoint");
        }
    }

    private void ensureControlPlaneMatchesDefaults(OpenSandboxClientOptions options) {
        String effectiveApiKey =
                options.getApiKey() == null ? defaultOptions.getApiKey() : options.getApiKey();
        if (!Objects.equals(defaultOptions.getEndpoint(), options.getEndpoint())
                || !Objects.equals(defaultOptions.getApiKey(), effectiveApiKey)) {
            throw new SandboxException.SandboxConfigurationException(
                    "Redis OpenSandbox endpoint and API key are client-level settings because"
                            + " background lifecycle operations require stable credentials");
        }
    }

    private <T> T withLock(String workspaceId, LockedOperation<T> operation) throws Exception {
        RLock lock = store.lifecycleLock(workspaceId);
        boolean acquired;
        try {
            acquired =
                    lock.tryLock(
                            Math.max(1, lifecycle.getLockWait().toMillis()), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new SandboxException.SandboxRuntimeException(
                    "Interrupted while waiting for OpenSandbox lifecycle lock", interrupted);
        }
        if (!acquired) {
            throw new SandboxException.SandboxRuntimeException(
                    "Timed out waiting for OpenSandbox lifecycle lock", null);
        }
        try {
            return operation.run();
        } finally {
            lock.unlock();
        }
    }

    private static void rejectTarSnapshot(SandboxSnapshotSpec snapshotSpec) {
        if (snapshotSpec != null && !(snapshotSpec instanceof NoopSnapshotSpec)) {
            throw new SandboxException.SandboxConfigurationException(
                    "Redis OpenSandbox uses native snapshots; tar snapshot specs are unsupported");
        }
    }

    private static SandboxException.SandboxConfigurationException missingIdentity() {
        return new SandboxException.SandboxConfigurationException(
                "Redis OpenSandbox requires a resolved SandboxIsolationKey and agentId");
    }

    private static OpenSandbox requireOpenSandbox(Sandbox sandbox) {
        if (!(sandbox instanceof OpenSandbox openSandbox)) {
            throw new SandboxException.SandboxConfigurationException(
                    "OpenSandboxClient returned an incompatible sandbox");
        }
        return openSandbox;
    }

    private static OpenSandboxClientOptions optionsOrDefault(OpenSandboxClientOptions options) {
        return options == null
                ? new OpenSandboxClientOptions()
                : OpenSandboxClientOptions.copyOf(options);
    }

    private static String digest(String canonical) {
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(canonical.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static ScheduledExecutorService newScheduler() {
        AtomicInteger number = new AtomicInteger();
        ThreadFactory factory =
                runnable -> {
                    Thread thread =
                            new Thread(
                                    runnable,
                                    "agentscope-opensandbox-redis-heartbeat-"
                                            + number.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                };
        return Executors.newScheduledThreadPool(2, factory);
    }

    @FunctionalInterface
    private interface LockedOperation<T> {
        T run() throws Exception;
    }
}
