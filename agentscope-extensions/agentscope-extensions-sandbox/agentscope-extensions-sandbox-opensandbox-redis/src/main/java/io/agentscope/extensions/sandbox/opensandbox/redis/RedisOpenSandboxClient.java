/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.agentscope.extensions.sandbox.opensandbox.redis;

import io.agentscope.extensions.sandbox.opensandbox.OpenSandbox;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClient;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClient.NativeSnapshot;
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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
    private static final int SCHEMA_VERSION = OpenSandboxWorkspaceRecord.CURRENT_SCHEMA_VERSION;

    private final OpenSandboxClient delegate;
    private final OpenSandboxWorkspaceStore store;
    private final OpenSandboxClientOptions defaultOptions;
    private final OpenSandboxRedisLifecycleOptions lifecycle;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final String instanceId;
    private final boolean ownsScheduler;
    private final OpenSandboxLifecycleSweeper sweeper;

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
        if (ownsScheduler && this.lifecycle.isSweeperEnabled()) {
            this.sweeper =
                    new OpenSandboxLifecycleSweeper(
                            this.delegate, this.store, this.lifecycle, this.clock, this.scheduler);
            this.sweeper.start();
        } else {
            this.sweeper = null;
        }
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
        try {
            withLock(
                    managed.workspaceId(),
                    () -> {
                        OpenSandboxWorkspaceRecord current =
                                store.load(managed.workspaceId()).orElse(null);
                        if (current != null) {
                            validateGeneration(current.getGeneration());
                        }
                        List<OpenSandboxState> discovered =
                                delegate.listByMetadata(workspaceIdentity(managed.workspaceId()));
                        Map<String, NativeSnapshot> discoveredSnapshots =
                                delegate.listNativeSnapshotDetailsByNamePrefix(
                                        OpenSandboxLifecycleSweeper.snapshotNamePrefix(
                                                managed.workspaceId()));
                        long generation =
                                Math.max(
                                        managed.lease().generation(),
                                        current == null ? 0 : current.getGeneration());
                        for (OpenSandboxState state : discovered) {
                            generation = Math.max(generation, metadataGeneration(state));
                        }
                        for (NativeSnapshot snapshot : discoveredSnapshots.values()) {
                            generation =
                                    Math.max(
                                            generation,
                                            snapshotGeneration(
                                                    managed.workspaceId(), snapshot.name()));
                        }
                        long deletedThrough =
                                store.advanceDeletedThroughGeneration(
                                        managed.workspaceId(), generation);
                        validateGeneration(deletedThrough);
                        deleteLocked(managed, discovered, discoveredSnapshots, deletedThrough);
                        return null;
                    });
        } catch (RuntimeException error) {
            throw error;
        } catch (Exception error) {
            throw new SandboxException.SandboxRuntimeException(
                    "Failed to delete Redis-managed OpenSandbox", error);
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
                    Instant now = clock.instant();
                    store.scheduleRepair(lease.workspaceId(), now);
                    OpenSandboxWorkspaceRecord current =
                            store.load(lease.workspaceId()).orElse(null);
                    if (current != null) {
                        validateGeneration(current.getGeneration());
                    }
                    if (current == null || current.getGeneration() != lease.generation()) {
                        store.removeLease(lease.workspaceId(), lease.leaseId());
                        return null;
                    }
                    List<OpenSandboxActiveLease> remaining =
                            store.activeLeases(lease.workspaceId(), lease.generation()).stream()
                                    .filter(active -> !active.leaseId().equals(lease.leaseId()))
                                    .toList();
                    OpenSandboxWorkspaceRecord update = current.copy();
                    update.setLastAccessAt(now);
                    update.setUpdatedAt(now);
                    update.setDirty(true);
                    if (remaining.isEmpty()) {
                        update.setExpiresAt(
                                delegate.renew(current.getSandboxId(), idleRetention(lifecycle)));
                    }
                    if (!store.compareAndSet(current, update)) {
                        throw new SandboxException.SandboxRuntimeException(
                                "OpenSandbox record changed while releasing", null);
                    }
                    store.removeLease(lease.workspaceId(), lease.leaseId());
                    if (remaining.isEmpty()) {
                        store.scheduleIdle(lease.workspaceId(), now.plus(lifecycle.getIdleTtl()));
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

    static Duration idleRetention(OpenSandboxRedisLifecycleOptions lifecycle) {
        Objects.requireNonNull(lifecycle, "lifecycle");
        return lifecycle.getIdleTtl().plus(evictionRetention(lifecycle));
    }

    static Duration evictionRetention(OpenSandboxRedisLifecycleOptions lifecycle) {
        Objects.requireNonNull(lifecycle, "lifecycle");
        return lifecycle
                .getEvictionGrace()
                .plus(lifecycle.getSweepInterval().multipliedBy(2))
                .plus(
                        lifecycle
                                .getSnapshotReadyTimeout()
                                .multipliedBy(
                                        (OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE
                                                        + OpenSandboxLifecycleSweeper
                                                                .SNAPSHOT_CONCURRENCY
                                                        - 1)
                                                / OpenSandboxLifecycleSweeper
                                                        .SNAPSHOT_CONCURRENCY));
    }

    static Duration snapshotAttemptRetention(OpenSandboxRedisLifecycleOptions lifecycle) {
        Objects.requireNonNull(lifecycle, "lifecycle");
        return lifecycle
                .getSnapshotReadyTimeout()
                .plus(lifecycle.getSweepInterval().multipliedBy(2));
    }

    @Override
    public void close() {
        if (sweeper != null) {
            sweeper.close();
        } else if (ownsScheduler) {
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
                        if (current != null) {
                            validateGeneration(current.getGeneration());
                        }
                        long deletedThrough = store.deletedThroughGeneration(workspaceId);
                        validateGeneration(deletedThrough);
                        if (current != null && current.getGeneration() <= deletedThrough) {
                            markDeletedRecordOrphans(current);
                            if (!store.compareAndSet(current, null)) {
                                throw new SandboxException.SandboxRuntimeException(
                                        "OpenSandbox record changed while applying delete fence",
                                        null);
                            }
                            current = null;
                        }
                        OpenSandbox remote;
                        long generation;
                        String recoveredCurrentSnapshot = null;
                        String recoveredPreviousSnapshot = null;
                        if (current == null) {
                            MissingRecordRecovery recovery =
                                    recoverMissingRecord(
                                            spec, options, workspaceId, deletedThrough);
                            generation = recovery.generation();
                            remote = recovery.remote();
                            recoveredCurrentSnapshot = recovery.currentSnapshotId();
                            recoveredPreviousSnapshot = recovery.previousSnapshotId();
                        } else {
                            ensureProfileMatches(current, options);
                            generation = current.getGeneration();
                            remote = connectOrRebuild(current, spec, options, workspaceId);
                            if (!Objects.equals(
                                    current.getSandboxId(),
                                    ((OpenSandboxState) remote.getState()).getSandboxId())) {
                                generation = nextGeneration(current.getGeneration());
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
                        if (current == null) {
                            update.setNativeSnapshotId(recoveredCurrentSnapshot);
                            update.setPreviousNativeSnapshotId(recoveredPreviousSnapshot);
                        }
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
            return rebuild(current, spec, options, workspaceId, false);
        }
        OpenSandboxState info;
        try {
            info = delegate.describe(current.getSandboxId());
        } catch (RuntimeException error) {
            if (!delegate.isNotFound(error)) {
                throw error;
            }
            return rebuild(current, spec, options, workspaceId, false);
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
        OpenSandboxState persisted = connectionState(current, info, spec, options);
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

    private static OpenSandboxState connectionState(
            OpenSandboxWorkspaceRecord record,
            OpenSandboxState remote,
            WorkspaceSpec spec,
            OpenSandboxClientOptions options) {
        OpenSandboxState state = new OpenSandboxState();
        state.setSandboxId(record.getSandboxId());
        state.setSandboxOwned(true);
        state.setWorkspaceSpec(spec.copy());
        state.setWorkspaceRootReady(true);
        state.setImage(options.getImage());
        state.setEntrypoint(options.getEntrypoint());
        state.setResourceLimits(options.getResourceLimits());
        state.setSandboxTimeoutSeconds(options.getSandboxTimeoutSeconds());
        state.setMetadata(remote.getMetadata());
        state.setRemoteStatus(remote.getRemoteStatus());
        state.setRemoteCreatedAt(remote.getRemoteCreatedAt());
        state.setRemoteExpiresAt(remote.getRemoteExpiresAt());
        return state;
    }

    private MissingRecordRecovery recoverMissingRecord(
            WorkspaceSpec spec,
            OpenSandboxClientOptions options,
            String workspaceId,
            long deletedThrough)
            throws Exception {
        List<OpenSandboxState> live =
                new ArrayList<>(delegate.listByMetadata(workspaceIdentity(workspaceId)));
        live.sort(
                Comparator.comparingLong(RedisOpenSandboxClient::metadataGeneration)
                        .reversed()
                        .thenComparing(
                                OpenSandboxState::getRemoteCreatedAt,
                                Comparator.nullsLast(Comparator.reverseOrder()))
                        .thenComparing(OpenSandboxState::getSandboxId));
        List<OpenSandboxState> eligible =
                live.stream().filter(state -> metadataGeneration(state) > deletedThrough).toList();
        for (OpenSandboxState state : live) {
            if (!eligible.contains(state)) {
                store.markOrphanSandbox(workspaceId, state.getSandboxId(), clock.instant());
            }
        }
        if (!eligible.isEmpty()) {
            OpenSandboxState winner = eligible.get(0);
            String discoveredProfile = winner.getMetadata().get("agentscope.runtime-profile");
            if (discoveredProfile != null
                    && !discoveredProfile.equals(runtimeProfileHash(options))) {
                throw new SandboxException.SandboxConfigurationException(
                        "Discovered OpenSandbox runtime profile differs from the requested"
                                + " profile");
            }
            for (int index = 1; index < eligible.size(); index++) {
                store.markOrphanSandbox(
                        workspaceId, eligible.get(index).getSandboxId(), clock.instant());
            }
            if ("PAUSED".equalsIgnoreCase(winner.getRemoteStatus())) {
                delegate.resumeRemote(winner.getSandboxId());
            }
            winner.setWorkspaceSpec(spec.copy());
            winner.setWorkspaceRootReady(true);
            OpenSandbox remote = requireOpenSandbox(delegate.resume(winner));
            try {
                remote.startExisting();
            } catch (Exception error) {
                remote.disconnect();
                throw error;
            }
            return new MissingRecordRecovery(
                    remote, Math.max(1, metadataGeneration(winner)), null, null);
        }

        List<Map.Entry<String, NativeSnapshot>> ready =
                new ArrayList<>(
                        delegate
                                .listNativeSnapshotDetailsByNamePrefix(
                                        OpenSandboxLifecycleSweeper.snapshotNamePrefix(workspaceId))
                                .entrySet()
                                .stream()
                                .filter(snapshot -> isReady(snapshot.getValue()))
                                .toList());
        ready.sort(
                Comparator.<Map.Entry<String, NativeSnapshot>, Instant>comparing(
                                entry -> entry.getValue().createdAt(), Comparator.reverseOrder())
                        .thenComparing(Map.Entry::getKey));
        List<Map.Entry<String, NativeSnapshot>> eligibleSnapshots =
                ready.stream()
                        .filter(
                                snapshot ->
                                        deletedThrough == 0
                                                || snapshotGeneration(
                                                                workspaceId,
                                                                snapshot.getValue().name())
                                                        > deletedThrough)
                        .toList();
        if (deletedThrough > 0) {
            for (Map.Entry<String, NativeSnapshot> snapshot : ready) {
                if (eligibleSnapshots.contains(snapshot)) {
                    continue;
                }
                store.markOrphanSnapshot(workspaceId, snapshot.getKey(), clock.instant());
            }
        }
        if (eligibleSnapshots.isEmpty() && deletedThrough > 0) {
            long generation = nextGeneration(deletedThrough);
            return new MissingRecordRecovery(
                    createRemote(spec, options, workspaceId, generation, null),
                    generation,
                    null,
                    null);
        }
        if (eligibleSnapshots.isEmpty()) {
            return new MissingRecordRecovery(
                    createRemote(spec, options, workspaceId, 1, null), 1, null, null);
        }
        String currentSnapshot = eligibleSnapshots.get(0).getKey();
        String previousSnapshot =
                eligibleSnapshots.size() > 1 ? eligibleSnapshots.get(1).getKey() : null;
        long recoveryGeneration =
                Math.max(
                        nextGeneration(deletedThrough),
                        eligibleSnapshots.stream()
                                .mapToLong(
                                        snapshot ->
                                                snapshotGeneration(
                                                        workspaceId, snapshot.getValue().name()))
                                .max()
                                .orElse(1));
        Exception failure = null;
        List<String> recoverySnapshots = new ArrayList<>();
        recoverySnapshots.add(currentSnapshot);
        if (previousSnapshot != null) {
            recoverySnapshots.add(previousSnapshot);
        }
        for (String snapshotId : recoverySnapshots) {
            try {
                return new MissingRecordRecovery(
                        createRemote(spec, options, workspaceId, recoveryGeneration, snapshotId),
                        recoveryGeneration,
                        currentSnapshot,
                        previousSnapshot);
            } catch (Exception error) {
                if (failure == null) {
                    failure = error;
                } else {
                    failure.addSuppressed(error);
                }
            }
        }
        throw new SandboxException.SandboxRuntimeException(
                "All discovered native snapshot restore attempts failed; refusing empty workspace",
                failure);
    }

    private OpenSandbox rebuild(
            OpenSandboxWorkspaceRecord current,
            WorkspaceSpec spec,
            OpenSandboxClientOptions options,
            String workspaceId,
            boolean allowImageCreate)
            throws Exception {
        long generation = nextGeneration(current.getGeneration());
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
        validateGeneration(generation);
        OpenSandboxClientOptions options = OpenSandboxClientOptions.copyOf(requested);
        options.setRestoreSnapshotId(snapshotId);
        Map<String, String> metadata = new LinkedHashMap<>(options.getMetadata());
        metadata.put("agentscope.owner", "opensandbox-redis");
        metadata.put("agentscope.workspace-id", workspaceId);
        metadata.put("agentscope.generation", Long.toString(generation));
        metadata.put("agentscope.schema-version", Integer.toString(SCHEMA_VERSION));
        metadata.put("agentscope.runtime-profile", runtimeProfileHash(options));
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
                        if (current != null) {
                            validateGeneration(current.getGeneration());
                        }
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
                        if (current.getExpiresAt() == null
                                || !current.getExpiresAt()
                                        .isAfter(now.plus(lifecycle.getActiveRenewLead()))) {
                            Instant renewedUntil =
                                    delegate.renew(
                                            current.getSandboxId(),
                                            lifecycle.getActiveSandboxTtl());
                            OpenSandboxWorkspaceRecord renewed = current.copy();
                            renewed.setExpiresAt(renewedUntil);
                            renewed.setUpdatedAt(now);
                            if (!store.compareAndSet(current, renewed)) {
                                throw new SandboxException.SandboxRuntimeException(
                                        "OpenSandbox record changed while renewing active sandbox",
                                        null);
                            }
                        }
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

    private static Map<String, String> workspaceIdentity(String workspaceId) {
        return Map.of(
                "agentscope.owner", "opensandbox-redis", "agentscope.workspace-id", workspaceId);
    }

    private void markDeletedRecordOrphans(OpenSandboxWorkspaceRecord record) {
        Instant now = clock.instant();
        if (record.getSandboxId() != null && !record.getSandboxId().isBlank()) {
            store.markOrphanSandbox(record.getWorkspaceId(), record.getSandboxId(), now);
        }
        if (record.getNativeSnapshotId() != null && !record.getNativeSnapshotId().isBlank()) {
            store.markOrphanSnapshot(record.getWorkspaceId(), record.getNativeSnapshotId(), now);
        }
        if (record.getPreviousNativeSnapshotId() != null
                && !record.getPreviousNativeSnapshotId().isBlank()) {
            store.markOrphanSnapshot(
                    record.getWorkspaceId(), record.getPreviousNativeSnapshotId(), now);
        }
    }

    private void deleteDiscoveredSandbox(
            String workspaceId, String sandboxId, OpenSandboxState state) {
        try {
            deleteRemoteSandbox(workspaceId, sandboxId, delegate.resume(state));
        } catch (RuntimeException error) {
            if (!delegate.isNotFound(error)) {
                throw error;
            }
            store.removeOrphanSandbox(
                    new OpenSandboxWorkspaceStore.OrphanReference(workspaceId, sandboxId));
        }
    }

    private void deleteRemoteSandbox(String workspaceId, String sandboxId, Sandbox remote) {
        try {
            delegate.delete(remote);
        } catch (RuntimeException error) {
            if (!delegate.isNotFound(error)) {
                throw error;
            }
        }
        store.removeOrphanSandbox(
                new OpenSandboxWorkspaceStore.OrphanReference(workspaceId, sandboxId));
    }

    private void deleteRemoteSnapshot(String workspaceId, String snapshotId) {
        if (store.isSnapshotReferenced(snapshotId)) {
            return;
        }
        try {
            delegate.deleteNativeSnapshot(snapshotId);
        } catch (RuntimeException error) {
            if (!delegate.isNotFound(error)) {
                throw error;
            }
        }
        store.removeOrphanSnapshot(
                new OpenSandboxWorkspaceStore.OrphanReference(workspaceId, snapshotId));
    }

    private void deleteLocked(
            RedisManagedOpenSandbox managed,
            List<OpenSandboxState> discovered,
            Map<String, NativeSnapshot> discoveredSnapshots,
            long deletedThrough)
            throws Exception {
        Exception failure = null;
        try {
            managed.stop();
        } catch (Exception error) {
            failure = error;
        }
        try {
            OpenSandboxWorkspaceRecord current = store.load(managed.workspaceId()).orElse(null);
            if (current != null) {
                validateGeneration(current.getGeneration());
            }
            boolean deleteCurrent = current == null || current.getGeneration() <= deletedThrough;
            Instant now = clock.instant();
            Map<String, OpenSandboxState> sandboxes = new LinkedHashMap<>();
            for (OpenSandboxState state : discovered) {
                if (metadataGeneration(state) <= deletedThrough) {
                    sandboxes.put(state.getSandboxId(), state);
                }
            }
            String managedSandboxId =
                    ((OpenSandboxState) managed.delegate().getState()).getSandboxId();
            if (deleteCurrent
                    && current != null
                    && current.getSandboxId() != null
                    && !current.getSandboxId().equals(managedSandboxId)) {
                OpenSandboxState state = new OpenSandboxState();
                state.setSandboxId(current.getSandboxId());
                state.setSandboxOwned(true);
                sandboxes.putIfAbsent(current.getSandboxId(), state);
            }
            sandboxes.remove(managedSandboxId);

            LinkedHashSet<String> snapshots = new LinkedHashSet<>();
            if (deleteCurrent && current != null) {
                addIfPresent(snapshots, current.getNativeSnapshotId());
                addIfPresent(snapshots, current.getPreviousNativeSnapshotId());
            }
            discoveredSnapshots.values().stream()
                    .filter(
                            snapshot ->
                                    snapshotGeneration(managed.workspaceId(), snapshot.name())
                                            <= deletedThrough)
                    .map(NativeSnapshot::id)
                    .forEach(snapshots::add);

            store.markOrphanSandbox(managed.workspaceId(), managedSandboxId, now);
            for (String sandboxId : sandboxes.keySet()) {
                store.markOrphanSandbox(managed.workspaceId(), sandboxId, now);
            }
            for (String snapshotId : snapshots) {
                store.markOrphanSnapshot(managed.workspaceId(), snapshotId, now);
            }

            deleteRemoteSandbox(managed.workspaceId(), managedSandboxId, managed.delegate());
            for (Map.Entry<String, OpenSandboxState> entry : sandboxes.entrySet()) {
                deleteDiscoveredSandbox(managed.workspaceId(), entry.getKey(), entry.getValue());
            }
            if (deleteCurrent && current != null && !store.compareAndSet(current, null)) {
                throw new SandboxException.SandboxRuntimeException(
                        "OpenSandbox record changed while deleting", null);
            }
            for (String snapshotId : snapshots) {
                deleteRemoteSnapshot(managed.workspaceId(), snapshotId);
            }
            if (deleteCurrent) {
                store.clearLeases(managed.workspaceId());
                store.cancelIdle(managed.workspaceId());
                store.cancelRepair(managed.workspaceId());
            }
        } catch (Exception error) {
            if (failure == null) {
                failure = error;
            } else {
                failure.addSuppressed(error);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static void addIfPresent(LinkedHashSet<String> values, String value) {
        if (value != null && !value.isBlank()) {
            values.add(value);
        }
    }

    static long snapshotGeneration(String workspaceId, String snapshotName) {
        String prefix = OpenSandboxLifecycleSweeper.snapshotNamePrefix(workspaceId);
        if (snapshotName == null || !snapshotName.startsWith(prefix)) {
            throw invalidGeneration();
        }
        int separator = snapshotName.indexOf('-', prefix.length() + 1);
        if (separator < 0) {
            throw invalidGeneration();
        }
        return parseGeneration(snapshotName.substring(prefix.length(), separator));
    }

    private static boolean isReady(NativeSnapshot snapshot) {
        return "READY".equalsIgnoreCase(snapshot.status());
    }

    private static long metadataGeneration(OpenSandboxState state) {
        return parseGeneration(state.getMetadata().getOrDefault("agentscope.generation", "0"));
    }

    static long parseGeneration(String value) {
        try {
            return validateGeneration(Long.parseLong(value));
        } catch (NumberFormatException ignored) {
            throw invalidGeneration();
        }
    }

    private static long nextGeneration(long generation) {
        validateGeneration(generation);
        try {
            return validateGeneration(Math.addExact(generation, 1));
        } catch (ArithmeticException ignored) {
            throw invalidGeneration();
        }
    }

    static long validateGeneration(long generation) {
        if (generation < 0 || generation == Long.MAX_VALUE) {
            throw invalidGeneration();
        }
        return generation;
    }

    private static SandboxException.SandboxRuntimeException invalidGeneration() {
        return new SandboxException.SandboxRuntimeException("Invalid OpenSandbox generation", null);
    }

    private static ScheduledExecutorService newScheduler() {
        AtomicInteger number = new AtomicInteger();
        ThreadFactory factory =
                runnable -> {
                    Thread thread =
                            new Thread(
                                    runnable,
                                    "agentscope-opensandbox-redis-" + number.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                };
        return Executors.newScheduledThreadPool(2, factory);
    }

    @FunctionalInterface
    private interface LockedOperation<T> {
        T run() throws Exception;
    }

    private record MissingRecordRecovery(
            OpenSandbox remote,
            long generation,
            String currentSnapshotId,
            String previousSnapshotId) {}
}
