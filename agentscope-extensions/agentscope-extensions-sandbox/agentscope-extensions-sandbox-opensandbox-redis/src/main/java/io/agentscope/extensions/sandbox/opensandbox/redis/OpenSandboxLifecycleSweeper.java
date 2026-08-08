/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.agentscope.extensions.sandbox.opensandbox.redis;

import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClient;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClient.NativeSnapshot;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxState;
import io.agentscope.harness.agent.sandbox.Sandbox;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.redisson.api.RLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Coordinates idle OpenSandbox snapshot and pause operations across application instances. */
public final class OpenSandboxLifecycleSweeper implements AutoCloseable {
    static final int SWEEP_BATCH_SIZE = 100;
    static final int SNAPSHOT_CONCURRENCY = 1;
    private static final Logger log = LoggerFactory.getLogger(OpenSandboxLifecycleSweeper.class);

    private final OpenSandboxClient delegate;
    private final OpenSandboxWorkspaceStore store;
    private final OpenSandboxRedisLifecycleOptions lifecycle;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;

    public OpenSandboxLifecycleSweeper(
            OpenSandboxClient delegate,
            OpenSandboxWorkspaceStore store,
            OpenSandboxRedisLifecycleOptions lifecycle) {
        this(delegate, store, lifecycle, Clock.systemUTC(), newScheduler());
    }

    OpenSandboxLifecycleSweeper(
            OpenSandboxClient delegate,
            OpenSandboxWorkspaceStore store,
            OpenSandboxRedisLifecycleOptions lifecycle,
            Clock clock,
            ScheduledExecutorService scheduler) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.store = Objects.requireNonNull(store, "store");
        this.lifecycle =
                OpenSandboxRedisLifecycleOptions.copyOf(
                        Objects.requireNonNull(lifecycle, "lifecycle"));
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    /** Starts periodic sweeping when enabled. */
    public void start() {
        if (!lifecycle.isSweeperEnabled()) {
            return;
        }
        long interval = lifecycle.getSweepInterval().toMillis();
        scheduler.scheduleWithFixedDelay(
                this::sweepSafely, interval, interval, TimeUnit.MILLISECONDS);
    }

    /** Runs one bounded scan. Primarily useful for deterministic operational tests. */
    public void sweepOnce() {
        Instant now = clock.instant();
        Instant retryAt = now.plus(lifecycle.getSweepInterval());
        for (String workspaceId : store.claimDueRepairs(now, retryAt, SWEEP_BATCH_SIZE)) {
            try {
                repairWorkspace(workspaceId, now);
            } catch (RuntimeException error) {
                log.warn(
                        "[sandbox-opensandbox-redis] Lifecycle repair failed for {}",
                        workspaceId,
                        error);
            }
        }
        for (String workspaceId : store.claimDueIdle(now, retryAt, SWEEP_BATCH_SIZE)) {
            try {
                sweepWorkspace(workspaceId, now);
            } catch (RuntimeException error) {
                log.warn(
                        "[sandbox-opensandbox-redis] Lifecycle sweep failed for {}",
                        workspaceId,
                        error);
            }
        }
        cleanOrphans(now.minus(lifecycle.getOrphanGrace()));
    }

    private void repairWorkspace(String workspaceId, Instant now) {
        RLock lock = store.lifecycleLock(workspaceId);
        boolean acquired;
        try {
            acquired = lock.tryLock(0, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return;
        }
        if (!acquired) {
            return;
        }
        try {
            OpenSandboxWorkspaceRecord current = store.load(workspaceId).orElse(null);
            if (current != null) {
                RedisOpenSandboxClient.validateGeneration(current.getGeneration());
            }
            long deletedThrough = store.deletedThroughGeneration(workspaceId);
            RedisOpenSandboxClient.validateGeneration(deletedThrough);
            store.reconcileSnapshotReferences(workspaceId);
            if (current == null) {
                if (deletedThrough > 0) {
                    repairDeletedWorkspace(workspaceId, null, deletedThrough, now);
                } else {
                    store.cancelRepair(workspaceId);
                }
                return;
            }
            if (current.getGeneration() <= deletedThrough) {
                repairDeletedWorkspace(workspaceId, current, deletedThrough, now);
                return;
            }
            if (current.getLifecycleState() == OpenSandboxWorkspaceRecord.LifecycleState.PAUSED
                    || !store.activeLeases(workspaceId, current.getGeneration()).isEmpty()) {
                return;
            }
            boolean hasIdle = store.hasIdle(workspaceId);
            Instant expiresAt =
                    delegate.renew(
                            current.getSandboxId(),
                            RedisOpenSandboxClient.idleRetention(lifecycle));
            OpenSandboxWorkspaceRecord repaired = current.copy();
            repaired.setExpiresAt(expiresAt);
            repaired.setUpdatedAt(now);
            if (!store.compareAndSet(current, repaired)) {
                return;
            }
            discoverOrphanSnapshots(current, now);
            if (hasIdle) {
                return;
            }
            Instant dueAt = idleDueAt(current, now);
            store.scheduleIdle(workspaceId, dueAt.isBefore(now) ? now : dueAt);
        } finally {
            lock.unlock();
        }
    }

    private void repairDeletedWorkspace(
            String workspaceId,
            OpenSandboxWorkspaceRecord current,
            long deletedThrough,
            Instant now) {
        LinkedHashSet<String> sandboxes = new LinkedHashSet<>();
        if (current != null
                && current.getSandboxId() != null
                && !current.getSandboxId().isBlank()) {
            sandboxes.add(current.getSandboxId());
        }
        Map<String, String> identity =
                Map.of(
                        "agentscope.owner",
                        "opensandbox-redis",
                        "agentscope.workspace-id",
                        workspaceId);
        delegate.listByMetadata(identity).stream()
                .filter(state -> metadataGeneration(state) <= deletedThrough)
                .map(OpenSandboxState::getSandboxId)
                .forEach(sandboxes::add);
        for (String sandboxId : sandboxes) {
            store.markOrphanSandbox(workspaceId, sandboxId, now);
        }

        LinkedHashSet<String> snapshots = new LinkedHashSet<>();
        if (current != null) {
            addIfPresent(snapshots, current.getNativeSnapshotId());
            addIfPresent(snapshots, current.getPreviousNativeSnapshotId());
        }
        List<NativeSnapshot> discoveredSnapshots =
                delegate
                        .listNativeSnapshotDetailsByNamePrefix(snapshotNamePrefix(workspaceId))
                        .values()
                        .stream()
                        .filter(
                                snapshot ->
                                        RedisOpenSandboxClient.snapshotGeneration(
                                                        workspaceId, snapshot.name())
                                                <= deletedThrough)
                        .toList();
        discoveredSnapshots.stream().map(NativeSnapshot::id).forEach(snapshots::add);
        for (String snapshotId : snapshots) {
            store.markOrphanSnapshot(workspaceId, snapshotId, now);
        }
        if (current != null && !store.compareAndSet(current, null)) {
            return;
        }
        store.clearLeases(workspaceId);
        store.cancelIdle(workspaceId);
        if (discoveredSnapshots.stream()
                .allMatch(OpenSandboxLifecycleSweeper::isTerminalSnapshot)) {
            store.cancelRepair(workspaceId);
        }
    }

    private static boolean isTerminalSnapshot(NativeSnapshot snapshot) {
        return "READY".equalsIgnoreCase(snapshot.status())
                || "FAILED".equalsIgnoreCase(snapshot.status());
    }

    private static void addIfPresent(LinkedHashSet<String> values, String value) {
        if (value != null && !value.isBlank()) {
            values.add(value);
        }
    }

    private Instant idleDueAt(OpenSandboxWorkspaceRecord current, Instant now) {
        if (current.getLifecycleState()
                        == OpenSandboxWorkspaceRecord.LifecycleState.EVICTION_PENDING
                && current.getEvictionCandidateAt() != null) {
            return current.getEvictionCandidateAt().plus(lifecycle.getEvictionGrace());
        }
        return (current.getLastAccessAt() == null ? now : current.getLastAccessAt())
                .plus(lifecycle.getIdleTtl());
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }

    static String snapshotNamePrefix(String workspaceId) {
        return "agentscope-workspace-" + workspaceId + "-";
    }

    static String snapshotName(String workspaceId, long generation, Instant createdAt) {
        return snapshotNamePrefix(workspaceId)
                + generation
                + "-"
                + Objects.requireNonNull(createdAt, "createdAt").toEpochMilli();
    }

    private void sweepSafely() {
        try {
            sweepOnce();
        } catch (RuntimeException error) {
            log.warn("[sandbox-opensandbox-redis] Lifecycle sweep failed", error);
        }
    }

    private void sweepWorkspace(String workspaceId, Instant now) {
        RLock lock = store.lifecycleLock(workspaceId);
        boolean acquired;
        try {
            acquired = lock.tryLock(0, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return;
        }
        if (!acquired) {
            return;
        }
        try {
            OpenSandboxWorkspaceRecord current = store.load(workspaceId).orElse(null);
            if (current != null) {
                RedisOpenSandboxClient.validateGeneration(current.getGeneration());
            }
            if (current == null) {
                store.cancelIdle(workspaceId);
                return;
            }
            List<OpenSandboxActiveLease> leases =
                    store.activeLeases(workspaceId, current.getGeneration());
            if (!leases.isEmpty()) {
                renewActive(current, now);
                store.cancelIdle(workspaceId);
                return;
            }
            if (current.getLifecycleState() == OpenSandboxWorkspaceRecord.LifecycleState.PAUSED) {
                store.cancelIdle(workspaceId);
                return;
            }
            Instant dueAt = idleDueAt(current, now);
            if (now.isBefore(dueAt)) {
                store.scheduleIdle(workspaceId, dueAt);
                return;
            }
            if (current.getLifecycleState()
                            != OpenSandboxWorkspaceRecord.LifecycleState.EVICTION_PENDING
                    || current.getEvictionCandidateAt() == null) {
                enterEvictionGrace(current, now);
                return;
            }
            Instant graceEnds = current.getEvictionCandidateAt().plus(lifecycle.getEvictionGrace());
            if (now.isBefore(graceEnds)) {
                store.scheduleIdle(workspaceId, graceEnds);
                return;
            }
            evict(current, now);
        } finally {
            lock.unlock();
        }
    }

    private void renewActive(OpenSandboxWorkspaceRecord current, Instant now) {
        Instant expiresAt = delegate.renew(current.getSandboxId(), lifecycle.getActiveSandboxTtl());
        OpenSandboxWorkspaceRecord update = current.copy();
        update.setLifecycleState(OpenSandboxWorkspaceRecord.LifecycleState.RUNNING);
        update.setEvictionCandidateAt(null);
        update.setExpiresAt(expiresAt);
        update.setLastError(null);
        update.setUpdatedAt(now);
        store.compareAndSet(current, update);
    }

    private void enterEvictionGrace(OpenSandboxWorkspaceRecord current, Instant now) {
        Instant expiresAt =
                delegate.renew(
                        current.getSandboxId(),
                        RedisOpenSandboxClient.evictionRetention(lifecycle));
        OpenSandboxWorkspaceRecord update = current.copy();
        update.setLifecycleState(OpenSandboxWorkspaceRecord.LifecycleState.EVICTION_PENDING);
        update.setEvictionCandidateAt(now);
        update.setExpiresAt(expiresAt);
        update.setUpdatedAt(now);
        if (store.compareAndSet(current, update)) {
            store.scheduleIdle(current.getWorkspaceId(), now.plus(lifecycle.getEvictionGrace()));
        }
    }

    private void evict(OpenSandboxWorkspaceRecord current, Instant now) {
        OpenSandboxState remote;
        try {
            remote = delegate.describe(current.getSandboxId());
        } catch (RuntimeException error) {
            safetyRenew(current, now, error);
            return;
        }
        if ("PAUSED".equalsIgnoreCase(remote.getRemoteStatus())) {
            reconcilePaused(current, now, remote.getRemoteExpiresAt());
            return;
        }

        Instant snapshotExpiresAt;
        try {
            snapshotExpiresAt =
                    delegate.renew(
                            current.getSandboxId(),
                            RedisOpenSandboxClient.snapshotAttemptRetention(lifecycle));
        } catch (RuntimeException error) {
            safetyRenew(current, now, error);
            return;
        }

        String snapshotId = null;
        try {
            snapshotId =
                    delegate.createNativeSnapshot(
                            current.getSandboxId(),
                            snapshotName(current.getWorkspaceId(), current.getGeneration(), now));
            store.markOrphanSnapshot(current.getWorkspaceId(), snapshotId, now);
        } catch (RuntimeException error) {
            if (snapshotId != null) {
                try {
                    delegate.deleteNativeSnapshot(snapshotId);
                } catch (RuntimeException cleanupFailure) {
                    error.addSuppressed(cleanupFailure);
                }
            }
            safetyRenew(current, now, error);
            return;
        }
        try {
            snapshotId =
                    delegate.waitForNativeSnapshotReady(
                            snapshotId, lifecycle.getSnapshotReadyTimeout());
        } catch (RuntimeException error) {
            safetyRenew(current, now, error);
            return;
        }

        OpenSandboxWorkspaceRecord snapshotted = current.copy();
        snapshotted.setPreviousNativeSnapshotId(current.getNativeSnapshotId());
        snapshotted.setNativeSnapshotId(snapshotId);
        snapshotted.setDirty(false);
        snapshotted.setExpiresAt(snapshotExpiresAt);
        snapshotted.setLastError(null);
        snapshotted.setUpdatedAt(now);
        boolean switched;
        try {
            switched = store.compareAndSet(current, snapshotted);
        } catch (RuntimeException casFailure) {
            retainReadySnapshot(current, snapshotId, now, casFailure);
            return;
        }
        if (!switched) {
            retainReadySnapshot(current, snapshotId, now, null);
            return;
        }
        try {
            store.removeOrphanSnapshot(
                    new OpenSandboxWorkspaceStore.OrphanReference(
                            current.getWorkspaceId(), snapshotId));
        } catch (RuntimeException error) {
            log.warn(
                    "[sandbox-opensandbox-redis] Failed to promote snapshot marker {}",
                    snapshotId,
                    error);
        }
        if (current.getPreviousNativeSnapshotId() != null
                && !Objects.equals(current.getPreviousNativeSnapshotId(), snapshotId)) {
            try {
                store.markOrphanSnapshot(
                        current.getWorkspaceId(), current.getPreviousNativeSnapshotId(), now);
            } catch (RuntimeException error) {
                log.warn(
                        "[sandbox-opensandbox-redis] Failed to index displaced snapshot {}",
                        current.getPreviousNativeSnapshotId(),
                        error);
            }
        }

        Instant expiresAt;
        try {
            expiresAt = delegate.renew(current.getSandboxId(), lifecycle.getPauseRetention());
            delegate.pause(current.getSandboxId());
        } catch (RuntimeException error) {
            safetyRenew(snapshotted, now, error);
            return;
        }
        OpenSandboxWorkspaceRecord paused = snapshotted.copy();
        paused.setLifecycleState(OpenSandboxWorkspaceRecord.LifecycleState.PAUSED);
        paused.setPausedAt(now);
        paused.setExpiresAt(expiresAt);
        paused.setEvictionCandidateAt(null);
        paused.setUpdatedAt(now);
        if (store.compareAndSet(snapshotted, paused)) {
            store.cancelIdle(current.getWorkspaceId());
        }
    }

    private void reconcilePaused(
            OpenSandboxWorkspaceRecord current, Instant now, Instant remoteExpiresAt) {
        OpenSandboxWorkspaceRecord paused = current.copy();
        paused.setLifecycleState(OpenSandboxWorkspaceRecord.LifecycleState.PAUSED);
        paused.setPausedAt(current.getPausedAt() == null ? now : current.getPausedAt());
        paused.setExpiresAt(remoteExpiresAt);
        paused.setEvictionCandidateAt(null);
        paused.setUpdatedAt(now);
        if (store.compareAndSet(current, paused)) {
            store.cancelIdle(current.getWorkspaceId());
        }
    }

    private void safetyRenew(
            OpenSandboxWorkspaceRecord current, Instant now, RuntimeException failure) {
        try {
            Instant expiresAt =
                    delegate.renew(current.getSandboxId(), lifecycle.getActiveSandboxTtl());
            OpenSandboxWorkspaceRecord update = current.copy();
            update.setExpiresAt(expiresAt);
            update.setLastError(failure == null ? null : failure.getMessage());
            update.setUpdatedAt(now);
            store.compareAndSet(current, update);
        } catch (RuntimeException renewFailure) {
            if (failure != null) {
                renewFailure.addSuppressed(failure);
            }
            log.warn(
                    "[sandbox-opensandbox-redis] Safety renewal failed for {}",
                    current.getWorkspaceId(),
                    renewFailure);
        } finally {
            store.scheduleIdle(current.getWorkspaceId(), now.plus(lifecycle.getEvictionGrace()));
        }
    }

    private void retainReadySnapshot(
            OpenSandboxWorkspaceRecord current,
            String snapshotId,
            Instant now,
            RuntimeException primaryFailure) {
        RuntimeException failure = primaryFailure;
        try {
            safetyRenew(current, now, failure);
        } catch (RuntimeException renewFailure) {
            if (failure != null && failure != renewFailure) {
                failure.addSuppressed(renewFailure);
            }
            log.warn(
                    "[sandbox-opensandbox-redis] Ready snapshot retention failed for {}",
                    snapshotId,
                    failure == null ? renewFailure : failure);
        }
    }

    private void cleanOrphans(Instant cutoff) {
        Instant retryAt = cutoff.plus(lifecycle.getSweepInterval());
        for (OpenSandboxWorkspaceStore.OrphanReference orphan :
                store.claimDueOrphanSandboxes(cutoff, retryAt, SWEEP_BATCH_SIZE)) {
            try {
                cleanOrphanSandbox(orphan);
            } catch (RuntimeException error) {
                log.warn(
                        "[sandbox-opensandbox-redis] Duplicate cleanup failed for {}",
                        orphan.remoteId(),
                        error);
            }
        }
        for (OpenSandboxWorkspaceStore.OrphanReference orphan :
                store.claimDueOrphanSnapshots(cutoff, retryAt, SWEEP_BATCH_SIZE)) {
            try {
                cleanOrphanSnapshot(orphan);
            } catch (RuntimeException error) {
                log.warn(
                        "[sandbox-opensandbox-redis] Snapshot cleanup failed for {}",
                        orphan.remoteId(),
                        error);
            }
        }
    }

    private void cleanOrphanSandbox(OpenSandboxWorkspaceStore.OrphanReference orphan) {
        RLock lock = store.lifecycleLock(orphan.workspaceId());
        boolean acquired;
        try {
            acquired = lock.tryLock(0, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return;
        }
        if (!acquired) {
            return;
        }
        try {
            OpenSandboxWorkspaceRecord current = store.load(orphan.workspaceId()).orElse(null);
            if (current != null) {
                RedisOpenSandboxClient.validateGeneration(current.getGeneration());
            }
            if (current == null || !Objects.equals(current.getSandboxId(), orphan.remoteId())) {
                OpenSandboxState info;
                try {
                    info = delegate.describe(orphan.remoteId());
                } catch (RuntimeException error) {
                    if (delegate.isNotFound(error)) {
                        store.removeOrphanSandbox(orphan);
                        return;
                    }
                    throw error;
                }
                long generation = orphanGeneration(info, current);
                if (generation >= 0
                        && !store.activeLeases(orphan.workspaceId(), generation).isEmpty()) {
                    return;
                }
                OpenSandboxState state = new OpenSandboxState();
                state.setSandboxId(orphan.remoteId());
                state.setSandboxOwned(true);
                Sandbox remote = delegate.resume(state);
                try {
                    delegate.delete(remote);
                } catch (RuntimeException error) {
                    if (!delegate.isNotFound(error)) {
                        throw error;
                    }
                }
            }
            store.removeOrphanSandbox(orphan);
        } finally {
            lock.unlock();
        }
    }

    private void cleanOrphanSnapshot(OpenSandboxWorkspaceStore.OrphanReference orphan) {
        RLock lock = store.lifecycleLock(orphan.workspaceId());
        boolean acquired;
        try {
            acquired = lock.tryLock(0, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return;
        }
        if (!acquired) {
            return;
        }
        try {
            if (!store.isSnapshotReferenced(orphan.remoteId())) {
                try {
                    delegate.deleteNativeSnapshot(orphan.remoteId());
                } catch (RuntimeException error) {
                    if (!delegate.isNotFound(error)) {
                        throw error;
                    }
                }
            } else {
                return;
            }
            store.removeOrphanSnapshot(orphan);
        } finally {
            lock.unlock();
        }
    }

    private static long orphanGeneration(
            OpenSandboxState info, OpenSandboxWorkspaceRecord current) {
        if (info != null) {
            Map<String, String> metadata = info.getMetadata();
            return RedisOpenSandboxClient.parseGeneration(
                    metadata == null ? null : metadata.get("agentscope.generation"));
        }
        return RedisOpenSandboxClient.validateGeneration(
                current == null ? -1 : current.getGeneration());
    }

    private static long metadataGeneration(OpenSandboxState state) {
        Map<String, String> metadata = state.getMetadata();
        return RedisOpenSandboxClient.parseGeneration(
                metadata == null ? null : metadata.get("agentscope.generation"));
    }

    private void discoverOrphanSnapshots(OpenSandboxWorkspaceRecord current, Instant now) {
        delegate
                .listNativeSnapshotDetailsByNamePrefix(snapshotNamePrefix(current.getWorkspaceId()))
                .values()
                .stream()
                .filter(snapshot -> !snapshot.id().equals(current.getNativeSnapshotId()))
                .filter(snapshot -> !snapshot.id().equals(current.getPreviousNativeSnapshotId()))
                .forEach(
                        snapshot ->
                                store.markOrphanSnapshot(
                                        current.getWorkspaceId(), snapshot.id(), now));
    }

    private static ScheduledExecutorService newScheduler() {
        AtomicInteger number = new AtomicInteger();
        return Executors.newSingleThreadScheduledExecutor(
                runnable -> {
                    Thread thread =
                            new Thread(
                                    runnable,
                                    "agentscope-opensandbox-redis-sweeper-"
                                            + number.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                });
    }
}
