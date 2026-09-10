/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.agentscope.extensions.sandbox.opensandbox.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClient;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClient.NativeSnapshot;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxState;
import io.agentscope.harness.agent.sandbox.Sandbox;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.redisson.api.RLock;

class OpenSandboxLifecycleSweeperTest {
    private static final Instant NOW = Instant.parse("2026-08-08T10:00:00Z");
    private static final String WORKSPACE_ID = "workspace-1";

    private OpenSandboxClient delegate;
    private OpenSandboxWorkspaceStore store;
    private OpenSandboxRedisLifecycleOptions lifecycle;
    private RLock lock;
    private AtomicReference<OpenSandboxWorkspaceRecord> persisted;
    private OpenSandboxLifecycleSweeper sweeper;

    @BeforeEach
    void setUp() throws Exception {
        delegate = mock(OpenSandboxClient.class);
        store = mock(OpenSandboxWorkspaceStore.class);
        lifecycle = new OpenSandboxRedisLifecycleOptions();
        lock = mock(RLock.class);
        persisted = new AtomicReference<>(runningRecord());
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(WORKSPACE_ID));
        when(store.lifecycleLock(WORKSPACE_ID)).thenReturn(lock);
        when(lock.tryLock(0, TimeUnit.SECONDS)).thenReturn(true);
        when(store.load(WORKSPACE_ID))
                .thenAnswer(ignored -> java.util.Optional.ofNullable(persisted.get()));
        when(store.compareAndSet(any(), any()))
                .thenAnswer(
                        invocation -> {
                            OpenSandboxWorkspaceRecord expected = invocation.getArgument(0);
                            OpenSandboxWorkspaceRecord update = invocation.getArgument(1);
                            if (persisted.get() != expected) return false;
                            persisted.set(update);
                            return true;
                        });
        sweeper =
                new OpenSandboxLifecycleSweeper(
                        delegate,
                        store,
                        lifecycle,
                        Clock.fixed(NOW, ZoneOffset.UTC),
                        mock(ScheduledExecutorService.class));
    }

    @Test
    void activeLeaseOnlyRenewsSandbox() {
        when(store.activeLeases(WORKSPACE_ID, 3))
                .thenReturn(
                        List.of(
                                new OpenSandboxActiveLease(
                                        "lease", WORKSPACE_ID, 3, "node", NOW, NOW)));
        when(delegate.renew("sandbox-3", lifecycle.getActiveSandboxTtl()))
                .thenReturn(NOW.plus(lifecycle.getActiveSandboxTtl()));

        sweeper.sweepOnce();

        verify(delegate).renew("sandbox-3", lifecycle.getActiveSandboxTtl());
        verify(delegate, never()).createNativeSnapshot(any(), any());
        verify(delegate, never()).pause(any());
    }

    @Test
    void firstIdleSweepOnlyEntersEvictionGrace() {
        when(store.activeLeases(WORKSPACE_ID, 3)).thenReturn(List.of());

        sweeper.sweepOnce();

        assertEquals(
                OpenSandboxWorkspaceRecord.LifecycleState.EVICTION_PENDING,
                persisted.get().getLifecycleState());
        assertEquals(NOW, persisted.get().getEvictionCandidateAt());
        verify(delegate).renew("sandbox-3", RedisOpenSandboxClient.evictionRetention(lifecycle));
        verify(store).scheduleIdle(WORKSPACE_ID, NOW.plus(lifecycle.getEvictionGrace()));
        verify(delegate, never()).createNativeSnapshot(any(), any());
        verify(delegate, never()).pause(any());
    }

    @Test
    void staleClaimIsRescheduledWhenLastAccessMovedIdleDeadline() {
        OpenSandboxWorkspaceRecord recentlyReleased = runningRecord();
        recentlyReleased.setLastAccessAt(NOW);
        persisted.set(recentlyReleased);
        when(store.activeLeases(WORKSPACE_ID, 3)).thenReturn(List.of());

        sweeper.sweepOnce();

        assertEquals(
                OpenSandboxWorkspaceRecord.LifecycleState.RUNNING,
                persisted.get().getLifecycleState());
        verify(store).scheduleIdle(WORKSPACE_ID, NOW.plus(lifecycle.getIdleTtl()));
        verify(delegate, never()).renew(any(), any());
        verify(delegate, never()).createNativeSnapshot(any(), any());
    }

    @Test
    void pausedRecordWithActiveLeaseRenewsBeforeConsideringStaleIdleIndex() {
        OpenSandboxWorkspaceRecord paused = runningRecord();
        paused.setLifecycleState(OpenSandboxWorkspaceRecord.LifecycleState.PAUSED);
        persisted.set(paused);
        when(store.activeLeases(WORKSPACE_ID, 3))
                .thenReturn(
                        List.of(
                                new OpenSandboxActiveLease(
                                        "lease", WORKSPACE_ID, 3, "node", NOW, NOW)));
        when(delegate.renew("sandbox-3", lifecycle.getActiveSandboxTtl()))
                .thenReturn(NOW.plus(lifecycle.getActiveSandboxTtl()));

        sweeper.sweepOnce();

        assertEquals(
                OpenSandboxWorkspaceRecord.LifecycleState.RUNNING,
                persisted.get().getLifecycleState());
        verify(store).activeLeases(WORKSPACE_ID, 3);
        verify(delegate).renew("sandbox-3", lifecycle.getActiveSandboxTtl());
        verify(store).cancelIdle(WORKSPACE_ID);
        verify(delegate, never()).createNativeSnapshot(any(), any());
        verify(delegate, never()).pause(any());
    }

    @Test
    void staleIdleIndexForPausedRecordOnlyCancelsIdle() {
        OpenSandboxWorkspaceRecord paused = runningRecord();
        paused.setLifecycleState(OpenSandboxWorkspaceRecord.LifecycleState.PAUSED);
        persisted.set(paused);
        when(store.activeLeases(WORKSPACE_ID, 3)).thenReturn(List.of());

        sweeper.sweepOnce();

        assertEquals(
                OpenSandboxWorkspaceRecord.LifecycleState.PAUSED,
                persisted.get().getLifecycleState());
        verify(store).activeLeases(WORKSPACE_ID, 3);
        verify(store).cancelIdle(WORKSPACE_ID);
        verify(store, never()).compareAndSet(any(), any());
        verify(store, never()).scheduleIdle(any(), any());
        verifyNoInteractions(delegate);
    }

    @Test
    void afterGraceSnapshotsThenCasThenRenewsThenPauses() {
        OpenSandboxWorkspaceRecord record = runningRecord();
        record.setLifecycleState(OpenSandboxWorkspaceRecord.LifecycleState.EVICTION_PENDING);
        record.setEvictionCandidateAt(NOW.minus(lifecycle.getEvictionGrace()));
        record.setNativeSnapshotId("snapshot-old");
        persisted.set(record);
        when(store.activeLeases(WORKSPACE_ID, 3)).thenReturn(List.of());
        when(delegate.describe("sandbox-3")).thenReturn(remoteInfo("RUNNING"));
        when(delegate.createNativeSnapshot(
                        eq("sandbox-3"),
                        eq(OpenSandboxLifecycleSweeper.snapshotName(WORKSPACE_ID, 3, NOW))))
                .thenReturn("snapshot-new");
        when(delegate.waitForNativeSnapshotReady(
                        "snapshot-new", lifecycle.getSnapshotReadyTimeout()))
                .thenReturn("snapshot-new");
        when(delegate.renew("sandbox-3", lifecycle.getPauseRetention()))
                .thenReturn(NOW.plus(lifecycle.getPauseRetention()));

        sweeper.sweepOnce();

        InOrder order = inOrder(delegate, store);
        order.verify(delegate)
                .renew("sandbox-3", RedisOpenSandboxClient.snapshotAttemptRetention(lifecycle));
        order.verify(delegate)
                .createNativeSnapshot(
                        "sandbox-3",
                        OpenSandboxLifecycleSweeper.snapshotName(WORKSPACE_ID, 3, NOW));
        order.verify(store).markOrphanSnapshot(WORKSPACE_ID, "snapshot-new", NOW);
        order.verify(delegate)
                .waitForNativeSnapshotReady("snapshot-new", lifecycle.getSnapshotReadyTimeout());
        order.verify(store).compareAndSet(eq(record), any(OpenSandboxWorkspaceRecord.class));
        order.verify(store)
                .removeOrphanSnapshot(
                        new OpenSandboxWorkspaceStore.OrphanReference(
                                WORKSPACE_ID, "snapshot-new"));
        order.verify(delegate).renew("sandbox-3", lifecycle.getPauseRetention());
        order.verify(delegate).pause("sandbox-3");
        assertEquals("snapshot-new", persisted.get().getNativeSnapshotId());
        assertEquals("snapshot-old", persisted.get().getPreviousNativeSnapshotId());
        assertEquals(
                OpenSandboxWorkspaceRecord.LifecycleState.PAUSED,
                persisted.get().getLifecycleState());
    }

    @Test
    void successfulSnapshotRotationDefersDisplacedPreviousDeletion() {
        OpenSandboxWorkspaceRecord record = pendingRecord();
        record.setNativeSnapshotId("snapshot-old");
        record.setPreviousNativeSnapshotId("snapshot-older");
        persisted.set(record);
        when(store.activeLeases(WORKSPACE_ID, 3)).thenReturn(List.of());
        when(delegate.describe("sandbox-3")).thenReturn(remoteInfo("RUNNING"));
        readySnapshot("snapshot-new");

        sweeper.sweepOnce();

        verify(store).markOrphanSnapshot(WORKSPACE_ID, "snapshot-older", NOW);
    }

    @Test
    void snapshotFailureRenewsSafetyAndDoesNotPauseOrOverwrite() {
        OpenSandboxWorkspaceRecord record = pendingRecord();
        record.setNativeSnapshotId("snapshot-old");
        persisted.set(record);
        when(store.activeLeases(WORKSPACE_ID, 3)).thenReturn(List.of());
        when(delegate.describe("sandbox-3")).thenReturn(remoteInfo("RUNNING"));
        when(delegate.createNativeSnapshot(any(), any()))
                .thenThrow(new IllegalStateException("snapshot failed"));

        sweeper.sweepOnce();

        verify(delegate).renew("sandbox-3", lifecycle.getActiveSandboxTtl());
        verify(delegate, never()).pause(any());
        assertEquals("snapshot-old", persisted.get().getNativeSnapshotId());
    }

    @Test
    void snapshotReadyTimeoutRetainsDurableOrphanMarker() {
        OpenSandboxWorkspaceRecord record = pendingRecord();
        persisted.set(record);
        when(store.activeLeases(WORKSPACE_ID, 3)).thenReturn(List.of());
        when(delegate.describe("sandbox-3")).thenReturn(remoteInfo("RUNNING"));
        when(delegate.createNativeSnapshot(any(), any())).thenReturn("snapshot-pending");
        when(delegate.waitForNativeSnapshotReady(
                        "snapshot-pending", lifecycle.getSnapshotReadyTimeout()))
                .thenThrow(new IllegalStateException("snapshot wait timed out"));

        sweeper.sweepOnce();

        InOrder order = inOrder(delegate, store);
        order.verify(delegate).createNativeSnapshot(any(), any());
        order.verify(store).markOrphanSnapshot(WORKSPACE_ID, "snapshot-pending", NOW);
        order.verify(delegate)
                .waitForNativeSnapshotReady(
                        "snapshot-pending", lifecycle.getSnapshotReadyTimeout());
        verify(store, never())
                .removeOrphanSnapshot(
                        new OpenSandboxWorkspaceStore.OrphanReference(
                                WORKSPACE_ID, "snapshot-pending"));
        verify(delegate, never()).pause(any());
    }

    @Test
    void snapshotCasFailureIndexesNewSnapshotAndDoesNotPause() {
        OpenSandboxWorkspaceRecord record = pendingRecord();
        record.setNativeSnapshotId("snapshot-old");
        persisted.set(record);
        when(store.activeLeases(WORKSPACE_ID, 3)).thenReturn(List.of());
        when(delegate.describe("sandbox-3")).thenReturn(remoteInfo("RUNNING"));
        readySnapshot("snapshot-new");
        when(store.compareAndSet(eq(record), any())).thenReturn(false);

        sweeper.sweepOnce();

        verify(store).markOrphanSnapshot(WORKSPACE_ID, "snapshot-new", NOW);
        verify(delegate).renew("sandbox-3", lifecycle.getActiveSandboxTtl());
        verify(delegate, never()).pause(any());
        assertEquals("snapshot-old", persisted.get().getNativeSnapshotId());
    }

    @Test
    void snapshotCasExceptionStillSafetyRenewsAndNeverPauses() {
        OpenSandboxWorkspaceRecord record = pendingRecord();
        persisted.set(record);
        when(store.activeLeases(WORKSPACE_ID, 3)).thenReturn(List.of());
        when(delegate.describe("sandbox-3")).thenReturn(remoteInfo("RUNNING"));
        readySnapshot("snapshot-new");
        when(store.compareAndSet(eq(record), any()))
                .thenThrow(new IllegalStateException("redis unavailable"));

        sweeper.sweepOnce();

        InOrder order = inOrder(delegate, store);
        order.verify(delegate).createNativeSnapshot(any(), any());
        order.verify(store).compareAndSet(eq(record), any());
        order.verify(delegate).renew("sandbox-3", lifecycle.getActiveSandboxTtl());
        verify(delegate, never()).pause(any());
    }

    @Test
    void orphanIndexFailureDeletesCreatedSnapshotBeforeSafetyRenew() {
        OpenSandboxWorkspaceRecord record = pendingRecord();
        persisted.set(record);
        when(store.activeLeases(WORKSPACE_ID, 3)).thenReturn(List.of());
        when(delegate.describe("sandbox-3")).thenReturn(remoteInfo("RUNNING"));
        readySnapshot("snapshot-new");
        when(store.compareAndSet(eq(record), any())).thenReturn(false);
        doThrow(new IllegalStateException("orphan index unavailable"))
                .when(store)
                .markOrphanSnapshot(WORKSPACE_ID, "snapshot-new", NOW);

        sweeper.sweepOnce();

        InOrder order = inOrder(delegate, store);
        order.verify(delegate).createNativeSnapshot(any(), any());
        order.verify(store).markOrphanSnapshot(WORKSPACE_ID, "snapshot-new", NOW);
        order.verify(delegate).deleteNativeSnapshot("snapshot-new");
        order.verify(delegate).renew("sandbox-3", lifecycle.getActiveSandboxTtl());
        verify(delegate, never()).waitForNativeSnapshotReady(any(), any());
        assertEquals(null, persisted.get().getNativeSnapshotId());
        verify(delegate, never()).pause(any());
    }

    @Test
    void markerAndImmediateDeleteFailureAreRediscoveredByRepairCatalog() {
        OpenSandboxWorkspaceRecord record = pendingRecord();
        persisted.set(record);
        when(store.activeLeases(WORKSPACE_ID, 3)).thenReturn(List.of());
        when(delegate.describe("sandbox-3")).thenReturn(remoteInfo("RUNNING"));
        readySnapshot("snapshot-new");
        doThrow(new IllegalStateException("orphan index unavailable"))
                .doNothing()
                .when(store)
                .markOrphanSnapshot(WORKSPACE_ID, "snapshot-new", NOW);
        doThrow(new IllegalStateException("snapshot delete unavailable"))
                .when(delegate)
                .deleteNativeSnapshot("snapshot-new");
        when(store.claimDueRepairs(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(), List.of(WORKSPACE_ID));
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(WORKSPACE_ID), List.of());
        when(delegate.listNativeSnapshotDetailsByNamePrefix(any()))
                .thenReturn(Map.of("snapshot-new", snapshot("snapshot-new", 3, NOW)));

        sweeper.sweepOnce();
        sweeper.sweepOnce();

        verify(store, times(2)).markOrphanSnapshot(WORKSPACE_ID, "snapshot-new", NOW);
    }

    @Test
    void pendingSnapshotKeepsDeletedWorkspaceRepairUntilSnapshotIsTerminal() {
        OpenSandboxWorkspaceRecord record = pendingRecord();
        persisted.set(record);
        when(store.activeLeases(WORKSPACE_ID, 3)).thenReturn(List.of());
        when(delegate.describe("sandbox-3")).thenReturn(remoteInfo("RUNNING"));
        when(delegate.createNativeSnapshot(any(), any())).thenReturn("snapshot-new");
        doThrow(new IllegalStateException("orphan index unavailable"))
                .doNothing()
                .when(store)
                .markOrphanSnapshot(WORKSPACE_ID, "snapshot-new", NOW);
        doThrow(new IllegalStateException("snapshot delete unavailable"))
                .when(delegate)
                .deleteNativeSnapshot("snapshot-new");
        when(store.claimDueRepairs(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(), List.of(WORKSPACE_ID), List.of(WORKSPACE_ID));
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(WORKSPACE_ID), List.of(), List.of());
        when(store.deletedThroughGeneration(WORKSPACE_ID)).thenReturn(3L);
        when(delegate.listNativeSnapshotDetailsByNamePrefix(any()))
                .thenReturn(
                        Map.of("snapshot-new", snapshot("snapshot-new", 3, NOW, "CREATING")),
                        Map.of("snapshot-new", snapshot("snapshot-new", 3, NOW, "READY")));

        sweeper.sweepOnce();
        sweeper.sweepOnce();

        assertEquals(null, persisted.get());
        verify(store, never()).cancelRepair(WORKSPACE_ID);

        sweeper.sweepOnce();

        verify(store, times(3)).markOrphanSnapshot(WORKSPACE_ID, "snapshot-new", NOW);
        verify(store).cancelRepair(WORKSPACE_ID);
    }

    @Test
    void failedSnapshotAllowsDeletedWorkspaceRepairToComplete() {
        persisted.set(null);
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of());
        when(store.claimDueRepairs(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(WORKSPACE_ID));
        when(store.deletedThroughGeneration(WORKSPACE_ID)).thenReturn(3L);
        when(delegate.listNativeSnapshotDetailsByNamePrefix(any()))
                .thenReturn(
                        Map.of("snapshot-failed", snapshot("snapshot-failed", 3, NOW, "FAILED")));

        sweeper.sweepOnce();

        verify(store).markOrphanSnapshot(WORKSPACE_ID, "snapshot-failed", NOW);
        verify(store).cancelRepair(WORKSPACE_ID);
    }

    @Test
    void pausedRemoteReconcilesRecordAfterPriorPostPauseCasFailure() {
        OpenSandboxWorkspaceRecord record = pendingRecord();
        persisted.set(record);
        when(store.activeLeases(WORKSPACE_ID, 3)).thenReturn(List.of());
        when(delegate.describe("sandbox-3")).thenReturn(remoteInfo("PAUSED"));

        sweeper.sweepOnce();

        assertEquals(
                OpenSandboxWorkspaceRecord.LifecycleState.PAUSED,
                persisted.get().getLifecycleState());
        verify(delegate, never()).createNativeSnapshot(any(), any());
        verify(delegate, never()).pause(any());
    }

    @Test
    void unavailableWorkspaceDoesNotStopRemainingBatch() throws Exception {
        String second = "workspace-2";
        OpenSandboxWorkspaceRecord secondRecord = runningRecord();
        secondRecord.setWorkspaceId(second);
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(WORKSPACE_ID, second));
        when(store.lifecycleLock(second)).thenReturn(mock(RLock.class));
        when(store.lifecycleLock(second).tryLock(0, TimeUnit.SECONDS)).thenReturn(true);
        when(store.load(WORKSPACE_ID)).thenThrow(new IllegalStateException("redis unavailable"));
        when(store.load(second)).thenReturn(java.util.Optional.of(secondRecord));
        when(store.activeLeases(second, 3)).thenReturn(List.of());

        sweeper.sweepOnce();

        verify(store).compareAndSet(eq(secondRecord), any(OpenSandboxWorkspaceRecord.class));
    }

    @Test
    void repairCatalogRestoresMissingIdleHintAfterClientCrash() {
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of());
        when(store.claimDueRepairs(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(WORKSPACE_ID));
        when(store.activeLeases(WORKSPACE_ID, 3)).thenReturn(List.of());
        when(store.hasIdle(WORKSPACE_ID)).thenReturn(false);
        Instant repairedExpiry = NOW.plus(RedisOpenSandboxClient.idleRetention(lifecycle));
        when(delegate.renew("sandbox-3", RedisOpenSandboxClient.idleRetention(lifecycle)))
                .thenReturn(repairedExpiry);

        sweeper.sweepOnce();

        verify(delegate).renew("sandbox-3", RedisOpenSandboxClient.idleRetention(lifecycle));
        assertEquals(repairedExpiry, persisted.get().getExpiresAt());
        verify(store).scheduleIdle(WORKSPACE_ID, NOW);
    }

    @Test
    void repairPassRenewsIdleEntryBeyondClaimedBatchCapacity() {
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of());
        when(store.claimDueRepairs(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(WORKSPACE_ID));
        when(store.activeLeases(WORKSPACE_ID, 3)).thenReturn(List.of());
        when(store.hasIdle(WORKSPACE_ID)).thenReturn(true);
        Instant extended = NOW.plus(RedisOpenSandboxClient.idleRetention(lifecycle));
        when(delegate.renew("sandbox-3", RedisOpenSandboxClient.idleRetention(lifecycle)))
                .thenReturn(extended);

        sweeper.sweepOnce();

        verify(delegate).renew("sandbox-3", RedisOpenSandboxClient.idleRetention(lifecycle));
        assertEquals(extended, persisted.get().getExpiresAt());
        verify(store, never()).scheduleIdle(eq(WORKSPACE_ID), any());
    }

    @Test
    void repairCatalogCompletesAbortedExplicitDeleteFromTombstone() {
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of());
        when(store.claimDueRepairs(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(WORKSPACE_ID));
        OpenSandboxWorkspaceRecord deleted = runningRecord();
        deleted.setNativeSnapshotId("snapshot-current");
        deleted.setPreviousNativeSnapshotId("snapshot-previous");
        persisted.set(deleted);
        when(store.deletedThroughGeneration(WORKSPACE_ID)).thenReturn(3L);
        OpenSandboxState duplicate = remoteInfo("RUNNING");
        duplicate.setSandboxId("sandbox-duplicate");
        duplicate.setMetadata(Map.of("agentscope.generation", "3"));
        when(delegate.listByMetadata(any())).thenReturn(List.of(duplicate));
        when(delegate.listNativeSnapshotDetailsByNamePrefix(any()))
                .thenReturn(Map.of("snapshot-prefix", snapshot("snapshot-prefix", 3, NOW)));

        sweeper.sweepOnce();

        verify(store).markOrphanSandbox(WORKSPACE_ID, "sandbox-3", NOW);
        verify(store).markOrphanSandbox(WORKSPACE_ID, "sandbox-duplicate", NOW);
        verify(store).markOrphanSnapshot(WORKSPACE_ID, "snapshot-current", NOW);
        verify(store).markOrphanSnapshot(WORKSPACE_ID, "snapshot-previous", NOW);
        verify(store).markOrphanSnapshot(WORKSPACE_ID, "snapshot-prefix", NOW);
        assertEquals(null, persisted.get());
        verify(store).clearLeases(WORKSPACE_ID);
        verify(store).cancelIdle(WORKSPACE_ID);
        verify(store).cancelRepair(WORKSPACE_ID);
    }

    @Test
    void repairCatalogCompletesTombstonedCleanupWhenRecordWasAlreadyRemoved() {
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of());
        when(store.claimDueRepairs(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(WORKSPACE_ID));
        persisted.set(null);
        when(store.deletedThroughGeneration(WORKSPACE_ID)).thenReturn(3L);
        OpenSandboxState old = remoteInfo("RUNNING");
        old.setMetadata(Map.of("agentscope.generation", "3"));
        when(delegate.listByMetadata(any())).thenReturn(List.of(old));
        when(delegate.listNativeSnapshotDetailsByNamePrefix(any()))
                .thenReturn(Map.of("snapshot-old", snapshot("snapshot-old", 3, NOW)));

        sweeper.sweepOnce();

        verify(store).markOrphanSandbox(WORKSPACE_ID, "sandbox-3", NOW);
        verify(store).markOrphanSnapshot(WORKSPACE_ID, "snapshot-old", NOW);
        verify(store).clearLeases(WORKSPACE_ID);
        verify(store).cancelIdle(WORKSPACE_ID);
        verify(store).cancelRepair(WORKSPACE_ID);
    }

    @Test
    void repairCatalogRetainsTombstoneWhenDuplicateGenerationIsMalformed() {
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of());
        when(store.claimDueRepairs(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(WORKSPACE_ID));
        persisted.set(null);
        when(store.deletedThroughGeneration(WORKSPACE_ID)).thenReturn(3L);
        OpenSandboxState duplicate = remoteInfo("RUNNING");
        duplicate.setSandboxId("sandbox-duplicate");
        duplicate.setMetadata(Map.of("agentscope.generation", "not-a-generation"));
        when(delegate.listByMetadata(any())).thenReturn(List.of(duplicate));

        sweeper.sweepOnce();

        verify(store, never()).markOrphanSandbox(any(), any(), any());
        verify(store, never()).clearLeases(WORKSPACE_ID);
        verify(store, never()).cancelRepair(WORKSPACE_ID);
        verify(delegate, never()).resume(any());
        verify(delegate, never()).delete(any());
    }

    @Test
    void repairRejectsNegativePersistedGenerationBeforeCleanup() {
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of());
        when(store.claimDueRepairs(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(WORKSPACE_ID));
        OpenSandboxWorkspaceRecord invalid = runningRecord();
        invalid.setGeneration(-1);
        persisted.set(invalid);
        when(store.deletedThroughGeneration(WORKSPACE_ID)).thenReturn(3L);
        OpenSandboxState duplicate = remoteInfo("RUNNING");
        duplicate.setSandboxId("sandbox-duplicate");
        duplicate.setMetadata(Map.of("agentscope.generation", "2"));
        when(delegate.listByMetadata(any())).thenReturn(List.of(duplicate));
        when(delegate.listNativeSnapshotDetailsByNamePrefix(any())).thenReturn(Map.of());

        sweeper.sweepOnce();

        assertEquals(invalid, persisted.get());
        verify(store, never()).reconcileSnapshotReferences(any());
        verify(store, never()).markOrphanSandbox(any(), any(), any());
        verify(store, never()).markOrphanSnapshot(any(), any(), any());
        verify(store, never()).compareAndSet(any(), any());
        verify(store, never()).clearLeases(any());
        verify(store, never()).cancelIdle(any());
        verify(store, never()).cancelRepair(any());
        verifyNoInteractions(delegate);
    }

    @Test
    void lifecycleLockIsImmediateAndPreventsDuplicateAction() throws Exception {
        when(lock.tryLock(0, TimeUnit.SECONDS)).thenReturn(false);

        sweeper.sweepOnce();

        verify(store, never()).load(WORKSPACE_ID);
        verify(lock, never()).unlock();
    }

    @Test
    void claimedLockConflictsCannotStarveEntryAfterFirstHundred() throws Exception {
        List<String> blocked = new java.util.ArrayList<>();
        for (int index = 0; index < OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE; index++) {
            String workspaceId = "blocked-" + index;
            blocked.add(workspaceId);
            RLock blockedLock = mock(RLock.class);
            when(store.lifecycleLock(workspaceId)).thenReturn(blockedLock);
            when(blockedLock.tryLock(0, TimeUnit.SECONDS)).thenReturn(false);
        }
        String later = "workspace-101";
        OpenSandboxWorkspaceRecord laterRecord = runningRecord();
        laterRecord.setWorkspaceId(later);
        RLock laterLock = mock(RLock.class);
        when(laterLock.tryLock(0, TimeUnit.SECONDS)).thenReturn(true);
        when(store.lifecycleLock(later)).thenReturn(laterLock);
        when(store.load(later)).thenReturn(java.util.Optional.of(laterRecord));
        when(store.activeLeases(later, 3)).thenReturn(List.of());
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(blocked, List.of(later));

        sweeper.sweepOnce();
        sweeper.sweepOnce();

        verify(store).compareAndSet(eq(laterRecord), any(OpenSandboxWorkspaceRecord.class));
    }

    @Test
    void closeStopsOnlyInjectedScheduler() {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        OpenSandboxLifecycleSweeper owned =
                new OpenSandboxLifecycleSweeper(
                        delegate, store, lifecycle, Clock.fixed(NOW, ZoneOffset.UTC), scheduler);

        owned.close();

        verify(scheduler).shutdownNow();
        verify(store, never()).close();
    }

    @Test
    void duplicateAndUnreferencedSnapshotAreCleanedOnlyAfterOrphanGrace() {
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of());
        OpenSandboxWorkspaceStore.OrphanReference duplicate =
                new OpenSandboxWorkspaceStore.OrphanReference(WORKSPACE_ID, "sandbox-loser");
        OpenSandboxWorkspaceStore.OrphanReference orphanSnapshot =
                new OpenSandboxWorkspaceStore.OrphanReference(WORKSPACE_ID, "snapshot-orphan");
        Instant cutoff = NOW.minus(lifecycle.getOrphanGrace());
        when(store.claimDueOrphanSandboxes(
                        cutoff,
                        cutoff.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(duplicate));
        when(store.claimDueOrphanSnapshots(
                        cutoff,
                        cutoff.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(orphanSnapshot));
        when(store.load(WORKSPACE_ID)).thenReturn(java.util.Optional.of(runningRecord()));
        Sandbox duplicateHandle = mock(Sandbox.class);
        when(delegate.resume(any(OpenSandboxState.class))).thenReturn(duplicateHandle);

        sweeper.sweepOnce();

        verify(store)
                .claimDueOrphanSandboxes(
                        cutoff,
                        cutoff.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE);
        verify(delegate).delete(duplicateHandle);
        verify(store).removeOrphanSandbox(duplicate);
        verify(delegate).deleteNativeSnapshot("snapshot-orphan");
        verify(store).removeOrphanSnapshot(orphanSnapshot);
    }

    @Test
    void referencedCurrentOrPreviousSnapshotIsNeverDeleted() throws Exception {
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of());
        OpenSandboxWorkspaceRecord record = runningRecord();
        record.setNativeSnapshotId("snapshot-current");
        record.setPreviousNativeSnapshotId("snapshot-previous");
        when(store.load(WORKSPACE_ID)).thenReturn(java.util.Optional.of(record));
        when(store.isSnapshotReferenced("snapshot-current")).thenReturn(true);
        when(store.isSnapshotReferenced("snapshot-previous")).thenReturn(true);
        Instant cutoff = NOW.minus(lifecycle.getOrphanGrace());
        OpenSandboxWorkspaceStore.OrphanReference current =
                new OpenSandboxWorkspaceStore.OrphanReference(WORKSPACE_ID, "snapshot-current");
        OpenSandboxWorkspaceStore.OrphanReference previous =
                new OpenSandboxWorkspaceStore.OrphanReference(WORKSPACE_ID, "snapshot-previous");
        when(store.claimDueOrphanSnapshots(
                        cutoff,
                        cutoff.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(current, previous));

        sweeper.sweepOnce();

        verify(lock, times(2)).tryLock(0, TimeUnit.SECONDS);
        verify(delegate, never()).deleteNativeSnapshot(any());
        verify(store, never()).removeOrphanSnapshot(current);
        verify(store, never()).removeOrphanSnapshot(previous);
    }

    @Test
    void duplicateCleanupRechecksCurrentSandboxUnderLifecycleLock() throws Exception {
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of());
        Instant cutoff = NOW.minus(lifecycle.getOrphanGrace());
        OpenSandboxWorkspaceStore.OrphanReference candidate =
                new OpenSandboxWorkspaceStore.OrphanReference(WORKSPACE_ID, "sandbox-3");
        when(store.claimDueOrphanSandboxes(
                        cutoff,
                        cutoff.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(candidate));
        when(store.load(WORKSPACE_ID)).thenReturn(java.util.Optional.of(runningRecord()));

        sweeper.sweepOnce();

        verify(lock).tryLock(0, TimeUnit.SECONDS);
        verify(delegate, never()).delete(any());
        verify(store).removeOrphanSandbox(candidate);
    }

    @Test
    void duplicateCleanupDefersLoserWhileSameGenerationHasActiveLease() {
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of());
        Instant cutoff = NOW.minus(lifecycle.getOrphanGrace());
        OpenSandboxWorkspaceStore.OrphanReference loser =
                new OpenSandboxWorkspaceStore.OrphanReference(WORKSPACE_ID, "sandbox-loser");
        when(store.claimDueOrphanSandboxes(
                        cutoff,
                        cutoff.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(loser));
        when(store.load(WORKSPACE_ID)).thenReturn(java.util.Optional.of(runningRecord()));
        OpenSandboxState loserInfo = remoteInfo("RUNNING");
        loserInfo.setSandboxId("sandbox-loser");
        loserInfo.setMetadata(Map.of("agentscope.generation", "3"));
        when(delegate.describe("sandbox-loser")).thenReturn(loserInfo);
        when(store.activeLeases(WORKSPACE_ID, 3))
                .thenReturn(
                        List.of(
                                new OpenSandboxActiveLease(
                                        "lease", WORKSPACE_ID, 3, "node", NOW, NOW)),
                        List.of());
        Sandbox duplicateHandle = mock(Sandbox.class);
        when(delegate.resume(any(OpenSandboxState.class))).thenReturn(duplicateHandle);

        sweeper.sweepOnce();

        verify(delegate, never()).delete(any());
        verify(store, never()).removeOrphanSandbox(loser);

        sweeper.sweepOnce();

        verify(delegate).delete(duplicateHandle);
        verify(store).removeOrphanSandbox(loser);
    }

    @Test
    void duplicateCleanupRetainsOrphanWithNegativeGeneration() {
        assertInvalidOrphanGenerationIsRetained("-1");
    }

    @Test
    void duplicateCleanupRetainsOrphanWithMaximumGeneration() {
        assertInvalidOrphanGenerationIsRetained(Long.toString(Long.MAX_VALUE));
    }

    @Test
    void snapshotCleanupChecksGlobalReferencesBeforeDeleting() {
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of());
        Instant cutoff = NOW.minus(lifecycle.getOrphanGrace());
        OpenSandboxWorkspaceStore.OrphanReference orphan =
                new OpenSandboxWorkspaceStore.OrphanReference(WORKSPACE_ID, "snapshot-shared");
        when(store.claimDueOrphanSnapshots(
                        cutoff,
                        cutoff.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(orphan));
        when(store.isSnapshotReferenced("snapshot-shared")).thenReturn(true, false);

        sweeper.sweepOnce();

        verify(delegate, never()).deleteNativeSnapshot(any());

        sweeper.sweepOnce();

        verify(delegate).deleteNativeSnapshot("snapshot-shared");
        verify(store).removeOrphanSnapshot(orphan);
    }

    @Test
    void snapshotNotFoundCompletesRetriedCleanup() {
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of());
        Instant cutoff = NOW.minus(lifecycle.getOrphanGrace());
        OpenSandboxWorkspaceStore.OrphanReference orphan =
                new OpenSandboxWorkspaceStore.OrphanReference(WORKSPACE_ID, "snapshot-gone");
        when(store.claimDueOrphanSnapshots(
                        cutoff,
                        cutoff.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(orphan));
        RuntimeException notFound = new IllegalStateException("not found");
        doThrow(notFound).when(delegate).deleteNativeSnapshot("snapshot-gone");
        when(delegate.isNotFound(notFound)).thenReturn(true);

        sweeper.sweepOnce();

        verify(store).removeOrphanSnapshot(orphan);
    }

    @Test
    void remoteDeleteThenMarkerFailureConvergesThroughNotFoundRetry() {
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of());
        Instant cutoff = NOW.minus(lifecycle.getOrphanGrace());
        OpenSandboxWorkspaceStore.OrphanReference orphan =
                new OpenSandboxWorkspaceStore.OrphanReference(WORKSPACE_ID, "snapshot-retry");
        when(store.claimDueOrphanSnapshots(
                        cutoff,
                        cutoff.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(orphan));
        RuntimeException notFound = new IllegalStateException("not found");
        org.mockito.Mockito.doNothing()
                .doThrow(notFound)
                .when(delegate)
                .deleteNativeSnapshot("snapshot-retry");
        when(delegate.isNotFound(notFound)).thenReturn(true);
        doThrow(new IllegalStateException("redis remove failed"))
                .doNothing()
                .when(store)
                .removeOrphanSnapshot(orphan);

        sweeper.sweepOnce();
        sweeper.sweepOnce();

        verify(delegate, times(2)).deleteNativeSnapshot("snapshot-retry");
        verify(store, times(2)).removeOrphanSnapshot(orphan);
    }

    private void readySnapshot(String snapshotId) {
        when(delegate.createNativeSnapshot(any(), any())).thenReturn(snapshotId);
        when(delegate.waitForNativeSnapshotReady(snapshotId, lifecycle.getSnapshotReadyTimeout()))
                .thenReturn(snapshotId);
    }

    private void assertInvalidOrphanGenerationIsRetained(String generation) {
        when(store.claimDueIdle(
                        NOW,
                        NOW.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of());
        Instant cutoff = NOW.minus(lifecycle.getOrphanGrace());
        OpenSandboxWorkspaceStore.OrphanReference orphan =
                new OpenSandboxWorkspaceStore.OrphanReference(WORKSPACE_ID, "sandbox-invalid");
        when(store.claimDueOrphanSandboxes(
                        cutoff,
                        cutoff.plus(lifecycle.getSweepInterval()),
                        OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE))
                .thenReturn(List.of(orphan));
        OpenSandboxState invalid = remoteInfo("RUNNING");
        invalid.setSandboxId("sandbox-invalid");
        invalid.setMetadata(Map.of("agentscope.generation", generation));
        when(delegate.describe("sandbox-invalid")).thenReturn(invalid);

        sweeper.sweepOnce();

        verify(delegate, never()).resume(any());
        verify(delegate, never()).delete(any());
        verify(store, never()).removeOrphanSandbox(orphan);
    }

    private static NativeSnapshot snapshot(String snapshotId, long generation, Instant createdAt) {
        return snapshot(snapshotId, generation, createdAt, "READY");
    }

    private static NativeSnapshot snapshot(
            String snapshotId, long generation, Instant createdAt, String status) {
        return new NativeSnapshot(
                snapshotId,
                OpenSandboxLifecycleSweeper.snapshotName(WORKSPACE_ID, generation, createdAt),
                createdAt,
                status);
    }

    private static OpenSandboxWorkspaceRecord runningRecord() {
        OpenSandboxWorkspaceRecord record = new OpenSandboxWorkspaceRecord();
        record.setWorkspaceId(WORKSPACE_ID);
        record.setSandboxId("sandbox-3");
        record.setGeneration(3);
        record.setLifecycleState(OpenSandboxWorkspaceRecord.LifecycleState.RUNNING);
        record.setLastAccessAt(NOW.minus(Duration.ofHours(1)));
        return record;
    }

    private static OpenSandboxWorkspaceRecord pendingRecord() {
        OpenSandboxWorkspaceRecord record = runningRecord();
        record.setLifecycleState(OpenSandboxWorkspaceRecord.LifecycleState.EVICTION_PENDING);
        record.setEvictionCandidateAt(NOW.minus(Duration.ofMinutes(5)));
        return record;
    }

    private static OpenSandboxState remoteInfo(String status) {
        OpenSandboxState state = new OpenSandboxState();
        state.setSandboxId("sandbox-3");
        state.setRemoteStatus(status);
        return state;
    }
}
