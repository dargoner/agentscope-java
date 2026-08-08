/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.agentscope.extensions.sandbox.opensandbox.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandbox;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClient;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClientOptions;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxState;
import io.agentscope.harness.agent.IsolationScope;
import io.agentscope.harness.agent.sandbox.SandboxException;
import io.agentscope.harness.agent.sandbox.SandboxIsolationKey;
import io.agentscope.harness.agent.sandbox.WorkspaceSpec;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.redisson.api.RLock;

class RedisOpenSandboxClientTest {
    private static final Instant NOW = Instant.parse("2026-08-08T08:00:00Z");

    private OpenSandboxClient delegate;
    private OpenSandboxWorkspaceStore store;
    private RLock lock;
    private ScheduledExecutorService scheduler;
    private OpenSandboxClientOptions options;
    private OpenSandboxRedisLifecycleOptions lifecycle;
    private AtomicReference<OpenSandboxWorkspaceRecord> record;
    private RedisOpenSandboxClient client;

    @BeforeEach
    void setUp() throws Exception {
        delegate = mock(OpenSandboxClient.class);
        store = mock(OpenSandboxWorkspaceStore.class);
        lock = mock(RLock.class);
        scheduler = mock(ScheduledExecutorService.class);
        options = new OpenSandboxClientOptions();
        options.setEndpoint("http://default-a.internal:8080");
        options.setApiKey("secret-token-a");
        lifecycle = new OpenSandboxRedisLifecycleOptions();
        record = new AtomicReference<>();
        when(store.lifecycleLock(any())).thenReturn(lock);
        when(lock.tryLock(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(true);
        when(store.load(any())).thenAnswer(invocation -> Optional.ofNullable(record.get()));
        when(store.compareAndSet(
                        nullable(OpenSandboxWorkspaceRecord.class),
                        nullable(OpenSandboxWorkspaceRecord.class)))
                .thenAnswer(
                        invocation -> {
                            OpenSandboxWorkspaceRecord expected = invocation.getArgument(0);
                            OpenSandboxWorkspaceRecord update = invocation.getArgument(1);
                            if (record.get() != expected) return false;
                            record.set(update);
                            return true;
                        });
        when(delegate.serializeState(any())).thenReturn("serialized-state");
        client =
                new RedisOpenSandboxClient(
                        delegate,
                        store,
                        options,
                        lifecycle,
                        Clock.fixed(NOW, ZoneOffset.UTC),
                        scheduler,
                        "instance-a");
    }

    @Test
    void sameWorkspaceBorrowsOneRemoteSandboxWithIndependentLeases() throws Exception {
        WorkspaceSpec spec = workspace();
        OpenSandbox firstRemote = remote("sandbox-1", spec);
        OpenSandbox secondHandle = remote("sandbox-1", spec);
        OpenSandboxState firstState = (OpenSandboxState) firstRemote.getState();
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any())).thenReturn(firstRemote);
        when(delegate.deserializeState("serialized-state")).thenReturn(firstState);
        when(delegate.resume(any(OpenSandboxState.class))).thenReturn(secondHandle);
        when(delegate.describe("sandbox-1")).thenReturn(info("sandbox-1", "RUNNING"));

        RedisManagedOpenSandbox first =
                (RedisManagedOpenSandbox)
                        client.create(spec, null, options, isolationKey("user-1"), "agent-a");
        RedisManagedOpenSandbox second =
                (RedisManagedOpenSandbox)
                        client.create(spec, null, options, isolationKey("user-1"), "agent-a");

        assertNotEquals(first.leaseId(), second.leaseId());
        assertEquals(first.workspaceId(), second.workspaceId());
        verify(delegate, times(1)).create(any(WorkspaceSpec.class), isNull(), any());
        verify(delegate, times(1)).resume(any(OpenSandboxState.class));
        verify(firstRemote).start();
        verify(secondHandle).startExisting();
        ArgumentCaptor<OpenSandboxActiveLease> leases =
                ArgumentCaptor.forClass(OpenSandboxActiveLease.class);
        verify(store, times(2)).putLease(leases.capture(), eq(lifecycle.getActiveLeaseTtl()));
        assertNotEquals(
                leases.getAllValues().get(0).leaseId(), leases.getAllValues().get(1).leaseId());
    }

    @Test
    void pausedWorkspaceIsResumedOnceBeforeConnectingHandle() throws Exception {
        WorkspaceSpec spec = workspace();
        OpenSandboxWorkspaceRecord paused = persistedRecord(spec, "PAUSED");
        record.set(paused);
        OpenSandbox handle = remote("sandbox-1", spec);
        OpenSandboxState handleState = (OpenSandboxState) handle.getState();
        when(delegate.describe("sandbox-1")).thenReturn(info("sandbox-1", "PAUSED"));
        when(delegate.deserializeState("serialized-state")).thenReturn(handleState);
        when(delegate.resume(any(OpenSandboxState.class))).thenReturn(handle);

        client.create(spec, null, options, isolationKey("user-1"), "agent-a");

        verify(delegate, times(1)).resumeRemote("sandbox-1");
        verify(handle).startExisting();
    }

    @Test
    void resumeNotFoundImmediatelyRebuildsFromSnapshots() throws Exception {
        WorkspaceSpec spec = workspace();
        OpenSandboxWorkspaceRecord paused = persistedRecord(spec, "PAUSED");
        paused.setNativeSnapshotId("snapshot-current");
        paused.setPreviousNativeSnapshotId("snapshot-previous");
        record.set(paused);
        SandboxException.SandboxRuntimeException notFound =
                new SandboxException.SandboxRuntimeException(
                        "resume missing", new IllegalStateException());
        when(delegate.describe("sandbox-1")).thenReturn(info("sandbox-1", "PAUSED"));
        doThrow(notFound).when(delegate).resumeRemote("sandbox-1");
        when(delegate.isNotFound(notFound)).thenReturn(true);
        OpenSandbox current = remote("sandbox-current", spec);
        OpenSandbox previous = remote("sandbox-restored", spec);
        doThrow(new IllegalStateException("current failed")).when(current).start();
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any()))
                .thenReturn(current, previous);

        client.create(spec, null, options, isolationKey("user-1"), "agent-a");

        ArgumentCaptor<OpenSandboxClientOptions> attempts =
                ArgumentCaptor.forClass(OpenSandboxClientOptions.class);
        verify(delegate, times(2)).create(any(WorkspaceSpec.class), isNull(), attempts.capture());
        assertEquals(
                List.of("snapshot-current", "snapshot-previous"),
                attempts.getAllValues().stream()
                        .map(OpenSandboxClientOptions::getRestoreSnapshotId)
                        .toList());
        assertEquals("sandbox-restored", record.get().getSandboxId());
        verify(delegate, never()).deserializeState(any());
    }

    @Test
    void runningRemoteOverridesStalePausedRecordWithoutResumingAgain() throws Exception {
        WorkspaceSpec spec = workspace();
        OpenSandboxWorkspaceRecord paused = persistedRecord(spec, "PAUSED");
        record.set(paused);
        OpenSandbox handle = remote("sandbox-1", spec);
        OpenSandboxState handleState = (OpenSandboxState) handle.getState();
        when(delegate.describe("sandbox-1")).thenReturn(info("sandbox-1", "RUNNING"));
        when(delegate.deserializeState("serialized-state")).thenReturn(handleState);
        when(delegate.resume(any(OpenSandboxState.class))).thenReturn(handle);

        client.create(spec, null, options, isolationKey("user-1"), "agent-a");

        verify(delegate, never()).resumeRemote(any());
        verify(handle).startExisting();
    }

    @Test
    void differentPerCallControlPlaneIsRejectedBeforeLifecycleAccess() throws Exception {
        OpenSandboxClientOptions tenant = OpenSandboxClientOptions.copyOf(options);
        tenant.setEndpoint("http://tenant-b.internal:8080");
        tenant.setApiKey("secret-token-b");
        OpenSandbox remote = remote("sandbox-1", workspace());
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any())).thenReturn(remote);

        SandboxException.SandboxConfigurationException error =
                assertThrows(
                        SandboxException.SandboxConfigurationException.class,
                        () ->
                                client.create(
                                        workspace(),
                                        null,
                                        tenant,
                                        isolationKey("user-1"),
                                        "agent-a"));

        assertFalse(error.getMessage().contains("secret-token-a"));
        assertFalse(error.getMessage().contains("secret-token-b"));
        verify(store, never()).lifecycleLock(any());
        verify(delegate, never()).create(any(), any(), any());
    }

    @Test
    void matchingControlPlaneStillAllowsPerCallRuntimeFields() throws Exception {
        OpenSandboxClientOptions runtime = OpenSandboxClientOptions.copyOf(options);
        runtime.setImage("runtime-b:latest");
        runtime.setEntrypoint(List.of("/bin/runtime-b"));
        OpenSandbox remote = remote("sandbox-1", workspace());
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any())).thenReturn(remote);

        client.create(workspace(), null, runtime, isolationKey("user-1"), "agent-a");

        ArgumentCaptor<OpenSandboxClientOptions> captured =
                ArgumentCaptor.forClass(OpenSandboxClientOptions.class);
        verify(delegate).create(any(WorkspaceSpec.class), isNull(), captured.capture());
        assertEquals("runtime-b:latest", captured.getValue().getImage());
        assertEquals(List.of("/bin/runtime-b"), captured.getValue().getEntrypoint());
    }

    @Test
    void contextualResumeUsesClientDefaultControlPlane() throws Exception {
        WorkspaceSpec spec = workspace();
        record.set(persistedRecord(spec, "RUNNING"));
        OpenSandbox handle = remote("sandbox-1", spec);
        OpenSandboxState handleState = (OpenSandboxState) handle.getState();
        when(delegate.describe("sandbox-1")).thenReturn(info("sandbox-1", "RUNNING"));
        when(delegate.deserializeState("serialized-state")).thenReturn(handleState);
        when(delegate.resume(any(OpenSandboxState.class))).thenReturn(handle);
        OpenSandboxState requested = new OpenSandboxState();
        requested.setWorkspaceSpec(spec);

        client.resume(requested, isolationKey("user-1"), "agent-a");

        verify(delegate).describe("sandbox-1");
        verify(delegate).resume(any(OpenSandboxState.class));
    }

    @Test
    void lockTimeoutFailsClosedBeforeRemoteCreate() throws Exception {
        when(lock.tryLock(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(false);

        assertThrows(
                SandboxException.SandboxRuntimeException.class,
                () -> client.create(workspace(), null, options, isolationKey("user-1"), "agent-a"));

        verify(delegate, never()).create(any(), any(), any());
        verify(delegate, never()).resumeRemote(any());
    }

    @Test
    void missingSandboxFallsBackFromCurrentToPreviousSnapshotWithoutImageCreate() throws Exception {
        WorkspaceSpec spec = workspace();
        OpenSandboxWorkspaceRecord missing = persistedRecord(spec, "RUNNING");
        missing.setNativeSnapshotId("snapshot-current");
        missing.setPreviousNativeSnapshotId("snapshot-previous");
        missing.setRuntimeProfileHash(RedisOpenSandboxClient.runtimeProfileHash(options));
        record.set(missing);
        SandboxException.SandboxRuntimeException notFound =
                new SandboxException.SandboxRuntimeException(
                        "missing", new IllegalStateException());
        when(delegate.describe("sandbox-1")).thenThrow(notFound);
        when(delegate.isNotFound(notFound)).thenReturn(true);
        OpenSandbox current = remote("sandbox-current", spec);
        OpenSandbox previous = remote("sandbox-restored", spec);
        doThrow(new IllegalStateException("current failed")).when(current).start();
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any()))
                .thenReturn(current, previous);

        client.create(spec, null, options, isolationKey("user-1"), "agent-a");

        ArgumentCaptor<OpenSandboxClientOptions> attempts =
                ArgumentCaptor.forClass(OpenSandboxClientOptions.class);
        verify(delegate, times(2)).create(any(WorkspaceSpec.class), isNull(), attempts.capture());
        assertEquals(
                List.of("snapshot-current", "snapshot-previous"),
                attempts.getAllValues().stream()
                        .map(OpenSandboxClientOptions::getRestoreSnapshotId)
                        .toList());
        assertEquals("sandbox-restored", record.get().getSandboxId());
    }

    @Test
    void snapshotStartFailureIsDeletedAndPreservesCleanupFailure() throws Exception {
        WorkspaceSpec spec = workspace();
        OpenSandboxWorkspaceRecord missing = persistedRecord(spec, "RUNNING");
        missing.setNativeSnapshotId("snapshot-current");
        missing.setPreviousNativeSnapshotId("snapshot-previous");
        record.set(missing);
        SandboxException.SandboxRuntimeException notFound =
                new SandboxException.SandboxRuntimeException(
                        "missing", new IllegalStateException());
        when(delegate.describe("sandbox-1")).thenThrow(notFound);
        when(delegate.isNotFound(notFound)).thenReturn(true);
        OpenSandbox current = remote("sandbox-current", spec);
        OpenSandbox previous = remote("sandbox-previous", spec);
        IllegalStateException startFailure = new IllegalStateException("current start failed");
        IllegalStateException cleanupFailure = new IllegalStateException("current delete failed");
        IllegalStateException previousFailure = new IllegalStateException("previous start failed");
        doThrow(startFailure).when(current).start();
        doThrow(cleanupFailure).when(delegate).delete(current);
        doThrow(previousFailure).when(previous).start();
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any()))
                .thenReturn(current, previous);

        SandboxException.SandboxRuntimeException error =
                assertThrows(
                        SandboxException.SandboxRuntimeException.class,
                        () ->
                                client.create(
                                        spec, null, options, isolationKey("user-1"), "agent-a"));

        assertSame(startFailure, error.getCause());
        assertTrue(List.of(startFailure.getSuppressed()).contains(cleanupFailure));
        verify(delegate).delete(current);
        verify(previous).start();
    }

    @Test
    void connectNotFoundFallsBackThroughSnapshotsWithoutImageCreate() throws Exception {
        WorkspaceSpec spec = workspace();
        OpenSandboxWorkspaceRecord existing = persistedRecord(spec, "RUNNING");
        existing.setNativeSnapshotId("snapshot-current");
        existing.setPreviousNativeSnapshotId("snapshot-previous");
        record.set(existing);
        OpenSandbox disconnected = remote("sandbox-1", spec);
        OpenSandboxState disconnectedState = (OpenSandboxState) disconnected.getState();
        SandboxException.SandboxRuntimeException notFound =
                new SandboxException.SandboxRuntimeException(
                        "connect missing", new IllegalStateException());
        when(delegate.describe("sandbox-1")).thenReturn(info("sandbox-1", "RUNNING"));
        when(delegate.deserializeState("serialized-state")).thenReturn(disconnectedState);
        when(delegate.resume(any(OpenSandboxState.class))).thenReturn(disconnected);
        doThrow(notFound).when(disconnected).startExisting();
        when(delegate.isNotFound(notFound)).thenReturn(true);
        OpenSandbox current = remote("snapshot-current-handle", spec);
        OpenSandbox previous = remote("snapshot-previous-handle", spec);
        doThrow(new IllegalStateException("current failed")).when(current).start();
        doThrow(new IllegalStateException("previous failed")).when(previous).start();
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any()))
                .thenReturn(current, previous);

        assertThrows(
                SandboxException.SandboxRuntimeException.class,
                () -> client.create(spec, null, options, isolationKey("user-1"), "agent-a"));

        ArgumentCaptor<OpenSandboxClientOptions> attempts =
                ArgumentCaptor.forClass(OpenSandboxClientOptions.class);
        verify(delegate, times(2)).create(any(WorkspaceSpec.class), isNull(), attempts.capture());
        assertEquals(
                List.of("snapshot-current", "snapshot-previous"),
                attempts.getAllValues().stream()
                        .map(OpenSandboxClientOptions::getRestoreSnapshotId)
                        .toList());
    }

    @Test
    void connectNotFoundWithoutSnapshotsFailsClosedBeforeRemoteCreate() throws Exception {
        WorkspaceSpec spec = workspace();
        record.set(persistedRecord(spec, "RUNNING"));
        OpenSandbox disconnected = remote("sandbox-1", spec);
        OpenSandboxState disconnectedState = (OpenSandboxState) disconnected.getState();
        SandboxException.SandboxRuntimeException notFound =
                new SandboxException.SandboxRuntimeException(
                        "connect missing", new IllegalStateException());
        when(delegate.describe("sandbox-1")).thenReturn(info("sandbox-1", "RUNNING"));
        when(delegate.deserializeState("serialized-state")).thenReturn(disconnectedState);
        when(delegate.resume(any(OpenSandboxState.class))).thenReturn(disconnected);
        doThrow(notFound).when(disconnected).startExisting();
        when(delegate.isNotFound(notFound)).thenReturn(true);

        assertThrows(
                SandboxException.SandboxRuntimeException.class,
                () -> client.create(spec, null, options, isolationKey("user-1"), "agent-a"));

        verify(delegate, never()).create(any(), any(), any());
    }

    @Test
    void tarSnapshotConfigurationIsRejected() {
        assertThrows(
                SandboxException.SandboxConfigurationException.class,
                () ->
                        client.create(
                                workspace(),
                                ignored ->
                                        mock(
                                                io.agentscope.harness.agent.sandbox.snapshot
                                                        .SandboxSnapshot.class),
                                options,
                                isolationKey("user-1"),
                                "agent-a"));
    }

    @Test
    void heartbeatAndStopReleaseOnlyOwnLeaseAndScheduleIdle() throws Exception {
        WorkspaceSpec spec = workspace();
        OpenSandbox remote = remote("sandbox-1", spec);
        ScheduledFuture<?> heartbeat = mock(ScheduledFuture.class);
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any())).thenReturn(remote);
        doReturn(heartbeat)
                .when(scheduler)
                .scheduleAtFixedRate(any(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
        when(store.activeLeases(any(), anyLong())).thenReturn(List.of());
        RedisManagedOpenSandbox managed =
                (RedisManagedOpenSandbox)
                        client.create(spec, null, options, isolationKey("user-1"), "agent-a");

        managed.start();
        managed.stop();

        verify(store, times(2)).putLease(any(), eq(lifecycle.getActiveLeaseTtl()));
        verify(store).removeLease(managed.workspaceId(), managed.leaseId());
        verify(store).scheduleIdle(managed.workspaceId(), NOW.plus(lifecycle.getIdleTtl()));
        verify(remote).disconnect();
    }

    @Test
    void failedReleaseIsRetriedAndEventuallySchedulesIdle() throws Exception {
        WorkspaceSpec spec = workspace();
        OpenSandbox remote = remote("sandbox-1", spec);
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any())).thenReturn(remote);
        when(store.activeLeases(any(), anyLong())).thenReturn(List.of());
        RedisManagedOpenSandbox managed =
                (RedisManagedOpenSandbox)
                        client.create(spec, null, options, isolationKey("user-1"), "agent-a");
        doThrow(new IllegalStateException("redis unavailable"))
                .doNothing()
                .when(store)
                .removeLease(managed.workspaceId(), managed.leaseId());

        assertThrows(IllegalStateException.class, managed::stop);
        managed.stop();

        verify(store, times(2)).removeLease(managed.workspaceId(), managed.leaseId());
        verify(store).scheduleIdle(managed.workspaceId(), NOW.plus(lifecycle.getIdleTtl()));
        verify(remote, times(1)).disconnect();
    }

    @Test
    void stoppedHandleSuppressesLateHeartbeat() throws Exception {
        WorkspaceSpec spec = workspace();
        OpenSandbox remote = remote("sandbox-1", spec);
        ScheduledFuture<?> heartbeat = mock(ScheduledFuture.class);
        ArgumentCaptor<Runnable> scheduled = ArgumentCaptor.forClass(Runnable.class);
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any())).thenReturn(remote);
        doReturn(heartbeat)
                .when(scheduler)
                .scheduleAtFixedRate(
                        scheduled.capture(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
        when(store.activeLeases(any(), anyLong())).thenReturn(List.of());
        RedisManagedOpenSandbox managed =
                (RedisManagedOpenSandbox)
                        client.create(spec, null, options, isolationKey("user-1"), "agent-a");
        managed.start();

        managed.stop();
        scheduled.getValue().run();

        verify(store, times(2)).putLease(any(), eq(lifecycle.getActiveLeaseTtl()));
    }

    @Test
    void staleGenerationHeartbeatDoesNotCancelNewGenerationIdle() throws Exception {
        OpenSandboxWorkspaceRecord current = persistedRecord(workspace(), "RUNNING");
        current.setGeneration(2);
        record.set(current);
        String workspaceId = current.getWorkspaceId();
        OpenSandboxActiveLease stale =
                new OpenSandboxActiveLease("lease-old", workspaceId, 1, "instance-old", NOW, NOW);
        when(scheduler.scheduleAtFixedRate(any(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(mock(ScheduledFuture.class));
        store.scheduleIdle(workspaceId, NOW.plus(lifecycle.getIdleTtl()));

        client.startHeartbeat(stale, new Object(), () -> true);

        verify(store).removeLease(workspaceId, "lease-old");
        verify(store, never()).putLease(eq(stale), any());
        verify(store, never()).cancelIdle(workspaceId);
        verify(store, times(1)).scheduleIdle(workspaceId, NOW.plus(lifecycle.getIdleTtl()));
    }

    @Test
    void staleGenerationReleaseDoesNotCancelNewGenerationIdle() throws Exception {
        OpenSandboxWorkspaceRecord current = persistedRecord(workspace(), "RUNNING");
        current.setGeneration(2);
        record.set(current);
        String workspaceId = current.getWorkspaceId();
        OpenSandboxActiveLease stale =
                new OpenSandboxActiveLease("lease-old", workspaceId, 1, "instance-old", NOW, NOW);
        RedisManagedOpenSandbox managed =
                new RedisManagedOpenSandbox(client, remote("sandbox-old", workspace()), stale);
        store.scheduleIdle(workspaceId, NOW.plus(lifecycle.getIdleTtl()));

        client.release(managed);

        verify(store).removeLease(workspaceId, "lease-old");
        verify(store, never()).cancelIdle(workspaceId);
        verify(store, times(1)).scheduleIdle(workspaceId, NOW.plus(lifecycle.getIdleTtl()));
    }

    @Test
    void reverseReleaseDoesNotOverwriteLatestBorrowState() throws Exception {
        OpenSandboxWorkspaceRecord latest = persistedRecord(workspace(), "RUNNING");
        latest.setSerializedSandboxState("serialized-state-b");
        record.set(latest);
        String workspaceId = latest.getWorkspaceId();
        OpenSandbox turnA = remote("sandbox-1", workspace("/projection-a"));
        OpenSandbox turnB = remote("sandbox-1", workspace("/projection-b"));
        when(delegate.serializeState(turnA.getState())).thenReturn("released-state-a");
        when(delegate.serializeState(turnB.getState())).thenReturn("released-state-b");
        OpenSandboxActiveLease leaseA =
                new OpenSandboxActiveLease("lease-a", workspaceId, 1, "instance-a", NOW, NOW);
        OpenSandboxActiveLease leaseB =
                new OpenSandboxActiveLease("lease-b", workspaceId, 1, "instance-b", NOW, NOW);
        when(store.activeLeases(workspaceId, 1)).thenReturn(List.of());

        client.release(new RedisManagedOpenSandbox(client, turnB, leaseB));
        client.release(new RedisManagedOpenSandbox(client, turnA, leaseA));

        assertEquals("serialized-state-b", record.get().getSerializedSandboxState());
    }

    @Test
    void explicitDeleteKillsRemoteAndClearsWorkspaceRecord() throws Exception {
        WorkspaceSpec spec = workspace();
        OpenSandbox remote = remote("sandbox-1", spec);
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any())).thenReturn(remote);
        when(store.activeLeases(any(), anyLong())).thenReturn(List.of());
        RedisManagedOpenSandbox managed =
                (RedisManagedOpenSandbox)
                        client.create(spec, null, options, isolationKey("user-1"), "agent-a");

        managed.shutdown();
        client.delete(managed);

        verify(delegate).delete(remote);
        verify(store).clearLeases(managed.workspaceId());
        assertEquals(null, record.get());
    }

    @Test
    void explicitDeleteStillKillsAndClearsWhenStopReportsCleanupFailure() throws Exception {
        WorkspaceSpec spec = workspace();
        OpenSandbox remote = remote("sandbox-1", spec);
        ScheduledFuture<?> heartbeat = mock(ScheduledFuture.class);
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any())).thenReturn(remote);
        doReturn(heartbeat)
                .when(scheduler)
                .scheduleAtFixedRate(any(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
        doThrow(new IllegalStateException("cancel failed")).when(heartbeat).cancel(false);
        when(store.activeLeases(any(), anyLong())).thenReturn(List.of());
        RedisManagedOpenSandbox managed =
                (RedisManagedOpenSandbox)
                        client.create(spec, null, options, isolationKey("user-1"), "agent-a");
        managed.start();

        IllegalStateException error =
                assertThrows(IllegalStateException.class, () -> client.delete(managed));

        assertEquals("cancel failed", error.getMessage());
        verify(delegate).delete(remote);
        verify(store).clearLeases(managed.workspaceId());
        assertEquals(null, record.get());
    }

    private OpenSandbox remote(String sandboxId, WorkspaceSpec spec) throws Exception {
        OpenSandbox sandbox = mock(OpenSandbox.class);
        OpenSandboxState state = new OpenSandboxState();
        state.setSandboxId(sandboxId);
        state.setWorkspaceSpec(spec);
        state.setWorkspaceRootReady(true);
        when(sandbox.getState()).thenReturn(state);
        doNothing().when(sandbox).start();
        return sandbox;
    }

    private OpenSandboxWorkspaceRecord persistedRecord(WorkspaceSpec spec, String status) {
        OpenSandboxWorkspaceRecord persisted = new OpenSandboxWorkspaceRecord();
        persisted.setWorkspaceId(
                RedisOpenSandboxClient.workspaceId(isolationKey("user-1"), "agent-a"));
        persisted.setSandboxId("sandbox-1");
        persisted.setSerializedSandboxState("serialized-state");
        persisted.setRuntimeImage(options.getImage());
        persisted.setRuntimeProfileHash(RedisOpenSandboxClient.runtimeProfileHash(options));
        persisted.setLifecycleState(OpenSandboxWorkspaceRecord.LifecycleState.valueOf(status));
        persisted.setGeneration(1);
        return persisted;
    }

    private static OpenSandboxState info(String sandboxId, String status) {
        OpenSandboxState info = new OpenSandboxState();
        info.setSandboxId(sandboxId);
        info.setRemoteStatus(status);
        return info;
    }

    private static WorkspaceSpec workspace() {
        return workspace("/workspace");
    }

    private static WorkspaceSpec workspace(String root) {
        WorkspaceSpec spec = new WorkspaceSpec();
        spec.setRoot(root);
        return spec;
    }

    private static SandboxIsolationKey isolationKey(String userId) {
        RuntimeContext context =
                RuntimeContext.builder().userId(userId).sessionId("session-1").build();
        return SandboxIsolationKey.resolve(IsolationScope.USER, context, "agent-a").orElseThrow();
    }
}
