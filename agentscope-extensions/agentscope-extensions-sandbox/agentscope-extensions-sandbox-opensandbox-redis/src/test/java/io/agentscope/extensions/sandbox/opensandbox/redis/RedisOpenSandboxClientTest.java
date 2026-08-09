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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandbox;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClient;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClient.NativeSnapshot;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClientOptions;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxState;
import io.agentscope.harness.agent.IsolationScope;
import io.agentscope.harness.agent.sandbox.Sandbox;
import io.agentscope.harness.agent.sandbox.SandboxException;
import io.agentscope.harness.agent.sandbox.SandboxIsolationKey;
import io.agentscope.harness.agent.sandbox.WorkspaceSpec;
import io.agentscope.harness.agent.sandbox.layout.FileEntry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
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
        when(store.advanceDeletedThroughGeneration(any(), anyLong()))
                .thenAnswer(invocation -> invocation.getArgument(1, Long.class));
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
        assertEquals(
                RedisOpenSandboxClient.metadataLabelHash(
                        RedisOpenSandboxClient.runtimeProfileHash(runtime)),
                captured.getValue().getMetadata().get("agentscope.runtime-profile"));
    }

    @Test
    void createdSandboxMetadataUsesDockerLabelCompatibleHashValues() throws Exception {
        OpenSandbox remote = remote("sandbox-1", workspace());
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any())).thenReturn(remote);

        client.create(workspace(), null, options, isolationKey("user-111"), "agent-a");

        ArgumentCaptor<OpenSandboxClientOptions> captured =
                ArgumentCaptor.forClass(OpenSandboxClientOptions.class);
        verify(delegate).create(any(WorkspaceSpec.class), isNull(), captured.capture());
        Map<String, String> metadata = captured.getValue().getMetadata();
        for (String key : List.of("agentscope.workspace-id", "agentscope.runtime-profile")) {
            String value = metadata.get(key);
            assertTrue(value.length() <= 63, () -> key + " exceeds the Docker label limit");
            assertTrue(
                    value.matches("[A-Za-z0-9](?:[A-Za-z0-9._-]{0,61}[A-Za-z0-9])?"),
                    () -> key + " is not Docker label compatible: " + value);
        }
    }

    @Test
    void missingRecordDiscoversDeterministicWinnerAndDefersDuplicateCleanup() throws Exception {
        WorkspaceSpec spec = workspace();
        String workspaceId = RedisOpenSandboxClient.workspaceId(isolationKey("user-1"), "agent-a");
        OpenSandboxState generationFour = discovered("sandbox-z", workspaceId, 4, NOW);
        OpenSandboxState laterGenerationFive =
                discovered("sandbox-b", workspaceId, 5, NOW.plusSeconds(20));
        OpenSandboxState tieWinner = discovered("sandbox-a", workspaceId, 5, NOW.plusSeconds(20));
        when(delegate.listByMetadata(
                        Map.of(
                                "agentscope.owner",
                                "opensandbox-redis",
                                "agentscope.workspace-id",
                                RedisOpenSandboxClient.metadataLabelHash(workspaceId))))
                .thenReturn(List.of(generationFour, laterGenerationFive, tieWinner));
        OpenSandbox handle = remote("sandbox-a", spec);
        when(delegate.resume(tieWinner)).thenReturn(handle);

        client.create(spec, null, options, isolationKey("user-1"), "agent-a");

        verify(handle).startExisting();
        verify(store).markOrphanSandbox(workspaceId, "sandbox-z", NOW);
        verify(store).markOrphanSandbox(workspaceId, "sandbox-b", NOW);
        assertEquals("sandbox-a", record.get().getSandboxId());
        assertEquals(5, record.get().getGeneration());
        assertTrue(tieWinner.isWorkspaceRootReady());
        verify(delegate, never()).create(any(), any(), any());
    }

    @Test
    void missingRecordRecoversLatestTwoReadySnapshotsByDeterministicPrefix() throws Exception {
        WorkspaceSpec spec = workspace();
        String workspaceId = RedisOpenSandboxClient.workspaceId(isolationKey("user-1"), "agent-a");
        Map<String, NativeSnapshot> snapshots = new LinkedHashMap<>();
        snapshots.put(
                "snapshot-old", snapshot(workspaceId, "snapshot-old", 1, NOW.minusSeconds(20)));
        snapshots.put("snapshot-current", snapshot(workspaceId, "snapshot-current", 1, NOW));
        snapshots.put(
                "snapshot-previous",
                snapshot(workspaceId, "snapshot-previous", 1, NOW.minusSeconds(10)));
        when(delegate.listByMetadata(any())).thenReturn(List.of());
        when(delegate.listNativeSnapshotDetailsByNamePrefix(
                        OpenSandboxLifecycleSweeper.snapshotNamePrefix(workspaceId)))
                .thenReturn(snapshots);
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
        assertEquals("snapshot-current", record.get().getNativeSnapshotId());
        assertEquals("snapshot-previous", record.get().getPreviousNativeSnapshotId());
    }

    @Test
    void missingRecordRecoversSnapshotCreatedAboveHistoricalTombstone() throws Exception {
        WorkspaceSpec spec = workspace();
        String workspaceId = RedisOpenSandboxClient.workspaceId(isolationKey("user-1"), "agent-a");
        when(store.deletedThroughGeneration(workspaceId)).thenReturn(1L);
        when(delegate.listByMetadata(any())).thenReturn(List.of());
        when(delegate.listNativeSnapshotDetailsByNamePrefix(
                        OpenSandboxLifecycleSweeper.snapshotNamePrefix(workspaceId)))
                .thenReturn(
                        Map.of(
                                "snapshot-generation-1",
                                snapshot(
                                        workspaceId,
                                        "snapshot-generation-1",
                                        1,
                                        NOW.minusSeconds(10)),
                                "snapshot-generation-2",
                                snapshot(workspaceId, "snapshot-generation-2", 2, NOW)));
        OpenSandbox restored = remote("sandbox-restored", spec);
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any())).thenReturn(restored);

        client.create(spec, null, options, isolationKey("user-1"), "agent-a");

        ArgumentCaptor<OpenSandboxClientOptions> createOptions =
                ArgumentCaptor.forClass(OpenSandboxClientOptions.class);
        verify(delegate).create(any(WorkspaceSpec.class), isNull(), createOptions.capture());
        assertEquals("snapshot-generation-2", createOptions.getValue().getRestoreSnapshotId());
        assertEquals(2, record.get().getGeneration());
        assertEquals("snapshot-generation-2", record.get().getNativeSnapshotId());
        verify(store).markOrphanSnapshot(workspaceId, "snapshot-generation-1", NOW);
        verify(store, never()).markOrphanSnapshot(workspaceId, "snapshot-generation-2", NOW);
    }

    @Test
    void missingRecordUsesHighestEligibleSnapshotGeneration() throws Exception {
        WorkspaceSpec spec = workspace();
        String workspaceId = RedisOpenSandboxClient.workspaceId(isolationKey("user-1"), "agent-a");
        when(delegate.listByMetadata(any())).thenReturn(List.of());
        when(delegate.listNativeSnapshotDetailsByNamePrefix(
                        OpenSandboxLifecycleSweeper.snapshotNamePrefix(workspaceId)))
                .thenReturn(
                        Map.of(
                                "snapshot-newer-generation-2",
                                snapshot(workspaceId, "snapshot-newer-generation-2", 2, NOW),
                                "snapshot-older-generation-3",
                                snapshot(
                                        workspaceId,
                                        "snapshot-older-generation-3",
                                        3,
                                        NOW.minusSeconds(10))));
        OpenSandbox restored = remote("sandbox-restored", spec);
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any())).thenReturn(restored);

        client.create(spec, null, options, isolationKey("user-1"), "agent-a");

        ArgumentCaptor<OpenSandboxClientOptions> createOptions =
                ArgumentCaptor.forClass(OpenSandboxClientOptions.class);
        verify(delegate).create(any(WorkspaceSpec.class), isNull(), createOptions.capture());
        assertEquals(
                "snapshot-newer-generation-2", createOptions.getValue().getRestoreSnapshotId());
        assertEquals(3, record.get().getGeneration());
    }

    @Test
    void missingRecordRejectsNegativeSnapshotGenerationBeforeRemoteCreate() {
        WorkspaceSpec spec = workspace();
        String workspaceId = RedisOpenSandboxClient.workspaceId(isolationKey("user-1"), "agent-a");
        when(delegate.listByMetadata(any())).thenReturn(List.of());
        when(delegate.listNativeSnapshotDetailsByNamePrefix(
                        OpenSandboxLifecycleSweeper.snapshotNamePrefix(workspaceId)))
                .thenReturn(
                        Map.of(
                                "snapshot-invalid",
                                snapshot(workspaceId, "snapshot-invalid", -1, NOW)));

        SandboxException.SandboxRuntimeException error =
                assertThrows(
                        SandboxException.SandboxRuntimeException.class,
                        () ->
                                client.create(
                                        spec, null, options, isolationKey("user-1"), "agent-a"));

        assertEquals("Invalid OpenSandbox generation", error.getMessage());
        verify(delegate, never()).create(any(), any(), any());
    }

    @Test
    void missingRecordRejectsGenerationOverflowBeforeRemoteCreate() {
        WorkspaceSpec spec = workspace();
        String workspaceId = RedisOpenSandboxClient.workspaceId(isolationKey("user-1"), "agent-a");
        when(store.deletedThroughGeneration(workspaceId)).thenReturn(Long.MAX_VALUE);
        when(delegate.listByMetadata(any())).thenReturn(List.of());
        when(delegate.listNativeSnapshotDetailsByNamePrefix(any())).thenReturn(Map.of());

        SandboxException.SandboxRuntimeException error =
                assertThrows(
                        SandboxException.SandboxRuntimeException.class,
                        () ->
                                client.create(
                                        spec, null, options, isolationKey("user-1"), "agent-a"));

        assertEquals("Invalid OpenSandbox generation", error.getMessage());
        verify(delegate, never()).create(any(), any(), any());
    }

    @Test
    void missingRecordRejectsNegativeTombstoneBeforeRemoteDiscovery() {
        WorkspaceSpec spec = workspace();
        String workspaceId = RedisOpenSandboxClient.workspaceId(isolationKey("user-1"), "agent-a");
        when(store.deletedThroughGeneration(workspaceId)).thenReturn(-1L);

        SandboxException.SandboxRuntimeException error =
                assertThrows(
                        SandboxException.SandboxRuntimeException.class,
                        () ->
                                client.create(
                                        spec, null, options, isolationKey("user-1"), "agent-a"));

        assertEquals("Invalid OpenSandbox generation", error.getMessage());
        verify(delegate, never()).listByMetadata(any());
        verify(delegate, never()).listNativeSnapshotDetailsByNamePrefix(any());
        verify(delegate, never()).create(any(), any(), any());
        verify(store, never()).compareAndSet(any(), any());
    }

    @Test
    void missingRecordFailsClosedWhenBothDiscoveredSnapshotsFail() throws Exception {
        WorkspaceSpec spec = workspace();
        String workspaceId = RedisOpenSandboxClient.workspaceId(isolationKey("user-1"), "agent-a");
        when(delegate.listByMetadata(any())).thenReturn(List.of());
        when(delegate.listNativeSnapshotDetailsByNamePrefix(
                        OpenSandboxLifecycleSweeper.snapshotNamePrefix(workspaceId)))
                .thenReturn(
                        Map.of(
                                "snapshot-current",
                                snapshot(workspaceId, "snapshot-current", 1, NOW),
                                "snapshot-previous",
                                snapshot(
                                        workspaceId,
                                        "snapshot-previous",
                                        1,
                                        NOW.minusSeconds(10))));
        OpenSandbox current = remote("sandbox-current", spec);
        OpenSandbox previous = remote("sandbox-previous", spec);
        doThrow(new IllegalStateException("current failed")).when(current).start();
        doThrow(new IllegalStateException("previous failed")).when(previous).start();
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any()))
                .thenReturn(current, previous);

        assertThrows(
                SandboxException.SandboxRuntimeException.class,
                () -> client.create(spec, null, options, isolationKey("user-1"), "agent-a"));

        verify(delegate, times(2)).create(any(), isNull(), any());
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
    void describeNotFoundForExistingRecordWithoutSnapshotsFailsClosed() {
        WorkspaceSpec spec = workspace();
        record.set(persistedRecord(spec, "RUNNING"));
        SandboxException.SandboxRuntimeException notFound =
                new SandboxException.SandboxRuntimeException(
                        "missing", new IllegalStateException());
        when(delegate.describe("sandbox-1")).thenThrow(notFound);
        when(delegate.isNotFound(notFound)).thenReturn(true);

        assertThrows(
                SandboxException.SandboxRuntimeException.class,
                () -> client.create(spec, null, options, isolationKey("user-1"), "agent-a"));

        verify(delegate, never()).create(any(), any(), any());
        assertEquals(1, record.get().getGeneration());
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
    void lastReleaseRenewsThroughIdleGraceJitterAndSnapshotWaitBeforeSchedulingIdle()
            throws Exception {
        OpenSandboxWorkspaceRecord current = persistedRecord(workspace(), "RUNNING");
        record.set(current);
        OpenSandboxActiveLease lease =
                new OpenSandboxActiveLease("lease", current.getWorkspaceId(), 1, "node", NOW, NOW);
        RedisManagedOpenSandbox managed =
                new RedisManagedOpenSandbox(client, remote("sandbox-1", workspace()), lease);
        when(store.activeLeases(current.getWorkspaceId(), 1)).thenReturn(List.of());
        Duration retention = RedisOpenSandboxClient.idleRetention(lifecycle);
        Instant expiresAt = NOW.plus(retention);
        when(delegate.renew("sandbox-1", retention)).thenReturn(expiresAt);

        client.release(managed);

        InOrder order = inOrder(delegate, store);
        order.verify(store).scheduleRepair(current.getWorkspaceId(), NOW);
        order.verify(delegate).renew("sandbox-1", retention);
        order.verify(store).compareAndSet(eq(current), any(OpenSandboxWorkspaceRecord.class));
        order.verify(store).removeLease(current.getWorkspaceId(), "lease");
        order.verify(store)
                .scheduleIdle(current.getWorkspaceId(), NOW.plus(lifecycle.getIdleTtl()));
        assertEquals(
                lifecycle
                        .getIdleTtl()
                        .plus(lifecycle.getEvictionGrace())
                        .plus(lifecycle.getSweepInterval().multipliedBy(2))
                        .plus(
                                lifecycle
                                        .getSnapshotReadyTimeout()
                                        .multipliedBy(
                                                OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE)),
                retention);
        assertEquals(
                lifecycle
                        .getSnapshotReadyTimeout()
                        .plus(lifecycle.getSweepInterval().multipliedBy(2)),
                RedisOpenSandboxClient.snapshotAttemptRetention(lifecycle));
        assertEquals(1, OpenSandboxLifecycleSweeper.SNAPSHOT_CONCURRENCY);
        assertEquals(expiresAt, record.get().getExpiresAt());
    }

    @Test
    void failedLastReleaseRenewKeepsLeaseAndDurableRepairHint() throws Exception {
        OpenSandboxWorkspaceRecord current = persistedRecord(workspace(), "RUNNING");
        record.set(current);
        OpenSandboxActiveLease lease =
                new OpenSandboxActiveLease("lease", current.getWorkspaceId(), 1, "node", NOW, NOW);
        RedisManagedOpenSandbox managed =
                new RedisManagedOpenSandbox(client, remote("sandbox-1", workspace()), lease);
        when(store.activeLeases(current.getWorkspaceId(), 1)).thenReturn(List.of());
        when(delegate.renew("sandbox-1", RedisOpenSandboxClient.idleRetention(lifecycle)))
                .thenThrow(new IllegalStateException("renew unavailable"));

        assertThrows(IllegalStateException.class, () -> client.release(managed));

        verify(store).scheduleRepair(current.getWorkspaceId(), NOW);
        verify(store, never()).removeLease(current.getWorkspaceId(), "lease");
        verify(store, never()).compareAndSet(any(), any());
        verify(store, never()).scheduleIdle(any(), any());
    }

    @Test
    void failedReleaseRecordCasKeepsLeaseAndDurableRepairHint() throws Exception {
        OpenSandboxWorkspaceRecord current = persistedRecord(workspace(), "RUNNING");
        record.set(current);
        OpenSandboxActiveLease lease =
                new OpenSandboxActiveLease("lease", current.getWorkspaceId(), 1, "node", NOW, NOW);
        RedisManagedOpenSandbox managed =
                new RedisManagedOpenSandbox(client, remote("sandbox-1", workspace()), lease);
        when(store.activeLeases(current.getWorkspaceId(), 1)).thenReturn(List.of(lease));
        when(delegate.renew("sandbox-1", RedisOpenSandboxClient.idleRetention(lifecycle)))
                .thenReturn(NOW.plus(RedisOpenSandboxClient.idleRetention(lifecycle)));
        when(store.compareAndSet(eq(current), any())).thenReturn(false);

        assertThrows(SandboxException.SandboxRuntimeException.class, () -> client.release(managed));

        verify(store).scheduleRepair(current.getWorkspaceId(), NOW);
        verify(store, never()).removeLease(current.getWorkspaceId(), "lease");
        verify(store, never()).scheduleIdle(any(), any());
    }

    @Test
    void creationLeaseWriteFailureLeavesCommittedRecordForCatalogRepair() throws Exception {
        WorkspaceSpec spec = workspace();
        OpenSandbox remote = remote("sandbox-1", spec);
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any())).thenReturn(remote);
        doThrow(new IllegalStateException("lease write failed"))
                .when(store)
                .putLease(any(), eq(lifecycle.getActiveLeaseTtl()));

        assertThrows(
                IllegalStateException.class,
                () -> client.create(spec, null, options, isolationKey("user-1"), "agent-a"));

        assertEquals("sandbox-1", record.get().getSandboxId());
        verify(remote).disconnect();
        verify(store, never()).scheduleIdle(any(), any());
    }

    @Test
    void redisRecordNeverSerializesWorkspaceContentOrEnvironmentCredentials() throws Exception {
        WorkspaceSpec secretSpec = workspace();
        secretSpec.getEntries().put("secret.txt", new FileEntry("TOP_SECRET_FILE_CONTENT"));
        secretSpec.getEnvironment().put("SERVICE_TOKEN", "TOP_SECRET_ENV_TOKEN");
        OpenSandbox remote = remote("sandbox-1", secretSpec);
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any())).thenReturn(remote);
        when(delegate.serializeState(any())).thenReturn("TOP_SECRET_SERIALIZED_STATE");

        client.create(secretSpec, null, options, isolationKey("user-1"), "agent-a");

        String json =
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .findAndRegisterModules()
                        .writeValueAsString(record.get());
        assertFalse(json.contains("TOP_SECRET_FILE_CONTENT"));
        assertFalse(json.contains("TOP_SECRET_ENV_TOKEN"));
        assertFalse(json.contains("TOP_SECRET_SERIALIZED_STATE"));
        assertFalse(json.contains("serializedSandboxState"));
        verify(delegate, never()).serializeState(any());
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
    void activeHeartbeatRenewsRemoteWhenExpiryIsWithinLead() throws Exception {
        OpenSandboxWorkspaceRecord current = persistedRecord(workspace(), "RUNNING");
        current.setExpiresAt(NOW.plus(lifecycle.getActiveRenewLead()));
        record.set(current);
        OpenSandboxActiveLease lease =
                new OpenSandboxActiveLease(
                        "lease-active",
                        current.getWorkspaceId(),
                        current.getGeneration(),
                        "instance-a",
                        NOW,
                        NOW);
        when(scheduler.scheduleAtFixedRate(any(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(mock(ScheduledFuture.class));
        Instant renewedUntil = NOW.plus(lifecycle.getActiveSandboxTtl());
        when(delegate.renew(current.getSandboxId(), lifecycle.getActiveSandboxTtl()))
                .thenReturn(renewedUntil);

        client.startHeartbeat(lease, new Object(), () -> true);

        verify(delegate).renew(current.getSandboxId(), lifecycle.getActiveSandboxTtl());
        assertEquals(renewedUntil, record.get().getExpiresAt());
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
    void reverseReleaseDoesNotReplaceLatestSandboxIdentity() throws Exception {
        OpenSandboxWorkspaceRecord latest = persistedRecord(workspace(), "RUNNING");
        record.set(latest);
        String workspaceId = latest.getWorkspaceId();
        OpenSandbox turnA = remote("sandbox-1", workspace("/projection-a"));
        OpenSandbox turnB = remote("sandbox-1", workspace("/projection-b"));
        OpenSandboxActiveLease leaseA =
                new OpenSandboxActiveLease("lease-a", workspaceId, 1, "instance-a", NOW, NOW);
        OpenSandboxActiveLease leaseB =
                new OpenSandboxActiveLease("lease-b", workspaceId, 1, "instance-b", NOW, NOW);
        when(store.activeLeases(workspaceId, 1)).thenReturn(List.of());

        client.release(new RedisManagedOpenSandbox(client, turnB, leaseB));
        client.release(new RedisManagedOpenSandbox(client, turnA, leaseA));

        assertEquals("sandbox-1", record.get().getSandboxId());
        verify(delegate, never()).serializeState(any());
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

        InOrder order = inOrder(delegate, store);
        order.verify(store).advanceDeletedThroughGeneration(managed.workspaceId(), 1);
        order.verify(delegate).delete(remote);
        verify(store).clearLeases(managed.workspaceId());
        verify(store).cancelRepair(managed.workspaceId());
        assertEquals(null, record.get());
    }

    @Test
    void explicitDeleteThenImmediateCreateStartsEmptyAboveTombstone() throws Exception {
        WorkspaceSpec spec = workspace();
        OpenSandbox firstRemote = remote("sandbox-1", spec);
        OpenSandbox freshRemote = remote("sandbox-2", spec);
        java.util.concurrent.atomic.AtomicLong deletedThrough =
                new java.util.concurrent.atomic.AtomicLong();
        when(store.deletedThroughGeneration(any())).thenAnswer(ignored -> deletedThrough.get());
        when(store.advanceDeletedThroughGeneration(any(), anyLong()))
                .thenAnswer(
                        invocation ->
                                deletedThrough.updateAndGet(
                                        current ->
                                                Math.max(
                                                        current,
                                                        invocation.getArgument(1, Long.class))));
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any()))
                .thenReturn(firstRemote, freshRemote);
        when(delegate.listNativeSnapshotDetailsByNamePrefix(any()))
                .thenAnswer(
                        invocation -> {
                            String prefix = invocation.getArgument(0);
                            String workspaceId =
                                    prefix.substring(
                                            "agentscope-workspace-".length(), prefix.length() - 1);
                            return Map.of(
                                    "snapshot-old", snapshot(workspaceId, "snapshot-old", 1, NOW));
                        });
        when(store.activeLeases(any(), anyLong())).thenReturn(List.of());
        RedisManagedOpenSandbox first =
                (RedisManagedOpenSandbox)
                        client.create(spec, null, options, isolationKey("user-1"), "agent-a");

        first.shutdown();
        client.delete(first);
        client.create(spec, null, options, isolationKey("user-1"), "agent-a");

        assertEquals(2, record.get().getGeneration());
        ArgumentCaptor<OpenSandboxClientOptions> creates =
                ArgumentCaptor.forClass(OpenSandboxClientOptions.class);
        verify(delegate, times(2)).create(any(WorkspaceSpec.class), isNull(), creates.capture());
        assertEquals(null, creates.getAllValues().get(1).getRestoreSnapshotId());
        assertEquals("2", creates.getAllValues().get(1).getMetadata().get("agentscope.generation"));
        verify(store, times(2)).markOrphanSnapshot(first.workspaceId(), "snapshot-old", NOW);
    }

    @Test
    void explicitDeleteRetainsSnapshotStillReferencedByAnotherWorkspace() throws Exception {
        WorkspaceSpec spec = workspace();
        OpenSandbox remote = remote("sandbox-1", spec);
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any())).thenReturn(remote);
        when(store.activeLeases(any(), anyLong())).thenReturn(List.of());
        when(store.isSnapshotReferenced("snapshot-shared")).thenReturn(true);
        RedisManagedOpenSandbox managed =
                (RedisManagedOpenSandbox)
                        client.create(spec, null, options, isolationKey("user-1"), "agent-a");
        record.get().setNativeSnapshotId("snapshot-shared");

        managed.shutdown();
        client.delete(managed);

        OpenSandboxWorkspaceStore.OrphanReference orphan =
                new OpenSandboxWorkspaceStore.OrphanReference(
                        managed.workspaceId(), "snapshot-shared");
        verify(store).markOrphanSnapshot(managed.workspaceId(), "snapshot-shared", NOW);
        verify(delegate, never()).deleteNativeSnapshot("snapshot-shared");
        verify(store, never()).removeOrphanSnapshot(orphan);
        assertEquals(null, record.get());
    }

    @Test
    void explicitDeleteFencesHighestDiscoveredRemoteGeneration() throws Exception {
        OpenSandboxWorkspaceRecord current = persistedRecord(workspace(), "RUNNING");
        record.set(current);
        OpenSandbox remote = remote("sandbox-1", workspace());
        OpenSandboxActiveLease lease =
                new OpenSandboxActiveLease("lease", current.getWorkspaceId(), 1, "node", NOW, NOW);
        RedisManagedOpenSandbox managed = new RedisManagedOpenSandbox(client, remote, lease);
        OpenSandboxState newer =
                discovered("sandbox-5", current.getWorkspaceId(), 5, NOW.plusSeconds(1));
        when(delegate.listByMetadata(any())).thenReturn(List.of(newer));
        Sandbox newerHandle = mock(Sandbox.class);
        when(delegate.resume(newer)).thenReturn(newerHandle);

        managed.shutdown();
        client.delete(managed);

        verify(store).advanceDeletedThroughGeneration(current.getWorkspaceId(), 5);
        verify(delegate).delete(newerHandle);
    }

    @Test
    void explicitDeleteFencesSnapshotCatalogBeforeMissingWorkspaceCanRecover() throws Exception {
        String workspaceId = RedisOpenSandboxClient.workspaceId(isolationKey("user-1"), "agent-a");
        OpenSandbox stale = remote("sandbox-1", workspace());
        OpenSandboxActiveLease lease =
                new OpenSandboxActiveLease("lease", workspaceId, 1, "node", NOW, NOW);
        RedisManagedOpenSandbox managed = new RedisManagedOpenSandbox(client, stale, lease);
        java.util.concurrent.atomic.AtomicLong deletedThrough =
                new java.util.concurrent.atomic.AtomicLong();
        when(store.deletedThroughGeneration(workspaceId))
                .thenAnswer(ignored -> deletedThrough.get());
        when(store.advanceDeletedThroughGeneration(eq(workspaceId), anyLong()))
                .thenAnswer(
                        invocation ->
                                deletedThrough.updateAndGet(
                                        current ->
                                                Math.max(
                                                        current,
                                                        invocation.getArgument(1, Long.class))));
        when(delegate.listByMetadata(any())).thenReturn(List.of());
        when(delegate.listNativeSnapshotDetailsByNamePrefix(
                        OpenSandboxLifecycleSweeper.snapshotNamePrefix(workspaceId)))
                .thenReturn(
                        Map.of(
                                "snapshot-generation-2",
                                snapshot(workspaceId, "snapshot-generation-2", 2, NOW)));
        when(store.activeLeases(any(), anyLong())).thenReturn(List.of());
        OpenSandbox fresh = remote("sandbox-3", workspace());
        when(delegate.create(any(WorkspaceSpec.class), isNull(), any())).thenReturn(fresh);

        client.delete(managed);
        client.create(workspace(), null, options, isolationKey("user-1"), "agent-a");

        verify(store).advanceDeletedThroughGeneration(workspaceId, 2);
        verify(store, times(2)).markOrphanSnapshot(workspaceId, "snapshot-generation-2", NOW);
        verify(delegate).deleteNativeSnapshot("snapshot-generation-2");
        assertEquals(3, record.get().getGeneration());
        ArgumentCaptor<OpenSandboxClientOptions> createOptions =
                ArgumentCaptor.forClass(OpenSandboxClientOptions.class);
        verify(delegate).create(any(WorkspaceSpec.class), isNull(), createOptions.capture());
        assertEquals(null, createOptions.getValue().getRestoreSnapshotId());
    }

    @Test
    void explicitDeleteUsesHigherConcurrentTombstoneForCleanup() throws Exception {
        String workspaceId = RedisOpenSandboxClient.workspaceId(isolationKey("user-1"), "agent-a");
        OpenSandbox stale = remote("sandbox-1", workspace());
        OpenSandboxActiveLease lease =
                new OpenSandboxActiveLease("lease", workspaceId, 1, "node", NOW, NOW);
        RedisManagedOpenSandbox managed = new RedisManagedOpenSandbox(client, stale, lease);
        OpenSandboxWorkspaceRecord concurrent = persistedRecord(workspace(), "RUNNING");
        concurrent.setGeneration(5);
        record.set(concurrent);
        when(store.load(workspaceId))
                .thenReturn(Optional.empty(), Optional.of(concurrent), Optional.of(concurrent));
        when(store.advanceDeletedThroughGeneration(workspaceId, 1)).thenReturn(5L);
        when(delegate.listByMetadata(any())).thenReturn(List.of());
        when(delegate.listNativeSnapshotDetailsByNamePrefix(any())).thenReturn(Map.of());

        client.delete(managed);

        verify(store).advanceDeletedThroughGeneration(workspaceId, 1);
        verify(store).compareAndSet(concurrent, null);
        assertEquals(null, record.get());
    }

    @Test
    void explicitDeleteRejectsMalformedSnapshotGenerationBeforeSideEffects() throws Exception {
        String workspaceId = RedisOpenSandboxClient.workspaceId(isolationKey("user-1"), "agent-a");
        OpenSandbox stale = remote("sandbox-1", workspace());
        OpenSandboxActiveLease lease =
                new OpenSandboxActiveLease("lease", workspaceId, 1, "node", NOW, NOW);
        RedisManagedOpenSandbox managed = new RedisManagedOpenSandbox(client, stale, lease);
        when(delegate.listByMetadata(any())).thenReturn(List.of());
        when(delegate.listNativeSnapshotDetailsByNamePrefix(any()))
                .thenReturn(
                        Map.of(
                                "snapshot-invalid",
                                new NativeSnapshot("snapshot-invalid", "invalid-name", NOW)));

        SandboxException.SandboxRuntimeException error =
                assertThrows(
                        SandboxException.SandboxRuntimeException.class,
                        () -> client.delete(managed));

        assertEquals("Invalid OpenSandbox generation", error.getMessage());
        verify(store, never()).advanceDeletedThroughGeneration(any(), anyLong());
        verify(store, never()).removeLease(any(), any());
        verify(store, never()).markOrphanSandbox(any(), any(), any());
        verify(store, never()).markOrphanSnapshot(any(), any(), any());
        verify(store, never()).compareAndSet(any(), any());
        verify(stale, never()).disconnect();
        verify(delegate, never()).delete(any());
        verify(delegate, never()).deleteNativeSnapshot(any());
    }

    @Test
    void explicitDeleteKeepsLifecycleLockHeldFromFenceThroughRemoteCleanup() throws Exception {
        OpenSandboxWorkspaceRecord current = persistedRecord(workspace(), "RUNNING");
        record.set(current);
        OpenSandbox remote = remote("sandbox-1", workspace());
        OpenSandboxActiveLease lease =
                new OpenSandboxActiveLease("lease", current.getWorkspaceId(), 1, "node", NOW, NOW);
        RedisManagedOpenSandbox managed = new RedisManagedOpenSandbox(client, remote, lease);
        when(store.activeLeases(current.getWorkspaceId(), 1)).thenReturn(List.of());
        AtomicInteger unlocks = new AtomicInteger();
        doAnswer(
                        ignored -> {
                            unlocks.incrementAndGet();
                            return null;
                        })
                .when(lock)
                .unlock();
        doAnswer(
                        ignored -> {
                            assertEquals(1, unlocks.get());
                            return null;
                        })
                .when(delegate)
                .delete(remote);

        client.delete(managed);

        assertEquals(2, unlocks.get());
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
        InOrder order = inOrder(delegate, store);
        order.verify(store).advanceDeletedThroughGeneration(managed.workspaceId(), 1);
        order.verify(store).scheduleRepair(managed.workspaceId(), NOW);
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

    private OpenSandboxState discovered(
            String sandboxId, String workspaceId, long generation, Instant createdAt) {
        OpenSandboxState info = info(sandboxId, "RUNNING");
        info.setRemoteCreatedAt(createdAt);
        info.setMetadata(
                Map.of(
                        "agentscope.owner",
                        "opensandbox-redis",
                        "agentscope.workspace-id",
                        RedisOpenSandboxClient.metadataLabelHash(workspaceId),
                        "agentscope.generation",
                        Long.toString(generation),
                        "agentscope.runtime-profile",
                        RedisOpenSandboxClient.metadataLabelHash(
                                RedisOpenSandboxClient.runtimeProfileHash(options))));
        info.setWorkspaceSpec(workspace());
        return info;
    }

    private static NativeSnapshot snapshot(
            String workspaceId, String snapshotId, long generation, Instant createdAt) {
        return new NativeSnapshot(
                snapshotId,
                OpenSandboxLifecycleSweeper.snapshotName(workspaceId, generation, createdAt),
                createdAt);
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
