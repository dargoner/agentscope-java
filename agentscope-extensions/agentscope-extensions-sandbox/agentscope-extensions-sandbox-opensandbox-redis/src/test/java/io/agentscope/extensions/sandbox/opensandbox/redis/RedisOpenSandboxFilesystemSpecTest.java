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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.agentscope.core.model.Model;
import io.agentscope.core.state.AgentStateStore;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClientOptions;
import io.agentscope.harness.agent.DistributedStore;
import io.agentscope.harness.agent.HarnessAgent;
import io.agentscope.harness.agent.IsolationScope;
import io.agentscope.harness.agent.filesystem.remote.store.BaseStore;
import io.agentscope.harness.agent.sandbox.SandboxClient;
import io.agentscope.harness.agent.sandbox.SandboxContext;
import io.agentscope.harness.agent.sandbox.SandboxException;
import io.agentscope.harness.agent.sandbox.SandboxExecutionGuard;
import io.agentscope.harness.agent.sandbox.WorkspaceSpec;
import io.agentscope.harness.agent.sandbox.layout.WorkspaceProjectionEntry;
import io.agentscope.harness.agent.sandbox.snapshot.LocalSnapshotSpec;
import io.agentscope.harness.agent.sandbox.snapshot.NoopSnapshotSpec;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.redisson.api.RedissonClient;

class RedisOpenSandboxFilesystemSpecTest {

    @Test
    void requiresExternalRedissonAndDefaultsToUserIsolation() {
        NullPointerException error =
                assertThrows(
                        NullPointerException.class, () -> new RedisOpenSandboxFilesystemSpec(null));
        assertTrue(error.getMessage().contains("redisson"));

        RedissonClient redisson = mock(RedissonClient.class);
        RedisOpenSandboxFilesystemSpec spec = new RedisOpenSandboxFilesystemSpec(redisson);
        SandboxContext context = spec.toSandboxContext();

        assertEquals(IsolationScope.USER, context.getIsolationScope());
        assertInstanceOf(RedisOpenSandboxClient.class, context.getClient());
        ((RedisOpenSandboxClient) context.getClient()).close();
    }

    @Test
    void passesFluentClientLifecycleWorkspaceAndIsolationOptions() throws Exception {
        RedissonClient redisson = mock(RedissonClient.class);
        OpenSandboxClientOptions clientOptions = new OpenSandboxClientOptions();
        clientOptions.setEndpoint("https://sandbox-control.example.test");
        clientOptions.setApiKey("configured-outside-source");
        clientOptions.setImage("runtime.example.test/team/image:v1");
        OpenSandboxRedisLifecycleOptions lifecycleOptions = new OpenSandboxRedisLifecycleOptions();
        lifecycleOptions.setIdleTtl(Duration.ofMinutes(90));
        lifecycleOptions.setSweepInterval(Duration.ofMinutes(3));
        WorkspaceSpec workspaceSpec = new WorkspaceSpec();
        workspaceSpec.setRoot("/team-workspace");

        RedisOpenSandboxFilesystemSpec spec = new RedisOpenSandboxFilesystemSpec(redisson);
        assertSame(spec, spec.clientOptions(clientOptions));
        assertSame(spec, spec.lifecycleOptions(lifecycleOptions));
        assertSame(spec, spec.workspaceSpec(workspaceSpec));
        assertSame(spec, spec.isolationScope(IsolationScope.SESSION));

        SandboxContext context = spec.toSandboxContext();
        RedisOpenSandboxClient client =
                assertInstanceOf(RedisOpenSandboxClient.class, context.getClient());
        OpenSandboxClientOptions clientDefaults = field(client, "defaultOptions");
        OpenSandboxRedisLifecycleOptions lifecycleDefaults = field(client, "lifecycle");

        assertSame(clientOptions, context.getClientOptions());
        assertEquals("/team-workspace", context.getWorkspaceSpec().getRoot());
        assertEquals(IsolationScope.SESSION, context.getIsolationScope());
        assertNotSame(clientOptions, clientDefaults);
        assertEquals(clientOptions.getEndpoint(), clientDefaults.getEndpoint());
        assertEquals(clientOptions.getApiKey(), clientDefaults.getApiKey());
        assertEquals(clientOptions.getImage(), clientDefaults.getImage());
        assertNotSame(lifecycleOptions, lifecycleDefaults);
        assertEquals(Duration.ofMinutes(90), lifecycleDefaults.getIdleTtl());
        assertEquals(Duration.ofMinutes(3), lifecycleDefaults.getSweepInterval());
        client.close();
    }

    @Test
    void clientCloseOnlyStopsItsOwnedScheduler() throws Exception {
        RedissonClient redisson = mock(RedissonClient.class);
        RedisOpenSandboxFilesystemSpec spec = new RedisOpenSandboxFilesystemSpec(redisson);
        RedisOpenSandboxClient client =
                assertInstanceOf(RedisOpenSandboxClient.class, spec.toSandboxContext().getClient());
        ScheduledExecutorService scheduler = field(client, "scheduler");

        client.close();

        assertTrue(scheduler.isShutdown());
        verify(redisson, never()).shutdown();
    }

    @Test
    void harnessOwnsOnlyInternallyCreatedClient(@TempDir Path workspace) throws Exception {
        RedissonClient redisson = mock(RedissonClient.class);
        CapturingRedisOpenSandboxFilesystemSpec ownedSpec =
                new CapturingRedisOpenSandboxFilesystemSpec(redisson);
        HarnessAgent ownedAgent =
                HarnessAgent.builder()
                        .name("owned")
                        .model(mock(Model.class))
                        .workspace(workspace)
                        .filesystem(ownedSpec)
                        .build();
        ScheduledExecutorService ownedScheduler = field(ownedSpec.client(), "scheduler");

        ownedAgent.close();
        ownedAgent.close();

        assertTrue(ownedScheduler.isShutdown());

        SandboxClient<?> externalClient =
                mock(SandboxClient.class, withSettings().extraInterfaces(AutoCloseable.class));
        RedisOpenSandboxFilesystemSpec externalSpec = new RedisOpenSandboxFilesystemSpec(redisson);
        assertSame(externalSpec, externalSpec.client(externalClient));
        HarnessAgent externalAgent =
                HarnessAgent.builder()
                        .name("external")
                        .model(mock(Model.class))
                        .workspace(workspace)
                        .filesystem(externalSpec)
                        .build();

        externalAgent.close();

        assertFalse(externalSpec.materialize(null).clientOwned());
        verify((AutoCloseable) externalClient, never()).close();
    }

    @Test
    void concurrentHarnessBuildsEachOwnTheirMaterializedClient(@TempDir Path workspace)
            throws Exception {
        CoordinatedRedisOpenSandboxFilesystemSpec spec =
                new CoordinatedRedisOpenSandboxFilesystemSpec(
                        mock(RedissonClient.class), new CyclicBarrier(2));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        HarnessAgent first = null;
        HarnessAgent second = null;
        try {
            Future<HarnessAgent> firstFuture =
                    executor.submit(() -> buildAgent("first", workspace, spec));
            Future<HarnessAgent> secondFuture =
                    executor.submit(() -> buildAgent("second", workspace, spec));
            first = firstFuture.get();
            second = secondFuture.get();
            assertEquals(2, spec.clients().size());
            assertNotSame(spec.clients().get(0), spec.clients().get(1));

            first.close();
            second.close();

            for (RedisOpenSandboxClient client : spec.clients()) {
                ScheduledExecutorService scheduler = field(client, "scheduler");
                assertTrue(scheduler.isShutdown());
            }
        } finally {
            if (first != null) {
                first.close();
            }
            if (second != null) {
                second.close();
            }
            for (RedisOpenSandboxClient client : spec.clients()) {
                client.close();
            }
            executor.shutdownNow();
        }
    }

    @Test
    void projectionMethodsAreCovariantAndPropagateValues(@TempDir Path workspace) {
        RedisOpenSandboxFilesystemSpec spec =
                new RedisOpenSandboxFilesystemSpec(mock(RedissonClient.class));
        RedisOpenSandboxFilesystemSpec chained =
                spec.workspaceProjectionEnabled(true)
                        .workspaceProjectionRoots(List.of("AGENTS.md", "skills"));

        assertSame(spec, chained);
        SandboxContext projected = spec.toSandboxContext(workspace);
        WorkspaceProjectionEntry projection =
                assertInstanceOf(
                        WorkspaceProjectionEntry.class,
                        projected.getWorkspaceSpec().getEntries().get("__workspace_projection__"));
        assertEquals(List.of("AGENTS.md", "skills"), projection.getIncludeRoots());
        ((RedisOpenSandboxClient) projected.getClient()).close();

        assertSame(spec, spec.workspaceProjectionEnabled(false));
        SandboxContext unprojected = spec.toSandboxContext(workspace);
        assertFalse(
                unprojected
                        .getWorkspaceSpec()
                        .getEntries()
                        .containsKey("__workspace_projection__"));
        ((RedisOpenSandboxClient) unprojected.getClient()).close();
    }

    @Test
    void distributedStoreDoesNotOverrideRedisNativeLifecycle(@TempDir Path workspace) {
        DistributedStore distributedStore = mock(DistributedStore.class);
        when(distributedStore.agentStateStore()).thenReturn(mock(AgentStateStore.class));
        when(distributedStore.baseStore()).thenReturn(mock(BaseStore.class));
        when(distributedStore.sandboxSnapshotSpec())
                .thenReturn(new LocalSnapshotSpec("target/distributed-snapshots"));
        SandboxExecutionGuard fullTurnGuard = key -> () -> {};
        when(distributedStore.sandboxExecutionGuard()).thenReturn(fullTurnGuard);
        RedisOpenSandboxFilesystemSpec spec =
                new RedisOpenSandboxFilesystemSpec(mock(RedissonClient.class));

        HarnessAgent agent =
                assertDoesNotThrow(
                        () ->
                                HarnessAgent.builder()
                                        .name("distributed-native-snapshot")
                                        .model(mock(Model.class))
                                        .workspace(workspace)
                                        .distributedStore(distributedStore)
                                        .filesystem(spec)
                                        .build());
        try {
            assertInstanceOf(NoopSnapshotSpec.class, spec.getSnapshotSpecOverride());
            assertSame(SandboxExecutionGuard.noop(), spec.getExecutionGuard());
            verify(distributedStore, never()).sandboxSnapshotSpec();
            verify(distributedStore, never()).sandboxExecutionGuard();
        } finally {
            agent.close();
        }
    }

    @Test
    void executionGuardAcceptsOnlyNullAndOfficialNoop() {
        RedisOpenSandboxFilesystemSpec spec =
                new RedisOpenSandboxFilesystemSpec(mock(RedissonClient.class));

        assertSame(spec, spec.executionGuard(null));
        assertSame(SandboxExecutionGuard.noop(), spec.getExecutionGuard());
        assertSame(spec, spec.executionGuard(SandboxExecutionGuard.noop()));
        assertSame(SandboxExecutionGuard.noop(), spec.getExecutionGuard());

        SandboxException.SandboxConfigurationException error =
                assertThrows(
                        SandboxException.SandboxConfigurationException.class,
                        () -> spec.executionGuard(key -> () -> {}));
        assertTrue(error.getMessage().contains("execution guard"));
        assertSame(SandboxExecutionGuard.noop(), spec.getExecutionGuard());
    }

    @Test
    void rejectsTarSnapshotsButAcceptsNullAndNoop() {
        RedissonClient redisson = mock(RedissonClient.class);
        RedisOpenSandboxFilesystemSpec spec = new RedisOpenSandboxFilesystemSpec(redisson);

        assertSame(spec, spec.snapshotSpec(null));
        assertSame(spec, spec.snapshotSpec(new NoopSnapshotSpec()));
        SandboxException.SandboxConfigurationException error =
                assertThrows(
                        SandboxException.SandboxConfigurationException.class,
                        () -> spec.snapshotSpec(new LocalSnapshotSpec("target/snapshots")));

        assertTrue(error.getMessage().contains("native snapshots"));
        assertTrue(error.getMessage().contains("tar snapshot"));
    }

    @SuppressWarnings("unchecked")
    private static <T> T field(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return (T) field.get(target);
    }

    private static HarnessAgent buildAgent(
            String name, Path workspace, RedisOpenSandboxFilesystemSpec spec) {
        return HarnessAgent.builder()
                .name(name)
                .model(mock(Model.class))
                .workspace(workspace)
                .filesystem(spec)
                .build();
    }

    private static final class CapturingRedisOpenSandboxFilesystemSpec
            extends RedisOpenSandboxFilesystemSpec {

        private RedisOpenSandboxClient client;

        private CapturingRedisOpenSandboxFilesystemSpec(RedissonClient redisson) {
            super(redisson);
        }

        @Override
        protected SandboxClient<?> createClient() {
            client = (RedisOpenSandboxClient) super.createClient();
            return client;
        }

        private RedisOpenSandboxClient client() {
            return client;
        }
    }

    private static final class CoordinatedRedisOpenSandboxFilesystemSpec
            extends RedisOpenSandboxFilesystemSpec {

        private final CyclicBarrier unsynchronizedBarrier;
        private final List<RedisOpenSandboxClient> clients = new CopyOnWriteArrayList<>();

        private CoordinatedRedisOpenSandboxFilesystemSpec(
                RedissonClient redisson, CyclicBarrier unsynchronizedBarrier) {
            super(redisson);
            this.unsynchronizedBarrier = unsynchronizedBarrier;
        }

        @Override
        protected SandboxClient<?> createClient() {
            RedisOpenSandboxClient client = (RedisOpenSandboxClient) super.createClient();
            clients.add(client);
            if (!Thread.holdsLock(this)) {
                try {
                    unsynchronizedBarrier.await();
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to coordinate materialization", e);
                }
            }
            return client;
        }

        private List<RedisOpenSandboxClient> clients() {
            return clients;
        }
    }
}
