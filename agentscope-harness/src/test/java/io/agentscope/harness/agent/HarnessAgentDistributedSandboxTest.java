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
package io.agentscope.harness.agent;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.agentscope.core.hook.Hook;
import io.agentscope.core.hook.HookEvent;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.Model;
import io.agentscope.core.state.AgentStateStore;
import io.agentscope.harness.agent.example.support.InMemorySandboxClient;
import io.agentscope.harness.agent.filesystem.remote.store.BaseStore;
import io.agentscope.harness.agent.filesystem.spec.RemoteFilesystemSpec;
import io.agentscope.harness.agent.filesystem.spec.SandboxFilesystemSpec;
import io.agentscope.harness.agent.sandbox.SandboxClient;
import io.agentscope.harness.agent.sandbox.SandboxClientOptions;
import io.agentscope.harness.agent.sandbox.SandboxContext;
import io.agentscope.harness.agent.sandbox.WorkspaceSpec;
import io.agentscope.harness.agent.sandbox.impl.docker.DockerFilesystemSpec;
import io.agentscope.harness.agent.sandbox.snapshot.LocalSnapshotSpec;
import io.agentscope.harness.agent.sandbox.snapshot.SandboxSnapshotSpec;
import io.agentscope.harness.agent.subagent.task.TaskRepository;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class HarnessAgentDistributedSandboxTest {

    @TempDir Path workspace;

    @Test
    void sandboxMode_withLocalSession_buildsWithWarning() {
        // Sandbox mode with a local AgentStateStore now builds successfully (warn-only).
        assertDoesNotThrow(
                () ->
                        HarnessAgent.builder()
                                .name("agent")
                                .model(stubModel("ok"))
                                .workspace(workspace)
                                .filesystem(new DockerFilesystemSpec())
                                .build());
    }

    @Test
    void sandboxMode_withDistributedSession_builds() {
        AgentStateStore distributedSession = mock(AgentStateStore.class);
        assertDoesNotThrow(
                () ->
                        HarnessAgent.builder()
                                .name("agent")
                                .model(stubModel("ok"))
                                .workspace(workspace)
                                .stateStore(distributedSession)
                                .filesystem(new DockerFilesystemSpec())
                                .build());
    }

    @Test
    void sandboxMode_snapshotSpecOnFilesystemSpec() {
        AgentStateStore distributedSession = mock(AgentStateStore.class);
        DockerFilesystemSpec spec = new DockerFilesystemSpec();
        spec.isolationScope(IsolationScope.AGENT);
        spec.snapshotSpec(new LocalSnapshotSpec(workspace.resolve("snapshots")));

        assertDoesNotThrow(
                () ->
                        HarnessAgent.builder()
                                .name("agent")
                                .model(stubModel("ok"))
                                .workspace(workspace)
                                .stateStore(distributedSession)
                                .filesystem(spec)
                                .build());

        assertEquals(IsolationScope.AGENT, spec.getIsolationScope());
        assertInstanceOf(LocalSnapshotSpec.class, spec.toSandboxContext().getSnapshotSpec());
    }

    @Test
    void remoteFilesystemMode_withLocalSession_failsFast() {
        BaseStore store = mock(BaseStore.class);
        IllegalStateException ex =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                HarnessAgent.builder()
                                        .name("agent")
                                        .model(stubModel("ok"))
                                        .workspace(workspace)
                                        .filesystem(new RemoteFilesystemSpec(store))
                                        .build());
        assertEquals(
                true,
                ex.getMessage().contains("RemoteFilesystemSpec"),
                "Mode 1 should fail-fast when effective session is a local in-process"
                        + " implementation (JsonFileAgentStateStore / InMemoryAgentStateStore)");
    }

    @Test
    void remoteFilesystemMode_withDistributedSession_succeeds() throws Exception {
        BaseStore store = mock(BaseStore.class);
        AgentStateStore distributedSession = mock(AgentStateStore.class);
        HarnessAgent agent =
                HarnessAgent.builder()
                        .name("agent")
                        .model(stubModel("ok"))
                        .workspace(workspace)
                        .filesystem(new RemoteFilesystemSpec(store))
                        .stateStore(distributedSession)
                        .build();
        agent.close();
    }

    @Test
    void closeClosesSpecOwnedSandboxClientOnlyOnce() {
        CloseTrackingSandboxFilesystemSpec spec = new CloseTrackingSandboxFilesystemSpec();
        HarnessAgent agent =
                HarnessAgent.builder()
                        .name("agent")
                        .model(stubModel("ok"))
                        .workspace(workspace)
                        .filesystem(spec)
                        .build();

        agent.close();
        agent.close();

        assertEquals(1, spec.client().closeCount());
    }

    @Test
    void closeDoesNotCloseClientInjectedIntoFilesystemSpec() {
        CloseTrackingSandboxClient externalClient = new CloseTrackingSandboxClient();
        DockerFilesystemSpec spec = new DockerFilesystemSpec().client(externalClient);
        HarnessAgent agent =
                HarnessAgent.builder()
                        .name("agent")
                        .model(stubModel("ok"))
                        .workspace(workspace)
                        .filesystem(spec)
                        .build();

        agent.close();

        assertEquals(0, externalClient.closeCount());
    }

    @Test
    void closeAggregatesFailuresAndDoesNotRetryCleanup() {
        RuntimeException taskFailure = new RuntimeException("task close failed");
        RuntimeException clientFailure = new RuntimeException("client close failed");
        TaskRepository taskRepository = mock(TaskRepository.class);
        doThrow(taskFailure).when(taskRepository).shutdown();
        CloseTrackingSandboxClient ownedClient = new CloseTrackingSandboxClient(clientFailure);
        CloseTrackingSandboxFilesystemSpec spec =
                new CloseTrackingSandboxFilesystemSpec(ownedClient);
        HarnessAgent agent =
                HarnessAgent.builder()
                        .name("agent")
                        .model(stubModel("ok"))
                        .workspace(workspace)
                        .filesystem(spec)
                        .taskRepository(taskRepository)
                        .build();

        RuntimeException thrown = assertThrows(RuntimeException.class, agent::close);

        assertSame(taskFailure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(clientFailure, thrown.getSuppressed()[0]);
        assertEquals(1, ownedClient.closeCount());
        assertSame(taskFailure, assertThrows(RuntimeException.class, agent::close));
        verify(taskRepository, times(1)).shutdown();
        assertEquals(1, ownedClient.closeCount());
    }

    @Test
    void failedBuildRollsBackOwnedClientButNotExternalClient() {
        RuntimeException buildFailure = new RuntimeException("late build failed");
        RuntimeException closeFailure = new RuntimeException("rollback close failed");
        CloseTrackingSandboxClient ownedClient = new CloseTrackingSandboxClient(closeFailure);
        CloseTrackingSandboxFilesystemSpec ownedSpec =
                new CloseTrackingSandboxFilesystemSpec(ownedClient);

        RuntimeException thrown =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                HarnessAgent.builder()
                                        .name("owned")
                                        .model(stubModel("ok"))
                                        .workspace(workspace)
                                        .filesystem(ownedSpec)
                                        .hook(failingBuildHook(buildFailure))
                                        .build());

        assertSame(buildFailure, thrown);
        assertEquals(1, ownedClient.closeCount());
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(closeFailure, thrown.getSuppressed()[0]);

        CloseTrackingSandboxClient externalClient = new CloseTrackingSandboxClient();
        RuntimeException externalBuildFailure = new RuntimeException("external build failed");
        RuntimeException externalThrown =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                HarnessAgent.builder()
                                        .name("external")
                                        .model(stubModel("ok"))
                                        .workspace(workspace)
                                        .filesystem(
                                                new DockerFilesystemSpec().client(externalClient))
                                        .hook(failingBuildHook(externalBuildFailure))
                                        .build());

        assertSame(externalBuildFailure, externalThrown);
        assertEquals(0, externalClient.closeCount());
    }

    @Test
    void concurrentCloseWaitsAndRethrowsFirstFailure() throws Exception {
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch allowClose = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        RuntimeException closeFailure = new RuntimeException("client close failed");
        CloseTrackingSandboxClient ownedClient =
                new CloseTrackingSandboxClient(closeFailure, closeStarted, allowClose);
        HarnessAgent agent =
                HarnessAgent.builder()
                        .name("agent")
                        .model(stubModel("ok"))
                        .workspace(workspace)
                        .filesystem(new CloseTrackingSandboxFilesystemSpec(ownedClient))
                        .build();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = executor.submit(agent::close);
            assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
            Future<?> second =
                    executor.submit(
                            () -> {
                                secondStarted.countDown();
                                agent.close();
                            });
            assertTrue(secondStarted.await(5, TimeUnit.SECONDS));

            assertThrows(TimeoutException.class, () -> second.get(100, TimeUnit.MILLISECONDS));
            allowClose.countDown();

            ExecutionException firstThrown = assertThrows(ExecutionException.class, first::get);
            ExecutionException secondThrown = assertThrows(ExecutionException.class, second::get);
            assertSame(closeFailure, firstThrown.getCause());
            assertSame(closeFailure, secondThrown.getCause());
            assertEquals(1, ownedClient.closeCount());
        } finally {
            allowClose.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void closeDoesNotCloseClientFromRuntimeSandboxContext() {
        CloseTrackingSandboxFilesystemSpec spec = new CloseTrackingSandboxFilesystemSpec();
        CloseTrackingSandboxClient externalClient = new CloseTrackingSandboxClient();
        WorkspaceSpec workspaceSpec = new WorkspaceSpec();
        SandboxContext externalContext =
                SandboxContext.builder()
                        .client(externalClient)
                        .workspaceSpec(workspaceSpec)
                        .isolationScope(IsolationScope.SESSION)
                        .build();
        HarnessAgent agent =
                HarnessAgent.builder()
                        .name("agent")
                        .model(stubModel("ok"))
                        .workspace(workspace)
                        .filesystem(spec)
                        .build();

        agent.call(
                        io.agentscope.core.message.UserMessage.builder()
                                .content(
                                        io.agentscope.core.message.TextBlock.builder()
                                                .text("run")
                                                .build())
                                .build(),
                        io.agentscope.core.agent.RuntimeContext.builder()
                                .sessionId(UUID.randomUUID().toString())
                                .put(SandboxContext.class, externalContext)
                                .build())
                .block();
        agent.close();

        assertEquals(1, spec.client().closeCount());
        assertEquals(0, externalClient.closeCount());
    }

    private static Model stubModel(String assistantText) {
        Model model = mock(Model.class);
        when(model.getModelName()).thenReturn("stub-model");
        ChatResponse chunk =
                new ChatResponse(
                        "stub-id",
                        List.of(
                                io.agentscope.core.message.TextBlock.builder()
                                        .text(assistantText)
                                        .build()),
                        null,
                        Map.of(),
                        "stop");
        when(model.stream(anyList(), any(), any())).thenReturn(Flux.just(chunk));
        return model;
    }

    @SuppressWarnings("removal")
    private static Hook failingBuildHook(RuntimeException failure) {
        return new Hook() {
            @Override
            public <T extends HookEvent> Mono<T> onEvent(T event) {
                return Mono.just(event);
            }

            @Override
            public List<Object> tools() {
                throw failure;
            }
        };
    }

    private static final class CloseTrackingSandboxClient extends InMemorySandboxClient
            implements AutoCloseable {

        private final AtomicInteger closeCount = new AtomicInteger();
        private final RuntimeException closeFailure;
        private final CountDownLatch closeStarted;
        private final CountDownLatch allowClose;

        private CloseTrackingSandboxClient() {
            this(null, null, null);
        }

        private CloseTrackingSandboxClient(RuntimeException closeFailure) {
            this(closeFailure, null, null);
        }

        private CloseTrackingSandboxClient(
                RuntimeException closeFailure,
                CountDownLatch closeStarted,
                CountDownLatch allowClose) {
            this.closeFailure = closeFailure;
            this.closeStarted = closeStarted;
            this.allowClose = allowClose;
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
            if (closeStarted != null) {
                closeStarted.countDown();
            }
            if (allowClose != null) {
                try {
                    allowClose.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while closing test client", e);
                }
            }
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        int closeCount() {
            return closeCount.get();
        }
    }

    private static final class CloseTrackingSandboxFilesystemSpec extends SandboxFilesystemSpec {

        private final CloseTrackingSandboxClient client;

        private CloseTrackingSandboxFilesystemSpec() {
            this(new CloseTrackingSandboxClient());
        }

        private CloseTrackingSandboxFilesystemSpec(CloseTrackingSandboxClient client) {
            this.client = client;
        }

        @Override
        protected SandboxClient<?> createClient() {
            return client;
        }

        @Override
        protected boolean isClientOwned(SandboxClient<?> candidate) {
            return candidate == client;
        }

        @Override
        protected SandboxClientOptions clientOptions() {
            return null;
        }

        @Override
        protected SandboxSnapshotSpec snapshotSpec() {
            return null;
        }

        @Override
        protected WorkspaceSpec workspaceSpec() {
            return new WorkspaceSpec();
        }

        CloseTrackingSandboxClient client() {
            return client;
        }
    }
}
