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
package io.agentscope.harness.agent.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.formatter.ResponseFormat;
import io.agentscope.core.message.MessageMetadataKeys;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.model.ChatModelBase;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.ToolSchema;
import io.agentscope.harness.agent.HarnessAgent;
import io.agentscope.harness.agent.IsolationScope;
import io.agentscope.harness.agent.example.support.InMemorySandboxClient;
import io.agentscope.harness.agent.example.support.InMemorySandboxFilesystemSpec;
import io.agentscope.harness.agent.filesystem.local.LocalFilesystem;
import io.agentscope.harness.agent.memory.compaction.CompactionConfig;
import io.agentscope.harness.agent.sandbox.ExecResult;
import io.agentscope.harness.agent.sandbox.Sandbox;
import io.agentscope.harness.agent.sandbox.SandboxClientOptions;
import io.agentscope.harness.agent.sandbox.SandboxState;
import io.agentscope.harness.agent.sandbox.WorkspaceSpec;
import io.agentscope.harness.agent.sandbox.snapshot.SandboxSnapshotSpec;
import io.agentscope.harness.agent.session.WorkspaceSessionLogStore;
import io.agentscope.harness.agent.testing.HarnessQuiescence;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * Contract: a sandbox acquired for one agent call is released exactly once — on success, on a
 * failed overflow retry and on cancellation mid-retry — and the structured-output overload keeps
 * the same response format across the overflow retry.
 */
@HarnessQuiescence
class SandboxReleasePairingTest {

    @TempDir Path workspace;

    @TempDir Path stateHome;

    private String previousStateHome;

    @BeforeEach
    void overrideStateHome() {
        previousStateHome = System.getProperty("agentscope.state.home");
        System.setProperty("agentscope.state.home", stateHome.toString());
    }

    @AfterEach
    void restoreStateHome() {
        if (previousStateHome != null) {
            System.setProperty("agentscope.state.home", previousStateHome);
        } else {
            System.clearProperty("agentscope.state.home");
        }
    }

    @Test
    void failedOverflowRetryReleasesSandboxExactlyOnce() throws Exception {
        StopCountingSandboxClient client = new StopCountingSandboxClient();
        ScenarioModel model = new ScenarioModel(Scenario.FAIL_RETRY, false);
        HarnessAgent agent = agent(client, model);

        StepVerifier.create(agent.call(threeMessages(), ctx("session-1")))
                .expectErrorSatisfies(
                        error -> {
                            assertEquals("retry boom", error.getMessage());
                            assertNotNull(error.getCause(), "the first overflow must be attached");
                            assertEquals("context_length_exceeded", error.getCause().getMessage());
                        })
                .verify(Duration.ofSeconds(15));

        assertTrue(client.stopped.await(5, TimeUnit.SECONDS), "the sandbox must be released");
        Thread.sleep(200);
        assertEquals(2, model.conversationCalls.get(), "one overflow plus one failed retry");
        assertEquals(1, client.getCreateCount());
        assertEquals(0, client.getResumeCount());
        assertEquals(
                1, client.stopCount.get(), "the self-managed sandbox must be stopped exactly once");
    }

    @Test
    void cancelledMidRetryReleasesSandboxExactlyOnce() throws Exception {
        StopCountingSandboxClient client = new StopCountingSandboxClient();
        ScenarioModel model = new ScenarioModel(Scenario.CANCEL_RETRY, false);
        HarnessAgent agent = agent(client, model);
        AtomicReference<Msg> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Disposable subscription =
                agent.call(threeMessages(), ctx("session-1"))
                        .doOnNext(result::set)
                        .doOnError(failure::set)
                        .subscribe();
        try {
            assertTrue(
                    model.retryStarted.await(10, TimeUnit.SECONDS),
                    "the overflow retry model call must start");
            assertEquals(
                    1, client.getCreateCount(), "the sandbox must be acquired before the retry");
        } finally {
            subscription.dispose();
        }

        assertTrue(
                client.stopped.await(10, TimeUnit.SECONDS),
                "cancellation must release the acquired sandbox");
        Thread.sleep(200);
        assertNull(result.get(), "a cancelled call must not deliver a result");
        Throwable recorded = failure.get();
        assertTrue(
                recorded == null || recorded instanceof CancellationException,
                "cancellation must not surface as a business error");
        assertEquals(1, client.getCreateCount());
        assertEquals(0, client.getResumeCount());
        assertEquals(
                1, client.stopCount.get(), "the self-managed sandbox must be stopped exactly once");
    }

    @Test
    void structuredOutputRetryKeepsSchemaAndReleasesSandboxExactlyOnce() throws Exception {
        StopCountingSandboxClient client = new StopCountingSandboxClient();
        ScenarioModel model = new ScenarioModel(Scenario.SUCCESS_RETRY, true);
        HarnessAgent agent = agent(client, model);
        JsonNode schema =
                new ObjectMapper()
                        .readTree(
                                "{\"type\":\"object\",\"properties\":{\"answer\":{\"type\":"
                                        + "\"string\"}},\"required\":[\"answer\"]}");

        Msg result =
                agent.call(threeMessages(), schema, ctx("session-1")).block(Duration.ofSeconds(15));

        assertNotNull(result, "the structured call must produce a result");
        assertTrue(result.getTextContent().contains("answer"));
        assertNotNull(result.getMetadata());
        assertTrue(
                result.getMetadata().get(MessageMetadataKeys.STRUCTURED_OUTPUT) != null,
                "native structured output must be parsed into message metadata");
        assertEquals(2, model.conversationCalls.get(), "one overflow plus one successful retry");
        assertNotNull(model.responseFormats.get(0), "the first call must carry a response format");
        assertSame(
                model.responseFormats.get(0),
                model.responseFormats.get(1),
                "the same response format must flow through the overflow retry");
        assertTrue(client.stopped.await(5, TimeUnit.SECONDS), "the sandbox must be released");
        Thread.sleep(200);
        assertEquals(1, client.getCreateCount());
        assertEquals(0, client.getResumeCount());
        assertEquals(
                1, client.stopCount.get(), "the self-managed sandbox must be stopped exactly once");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private HarnessAgent agent(InMemorySandboxClient client, ChatModelBase model) {
        InMemorySandboxFilesystemSpec spec = new InMemorySandboxFilesystemSpec(client);
        spec.isolationScope(IsolationScope.SESSION);
        return HarnessAgent.builder()
                .name("pairing-assistant")
                .model(model)
                .workspace(workspace.toAbsolutePath().normalize().toString())
                .filesystem(spec)
                .compaction(CompactionConfig.builder().flushBeforeCompact(false).build())
                .disableMemoryHooks()
                .sessionLogStore(
                        new WorkspaceSessionLogStore(
                                new LocalFilesystem(workspace.resolve("journal"))))
                .build();
    }

    private static RuntimeContext ctx(String sessionId) {
        return RuntimeContext.builder().sessionId(sessionId).userId("user-a").build();
    }

    /** Padding that keeps the fixture larger than the fixed summary-wrapper cost. */
    private static final String PAD = " earlier conversation detail".repeat(40);

    private static List<Msg> threeMessages() {
        return List.of(userMsg("m1" + PAD), userMsg("m2"), userMsg("m3"));
    }

    private static Msg userMsg(String text) {
        return Msg.builder()
                .role(MsgRole.USER)
                .content(TextBlock.builder().text(text).build())
                .build();
    }

    private static ChatResponse textResponse(String text) {
        return ChatResponse.builder()
                .content(List.of(TextBlock.builder().text(text).build()))
                .build();
    }

    private enum Scenario {
        FAIL_RETRY,
        CANCEL_RETRY,
        SUCCESS_RETRY
    }

    /** Counts {@code stop()} through the released sandbox while delegating everything else. */
    private static final class StopCountingSandboxClient extends InMemorySandboxClient {

        private final AtomicInteger stopCount = new AtomicInteger();
        private final CountDownLatch stopped = new CountDownLatch(1);

        @Override
        public Sandbox create(
                WorkspaceSpec workspaceSpec,
                SandboxSnapshotSpec snapshotSpec,
                SandboxClientOptions options) {
            return wrap(super.create(workspaceSpec, snapshotSpec, options));
        }

        @Override
        public Sandbox resume(SandboxState sessionState) {
            return wrap(super.resume(sessionState));
        }

        private Sandbox wrap(Sandbox delegate) {
            return new Sandbox() {
                @Override
                public void start() throws Exception {
                    delegate.start();
                }

                @Override
                public void stop() throws Exception {
                    stopCount.incrementAndGet();
                    stopped.countDown();
                    delegate.stop();
                }

                @Override
                public void shutdown() throws Exception {
                    delegate.shutdown();
                }

                @Override
                public void close() throws Exception {
                    delegate.close();
                }

                @Override
                public boolean isRunning() {
                    return delegate.isRunning();
                }

                @Override
                public SandboxState getState() {
                    return delegate.getState();
                }

                @Override
                public ExecResult exec(
                        RuntimeContext runtimeContext, String command, Integer timeoutSeconds)
                        throws Exception {
                    return delegate.exec(runtimeContext, command, timeoutSeconds);
                }

                @Override
                public InputStream persistWorkspace() throws Exception {
                    return delegate.persistWorkspace();
                }

                @Override
                public void hydrateWorkspace(InputStream archive) throws Exception {
                    delegate.hydrateWorkspace(archive);
                }
            };
        }
    }

    /** Answers compaction prompts directly and plays a configurable conversation-call scenario. */
    private static final class ScenarioModel extends ChatModelBase {

        private final Scenario scenario;
        private final AtomicInteger conversationCalls = new AtomicInteger();
        private final List<ResponseFormat> responseFormats = new CopyOnWriteArrayList<>();
        private final CountDownLatch retryStarted = new CountDownLatch(1);

        private ScenarioModel(Scenario scenario, boolean nativeStructured) {
            this.scenario = scenario;
            if (nativeStructured) {
                setNativeStructuredOutput(true);
            }
            setContextWindowSize(200_000);
        }

        @Override
        public String getModelName() {
            return "sandbox-pairing-model";
        }

        @Override
        protected Flux<ChatResponse> doStream(
                List<Msg> messages, List<ToolSchema> tools, GenerateOptions options) {
            String first = messages.isEmpty() ? "" : messages.get(0).getTextContent();
            if (first != null && first.contains("<messages>")) {
                return Flux.just(textResponse("summary"));
            }
            if (first != null && first.contains("memory extraction assistant")) {
                return Flux.just(textResponse("no memories"));
            }
            int call = conversationCalls.incrementAndGet();
            responseFormats.add(options == null ? null : options.getResponseFormat());
            if (call == 1) {
                return Flux.error(new IllegalStateException("context_length_exceeded"));
            }
            if (scenario == Scenario.FAIL_RETRY) {
                return Flux.error(new IllegalStateException("retry boom"));
            }
            if (scenario == Scenario.CANCEL_RETRY) {
                retryStarted.countDown();
                return Flux.never();
            }
            return Flux.just(textResponse("{\"answer\":\"ok\"}"));
        }
    }
}
