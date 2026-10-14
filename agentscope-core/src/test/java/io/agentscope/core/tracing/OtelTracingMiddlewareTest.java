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
package io.agentscope.core.tracing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.agentscope.core.agent.Agent;
import io.agentscope.core.agent.Event;
import io.agentscope.core.agent.StreamOptions;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.event.AgentResultEvent;
import io.agentscope.core.event.AgentStartEvent;
import io.agentscope.core.event.ModelCallEndEvent;
import io.agentscope.core.event.TextBlockDeltaEvent;
import io.agentscope.core.event.ThinkingBlockDeltaEvent;
import io.agentscope.core.event.ToolCallDeltaEvent;
import io.agentscope.core.event.ToolResultDataDeltaEvent;
import io.agentscope.core.event.ToolResultEndEvent;
import io.agentscope.core.event.ToolResultTextDeltaEvent;
import io.agentscope.core.message.AssistantMessage;
import io.agentscope.core.message.AudioBlock;
import io.agentscope.core.message.Base64Source;
import io.agentscope.core.message.DataBlock;
import io.agentscope.core.message.ImageBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.SystemMessage;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.message.ToolResultMessage;
import io.agentscope.core.message.ToolResultState;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.message.URLSource;
import io.agentscope.core.message.UserMessage;
import io.agentscope.core.message.VideoBlock;
import io.agentscope.core.middleware.ActingInput;
import io.agentscope.core.middleware.AgentInput;
import io.agentscope.core.middleware.ModelCallInput;
import io.agentscope.core.model.ChatUsage;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.ToolSchema;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.instrumentation.reactor.v3_1.ContextPropagationOperator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

class OtelTracingMiddlewareTest {

    private static final AttributeKey<String> GEN_AI_INPUT_MESSAGES =
            AttributeKey.stringKey("gen_ai.input.messages");
    private static final AttributeKey<String> GEN_AI_OUTPUT_MESSAGES =
            AttributeKey.stringKey("gen_ai.output.messages");
    private static final AttributeKey<String> GEN_AI_TOOL_CALL_ARGUMENTS =
            AttributeKey.stringKey("gen_ai.tool.call.arguments");
    private static final AttributeKey<String> GEN_AI_TOOL_CALL_RESULT =
            AttributeKey.stringKey("gen_ai.tool.call.result");

    private InMemorySpanExporter spanExporter;
    private OpenTelemetrySdk openTelemetrySdk;
    private OtelTracingMiddleware middleware;

    @BeforeEach
    void setUp() {
        GlobalOpenTelemetry.resetForTest();
        spanExporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider =
                SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                        .build();
        openTelemetrySdk =
                OpenTelemetrySdk.builder()
                        .setTracerProvider(tracerProvider)
                        .buildAndRegisterGlobal();
        middleware = new OtelTracingMiddleware(true);
    }

    @AfterEach
    void tearDown() {
        if (openTelemetrySdk != null) {
            openTelemetrySdk.close();
            openTelemetrySdk = null;
        }
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    void onAgent_createsInvokeAgentSpan() {
        Agent agent = stubAgent("test-agent", "agent-001");
        AgentInput input = new AgentInput(List.of());

        AgentStartEvent rse = new AgentStartEvent("sess-1", "reply-42", "test-agent");
        Flux<AgentEvent> result = middleware.onAgent(agent, null, input, in -> Flux.just(rse));
        result.collectList().block();

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());

        SpanData span = spans.get(0);
        assertEquals("invoke_agent test-agent", span.getName());
        assertEquals(StatusCode.OK, span.getStatus().getStatusCode());
        assertEquals(
                "invoke_agent",
                span.getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "gen_ai.operation.name")));
        assertEquals(
                "test-agent",
                span.getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "gen_ai.agent.name")));
        assertEquals(
                "agent-001",
                span.getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "gen_ai.agent.id")));
        assertEquals(
                "reply-42",
                span.getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "agentscope.agent.reply_id")));
    }

    @Test
    void onAgent_recordsErrorOnFailure() {
        Agent agent = stubAgent("err-agent", "agent-002");
        AgentInput input = new AgentInput(List.of());

        Flux<AgentEvent> result =
                middleware.onAgent(
                        agent, null, input, in -> Flux.error(new RuntimeException("boom")));
        try {
            result.collectList().block();
        } catch (Exception ignored) {
        }

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        assertEquals(StatusCode.ERROR, spans.get(0).getStatus().getStatusCode());
        assertEquals("RuntimeException", spans.get(0).getStatus().getDescription());
        assertFalse(spans.get(0).getStatus().getDescription().contains("boom"));
        assertTrue(
                spans.get(0).getEvents().stream()
                        .noneMatch(event -> "exception".equals(event.getName())),
                "raw exceptions must not be recorded on the span");
    }

    @Test
    void onAgent_keepsTopLevelReplyIdWhenSubagentStarts() {
        Agent agent = stubAgent("parent", "agent-parent");
        AgentStartEvent parentStart = new AgentStartEvent("sess-1", "parent-reply", "parent");
        AgentStartEvent childStart = new AgentStartEvent("sess-1", "child-reply", "child");
        childStart.withSource("parent/child");

        middleware
                .onAgent(
                        agent,
                        null,
                        new AgentInput(List.of()),
                        in -> Flux.just(parentStart, childStart))
                .collectList()
                .block();

        SpanData span = spanExporter.getFinishedSpanItems().get(0);
        assertEquals(
                "parent-reply",
                span.getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "agentscope.agent.reply_id")));
    }

    @Test
    void onAgent_recordsInputAndOutputMessagesWhenContentEnabled() {
        Agent agent = stubAgent("content-agent", "agent-content");
        AgentInput input = new AgentInput(List.of(new UserMessage("agent input")));

        middleware
                .onAgent(
                        agent,
                        null,
                        input,
                        in -> Flux.just(new AgentResultEvent(new AssistantMessage("agent output"))))
                .collectList()
                .block();

        var attributes = spanExporter.getFinishedSpanItems().get(0).getAttributes();
        String inputMessages =
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "gen_ai.input.messages"));
        String outputMessages =
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "gen_ai.output.messages"));
        assertNotNull(inputMessages);
        assertTrue(inputMessages.contains("agent input"));
        assertNotNull(outputMessages);
        assertTrue(outputMessages.contains("agent output"));
        assertTrue(outputMessages.contains("\"role\":\"assistant\""));
    }

    @Test
    void onAgent_defaultMiddlewareDoesNotRecordContent() {
        OtelTracingMiddleware defaultMiddleware = new OtelTracingMiddleware();
        Agent agent = stubAgent("privacy-agent", "agent-private");

        defaultMiddleware
                .onAgent(
                        agent,
                        null,
                        new AgentInput(List.of(new UserMessage("secret input"))),
                        in ->
                                Flux.just(
                                        new AgentResultEvent(
                                                new AssistantMessage("secret output"))))
                .collectList()
                .block();

        var attributes = spanExporter.getFinishedSpanItems().get(0).getAttributes();
        assertNull(
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "gen_ai.input.messages")));
        assertNull(
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "gen_ai.output.messages")));
    }

    @Test
    void onModelCall_createsChatSpanWithUsage() {
        Agent agent = stubAgent("model-agent", "agent-003");
        ChatUsage usage =
                ChatUsage.builder()
                        .inputTokens(100)
                        .outputTokens(50)
                        .cachedTokens(30)
                        .cacheCreationTokens(10)
                        .reasoningTokens(20)
                        .toolUsePromptTokens(15)
                        .time(1.5)
                        .build();
        ModelCallEndEvent mce = new ModelCallEndEvent("reply-1", usage);

        ModelCallInput input = new ModelCallInput(List.of(), null, null, new StubModel("gpt-4o"));
        Flux<AgentEvent> result = middleware.onModelCall(agent, null, input, in -> Flux.just(mce));
        result.collectList().block();

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());

        SpanData span = spans.get(0);
        assertEquals("chat gpt-4o", span.getName());
        assertEquals(StatusCode.OK, span.getStatus().getStatusCode());
        assertEquals(
                "chat",
                span.getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "gen_ai.operation.name")));
        assertEquals(
                "gpt-4o",
                span.getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "gen_ai.request.model")));
        assertEquals(
                100L,
                span.getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.longKey(
                                        "gen_ai.usage.input_tokens")));
        assertEquals(
                50L,
                span.getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.longKey(
                                        "gen_ai.usage.output_tokens")));
        assertEquals(
                30L,
                span.getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.longKey(
                                        "gen_ai.usage.cache_read.input_tokens")));
        assertEquals(
                10L,
                span.getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.longKey(
                                        "gen_ai.usage.cache_creation.input_tokens")));
        assertEquals(
                20L,
                span.getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.longKey(
                                        "gen_ai.usage.reasoning.output_tokens")));
        assertEquals(
                15L,
                span.getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.longKey(
                                        "agentscope.usage.tool_use_prompt_tokens")));
    }

    @Test
    void onModelCall_ignoresNullUsage() {
        Agent agent = stubAgent("model-agent-no-usage", "agent-no-usage");
        ModelCallEndEvent mce = new ModelCallEndEvent("reply-no-usage", null);

        ModelCallInput input = new ModelCallInput(List.of(), null, null, new StubModel("gpt-4o"));
        middleware.onModelCall(agent, null, input, in -> Flux.just(mce)).collectList().block();

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        assertEquals(
                null,
                spans.get(0)
                        .getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.longKey(
                                        "gen_ai.usage.input_tokens")));
    }

    @Test
    void onModelCall_recordsSemanticInputAndOutputMessages() {
        Agent agent = stubAgent("content-agent", "agent-106");
        ModelCallInput input =
                new ModelCallInput(
                        List.of(
                                new SystemMessage("You are concise."),
                                new UserMessage("What is AgentScope?")),
                        null,
                        null,
                        new StubModel("gpt-4o"));

        Flux<AgentEvent> result =
                middleware.onModelCall(
                        agent,
                        null,
                        input,
                        in ->
                                Flux.just(
                                        new TextBlockDeltaEvent("reply-1", "text", "AgentScope"),
                                        new TextBlockDeltaEvent("reply-1", "text", " is a"),
                                        new TextBlockDeltaEvent("reply-1", "text", " framework."),
                                        new ModelCallEndEvent(
                                                "reply-1", new ChatUsage(12, 8, 0.2))));
        result.collectList().block();

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());

        var attributes = spans.get(0).getAttributes();
        String inputMessagesJson =
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "gen_ai.input.messages"));
        String outputMessagesJson =
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "gen_ai.output.messages"));

        assertNotNull(inputMessagesJson);
        assertTrue(inputMessagesJson.contains("\"role\":\"system\""));
        assertTrue(inputMessagesJson.contains("\"type\":\"text\""));
        assertTrue(inputMessagesJson.contains("You are concise."));
        assertTrue(inputMessagesJson.contains("\"role\":\"user\""));
        assertTrue(inputMessagesJson.contains("What is AgentScope?"));
        assertNotNull(outputMessagesJson);
        assertTrue(outputMessagesJson.contains("\"role\":\"assistant\""));
        assertTrue(outputMessagesJson.contains("\"type\":\"text\""));
        assertTrue(outputMessagesJson.contains("AgentScope is a framework."));
    }

    @Test
    void onModelCall_recordsSemanticOutputReasoningContent() {
        Agent agent = stubAgent("reasoning-agent", "agent-107");
        ModelCallInput input = new ModelCallInput(List.of(), null, null, new StubModel("gpt-4o"));

        middleware
                .onModelCall(
                        agent,
                        null,
                        input,
                        in ->
                                Flux.just(
                                        new ThinkingBlockDeltaEvent(
                                                "reply-1", "thinking", "I should answer briefly."),
                                        new ModelCallEndEvent("reply-1", null)))
                .collectList()
                .block();

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        String outputMessagesJson =
                spans.get(0)
                        .getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "gen_ai.output.messages"));
        assertNotNull(outputMessagesJson);
        assertTrue(outputMessagesJson.contains("\"type\":\"reasoning\""));
        assertTrue(outputMessagesJson.contains("I should answer briefly."));
    }

    @Test
    void onModelCall_recordsSemanticToolCallAndToolResultMessages() {
        Agent agent = stubAgent("tool-message-agent", "agent-108");
        ToolUseBlock toolCall =
                ToolUseBlock.builder()
                        .id("call-1")
                        .name("search")
                        .input(Map.of("query", "AgentScope"))
                        .build();
        ModelCallInput input =
                new ModelCallInput(
                        List.of(
                                new UserMessage("Search for AgentScope."),
                                new AssistantMessage(toolCall),
                                new ToolResultMessage("call-1", "search", "AgentScope docs")),
                        null,
                        null,
                        new StubModel("gpt-4o"));

        middleware
                .onModelCall(
                        agent, null, input, in -> Flux.just(new ModelCallEndEvent("reply-1", null)))
                .collectList()
                .block();

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        String inputMessagesJson =
                spans.get(0)
                        .getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "gen_ai.input.messages"));

        assertNotNull(inputMessagesJson);
        assertTrue(inputMessagesJson.contains("\"role\":\"assistant\""));
        assertTrue(inputMessagesJson.contains("\"type\":\"tool_call\""));
        assertTrue(inputMessagesJson.contains("\"id\":\"call-1\""));
        assertTrue(inputMessagesJson.contains("\"name\":\"search\""));
        assertTrue(inputMessagesJson.contains("\"query\":\"AgentScope\""));
        assertTrue(inputMessagesJson.contains("\"role\":\"tool\""));
        assertTrue(inputMessagesJson.contains("\"type\":\"tool_call_response\""));
        assertTrue(inputMessagesJson.contains("\"response\""));
        assertTrue(inputMessagesJson.contains("AgentScope docs"));
    }

    @Test
    void onModelCall_recordsGenerationOptionsAndToolDefinitions() {
        Agent agent = stubAgent("options-agent", "agent-109");
        GenerateOptions options =
                GenerateOptions.builder().stream(true)
                        .temperature(0.3)
                        .topP(0.8)
                        .topK(20)
                        .maxTokens(128)
                        .presencePenalty(0.1)
                        .frequencyPenalty(0.2)
                        .seed(42L)
                        .reasoningEffort("low")
                        .build();
        ToolSchema tool =
                ToolSchema.builder()
                        .name("lookup")
                        .description("Lookup a term")
                        .parameters(
                                Map.of(
                                        "type",
                                        "object",
                                        "properties",
                                        Map.of("term", Map.of("type", "string"))))
                        .build();
        ModelCallInput input =
                new ModelCallInput(
                        List.of(new UserMessage("Lookup AgentScope.")),
                        List.of(tool),
                        options,
                        new StubModel("gpt-4o"));

        middleware
                .onModelCall(
                        agent, null, input, in -> Flux.just(new ModelCallEndEvent("reply-1", null)))
                .collectList()
                .block();

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        var attributes = spans.get(0).getAttributes();
        assertEquals(
                true,
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.booleanKey(
                                "gen_ai.request.stream")));
        assertEquals(
                0.3,
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.doubleKey(
                                "gen_ai.request.temperature")));
        assertEquals(
                0.8,
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.doubleKey(
                                "gen_ai.request.top_p")));
        assertEquals(
                20L,
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.longKey("gen_ai.request.top_k")));
        assertEquals(
                128L,
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.longKey(
                                "gen_ai.request.max_tokens")));
        assertEquals(
                0.1,
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.doubleKey(
                                "gen_ai.request.presence_penalty")));
        assertEquals(
                0.2,
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.doubleKey(
                                "gen_ai.request.frequency_penalty")));
        assertEquals(
                42L,
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.longKey("gen_ai.request.seed")));
        assertEquals(
                "low",
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "gen_ai.request.reasoning.level")));
        String toolDefinitionsJson =
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "gen_ai.tool.definitions"));
        assertNotNull(toolDefinitionsJson);
        assertTrue(toolDefinitionsJson.contains("\"type\":\"function\""));
        assertTrue(toolDefinitionsJson.contains("\"name\":\"lookup\""));
        assertTrue(toolDefinitionsJson.contains("\"description\":\"Lookup a term\""));
        assertTrue(toolDefinitionsJson.contains("\"term\""));
    }

    @Test
    void onModelCall_recordsStreamedToolCallOutputMessage() {
        Agent agent = stubAgent("stream-tool-agent", "agent-110");
        ModelCallInput input =
                new ModelCallInput(
                        List.of(new UserMessage("Lookup AgentScope.")),
                        null,
                        null,
                        new StubModel("gpt-4o"));

        middleware
                .onModelCall(
                        agent,
                        null,
                        input,
                        in ->
                                Flux.just(
                                        new ToolCallDeltaEvent(
                                                "reply-1", "call-1", "lookup", "{\"term\":"),
                                        new ToolCallDeltaEvent(
                                                "reply-1", "call-1", "lookup", "\"AgentScope\"}"),
                                        new ModelCallEndEvent("reply-1", null)))
                .collectList()
                .block();

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        String outputMessagesJson =
                spans.get(0)
                        .getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "gen_ai.output.messages"));

        assertNotNull(outputMessagesJson);
        assertTrue(outputMessagesJson.contains("\"role\":\"assistant\""));
        assertTrue(outputMessagesJson.contains("\"type\":\"tool_call\""));
        assertTrue(outputMessagesJson.contains("\"id\":\"call-1\""));
        assertTrue(outputMessagesJson.contains("\"name\":\"lookup\""));
        assertTrue(outputMessagesJson.contains("\"arguments\":{\"term\":\"AgentScope\"}"));
    }

    @Test
    void defaultMiddleware_doesNotRecordMessageContent() {
        OtelTracingMiddleware defaultMiddleware = new OtelTracingMiddleware();
        Agent agent = stubAgent("privacy-agent", "agent-111");
        ModelCallInput input =
                new ModelCallInput(
                        List.of(new UserMessage("secret input")),
                        null,
                        null,
                        new StubModel("gpt-4o"));

        defaultMiddleware
                .onModelCall(
                        agent,
                        null,
                        input,
                        in ->
                                Flux.just(
                                        new TextBlockDeltaEvent("reply-1", "text", "secret output"),
                                        new ModelCallEndEvent("reply-1", null)))
                .collectList()
                .block();

        var attributes = spanExporter.getFinishedSpanItems().get(0).getAttributes();
        assertNull(
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "gen_ai.input.messages")));
        assertNull(
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "gen_ai.output.messages")));
    }

    @Test
    void onActing_createsExecuteToolSpan() {
        Agent agent = stubAgent("tool-agent", "agent-004");
        ToolUseBlock toolCall =
                ToolUseBlock.builder()
                        .id("call-1")
                        .name("search")
                        .input(Map.of("q", "hello"))
                        .build();

        ToolResultEndEvent tre =
                new ToolResultEndEvent("reply-1", "call-1", "testTool", ToolResultState.SUCCESS);
        ActingInput input = new ActingInput(List.of(toolCall));
        Flux<AgentEvent> result = middleware.onActing(agent, null, input, in -> Flux.just(tre));
        result.collectList().block();

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());

        SpanData span = spans.get(0);
        assertEquals("execute_tool search", span.getName());
        assertEquals(StatusCode.OK, span.getStatus().getStatusCode());
        assertEquals(
                "execute_tool",
                span.getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "gen_ai.operation.name")));
        assertEquals(
                "search",
                span.getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "gen_ai.tool.name")));
        assertEquals(
                "call-1",
                span.getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "gen_ai.tool.call.id")));
    }

    @Test
    void onActing_recordsToolArgumentsAndTextResultWhenContentEnabled() {
        Agent agent = stubAgent("tool-content-agent", "agent-tool-content");
        ToolUseBlock toolCall =
                ToolUseBlock.builder()
                        .id("call-content")
                        .name("search")
                        .input(Map.of("query", "AgentScope"))
                        .build();
        ActingInput input = new ActingInput(List.of(toolCall));

        middleware
                .onActing(
                        agent,
                        null,
                        input,
                        in ->
                                Flux.just(
                                        new ToolResultTextDeltaEvent(
                                                "reply-1", "call-content", "search", "AgentScope "),
                                        new ToolResultTextDeltaEvent(
                                                "reply-1",
                                                "call-content",
                                                "search",
                                                "documentation"),
                                        new ToolResultEndEvent(
                                                "reply-1",
                                                "call-content",
                                                "search",
                                                ToolResultState.SUCCESS)))
                .collectList()
                .block();

        var attributes = spanExporter.getFinishedSpanItems().get(0).getAttributes();
        String arguments =
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "gen_ai.tool.call.arguments"));
        String result =
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "gen_ai.tool.call.result"));
        assertNotNull(arguments);
        assertTrue(arguments.contains("\"query\":\"AgentScope\""));
        assertNotNull(result);
        assertTrue(result.contains("AgentScope documentation"));
        assertTrue(result.contains("\"text\":\"AgentScope documentation\""));
    }

    @Test
    void onActing_recordsDataResultWhenContentEnabled() {
        Agent agent = stubAgent("tool-data-agent", "agent-tool-data");
        ToolUseBlock toolCall =
                ToolUseBlock.builder().id("call-data").name("lookup").input(Map.of()).build();
        ActingInput input = new ActingInput(List.of(toolCall));

        middleware
                .onActing(
                        agent,
                        null,
                        input,
                        in ->
                                Flux.just(
                                        new ToolResultDataDeltaEvent(
                                                "reply-1",
                                                "call-data",
                                                "lookup",
                                                TextBlock.builder().text("data result").build()),
                                        new ToolResultEndEvent(
                                                "reply-1",
                                                "call-data",
                                                "lookup",
                                                ToolResultState.SUCCESS)))
                .collectList()
                .block();

        String result =
                spanExporter
                        .getFinishedSpanItems()
                        .get(0)
                        .getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "gen_ai.tool.call.result"));
        assertNotNull(result);
        assertTrue(result.contains("data result"));
    }

    @Test
    void onActing_replacesMediaInToolResultAttributeWhenContentEnabled() {
        Agent agent = stubAgent("tool-media-agent", "agent-tool-media");
        ToolUseBlock toolCall =
                ToolUseBlock.builder().id("call-media").name("lookup").input(Map.of()).build();
        ActingInput input = new ActingInput(List.of(toolCall));
        String base64Marker = "BASE64-SECRET-TOKEN";
        String signedUrl = "https://media.example/secret?sig=abc";

        middleware
                .onActing(
                        agent,
                        null,
                        input,
                        in ->
                                Flux.just(
                                        new ToolResultDataDeltaEvent(
                                                "reply-1",
                                                "call-media",
                                                "lookup",
                                                ImageBlock.builder()
                                                        .source(
                                                                Base64Source.builder()
                                                                        .mediaType("image/png")
                                                                        .data(base64Marker)
                                                                        .build())
                                                        .build()),
                                        new ToolResultDataDeltaEvent(
                                                "reply-1",
                                                "call-media",
                                                "lookup",
                                                AudioBlock.builder()
                                                        .source(
                                                                URLSource.builder()
                                                                        .url(signedUrl)
                                                                        .build())
                                                        .build()),
                                        new ToolResultEndEvent(
                                                "reply-1",
                                                "call-media",
                                                "lookup",
                                                ToolResultState.SUCCESS)))
                .collectList()
                .block();

        SpanData span = spanExporter.getFinishedSpanItems().get(0);
        String result = span.getAttributes().get(GEN_AI_TOOL_CALL_RESULT);
        assertNotNull(result);
        assertTrue(result.contains("[image]"));
        assertTrue(result.contains("[audio]"));
        assertFalse(result.contains(base64Marker));
        assertFalse(result.contains("sig=abc"));
        assertNoSensitiveLeak(span, base64Marker, signedUrl);
    }

    @Test
    void onModelCall_replacesMediaInToolResultMessagesWhenContentEnabled() {
        String base64Marker = "BASE64-INPUT-MESSAGE-SECRET";
        Agent agent = stubAgent("tool-media-message-agent", "agent-tool-media-msg");
        ToolResultBlock mediaResult =
                ToolResultBlock.builder()
                        .id("call-media-msg")
                        .name("fetch")
                        .output(
                                List.of(
                                        TextBlock.builder().text("fetched content").build(),
                                        ImageBlock.builder()
                                                .source(
                                                        Base64Source.builder()
                                                                .mediaType("image/png")
                                                                .data(base64Marker)
                                                                .build())
                                                .build()))
                        .build();
        ModelCallInput input =
                new ModelCallInput(
                        List.of(new ToolResultMessage(List.of(mediaResult))),
                        null,
                        null,
                        new StubModel("gpt-4o"));

        middleware
                .onModelCall(
                        agent, null, input, in -> Flux.just(new ModelCallEndEvent("reply-1", null)))
                .collectList()
                .block();

        String inputMessagesJson =
                spanExporter
                        .getFinishedSpanItems()
                        .get(0)
                        .getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "gen_ai.input.messages"));

        assertNotNull(inputMessagesJson);
        assertTrue(inputMessagesJson.contains("fetched content"));
        assertTrue(inputMessagesJson.contains("[image]"));
        assertFalse(inputMessagesJson.contains(base64Marker));
        assertNoSensitiveLeak(spanExporter.getFinishedSpanItems().get(0), base64Marker);
    }

    @Test
    void onActing_recordsClassOnlyErrorAndSanitizesNestedMediaWhenContentEnabled() {
        Agent agent = stubAgent("tool-fail-agent", "agent-tool-fail");
        ToolUseBlock toolCall =
                ToolUseBlock.builder().id("call-fail").name("fetch").input(Map.of()).build();
        ActingInput input = new ActingInput(List.of(toolCall));
        String base64Marker = "BASE64-NESTED-FAIL-SECRET";
        String signedUrl = "https://media.example/nested?sig=fail";
        ToolResultBlock nested =
                ToolResultBlock.builder()
                        .id("inner-fail")
                        .name("inner-fetch")
                        .output(
                                List.of(
                                        TextBlock.builder().text("nested ok").build(),
                                        ImageBlock.builder()
                                                .source(
                                                        Base64Source.builder()
                                                                .mediaType("image/png")
                                                                .data(base64Marker)
                                                                .build())
                                                .build(),
                                        AudioBlock.builder()
                                                .source(URLSource.builder().url(signedUrl).build())
                                                .build(),
                                        VideoBlock.builder()
                                                .source(URLSource.builder().url(signedUrl).build())
                                                .build(),
                                        DataBlock.builder()
                                                .source(
                                                        Base64Source.builder()
                                                                .mediaType(
                                                                        "application/octet-stream")
                                                                .data(base64Marker)
                                                                .build())
                                                .build()))
                        .build();

        Flux<AgentEvent> result =
                middleware.onActing(
                        agent,
                        null,
                        input,
                        in ->
                                Flux.concat(
                                        Flux.just(
                                                new ToolResultDataDeltaEvent(
                                                        "reply-1", "call-fail", "fetch", nested)),
                                        Flux.error(new RuntimeException("boom"))));
        try {
            result.collectList().block();
        } catch (Exception ignored) {
        }

        SpanData span = spanExporter.getFinishedSpanItems().get(0);
        assertClassOnlyFailure(span);
        assertNotNull(span.getAttributes().get(GEN_AI_TOOL_CALL_ARGUMENTS));
        String resultJson = span.getAttributes().get(GEN_AI_TOOL_CALL_RESULT);
        assertNotNull(resultJson);
        assertTrue(resultJson.contains("nested ok"));
        assertTrue(resultJson.contains("[image]"));
        assertTrue(resultJson.contains("[audio]"));
        assertTrue(resultJson.contains("[video]"));
        assertTrue(resultJson.contains("[data]"));
        assertNoSensitiveLeak(span, base64Marker, signedUrl);
    }

    @Test
    void onActing_recordsClassOnlyErrorWithoutContentWhenContentDisabled() {
        OtelTracingMiddleware defaultMiddleware = new OtelTracingMiddleware();
        Agent agent = stubAgent("tool-fail-private", "agent-tool-fail-private");
        ToolUseBlock toolCall =
                ToolUseBlock.builder()
                        .id("call-private-fail")
                        .name("fetch")
                        .input(Map.of())
                        .build();
        String base64Marker = "BASE64-PRIVATE-FAIL-SECRET";

        Flux<AgentEvent> result =
                defaultMiddleware.onActing(
                        agent,
                        null,
                        new ActingInput(List.of(toolCall)),
                        in ->
                                Flux.concat(
                                        Flux.just(
                                                new ToolResultDataDeltaEvent(
                                                        "reply-1",
                                                        "call-private-fail",
                                                        "fetch",
                                                        ImageBlock.builder()
                                                                .source(
                                                                        Base64Source.builder()
                                                                                .mediaType(
                                                                                        "image/png")
                                                                                .data(base64Marker)
                                                                                .build())
                                                                .build())),
                                        Flux.error(new RuntimeException("boom"))));
        try {
            result.collectList().block();
        } catch (Exception ignored) {
        }

        SpanData span = spanExporter.getFinishedSpanItems().get(0);
        assertClassOnlyFailure(span);
        assertNull(span.getAttributes().get(GEN_AI_TOOL_CALL_ARGUMENTS));
        assertNull(span.getAttributes().get(GEN_AI_TOOL_CALL_RESULT));
        assertNoSensitiveLeak(span, base64Marker);
    }

    @Test
    void onModelCall_recordsClassOnlyErrorAndSanitizesNestedMediaWhenContentEnabled() {
        Agent agent = stubAgent("model-fail-agent", "agent-model-fail");
        String base64Marker = "BASE64-MODEL-FAIL-SECRET";
        String signedUrl = "https://media.example/model?sig=fail";
        ToolResultBlock mediaResult =
                ToolResultBlock.builder()
                        .id("call-model-fail")
                        .name("fetch")
                        .output(
                                List.of(
                                        TextBlock.builder().text("request payload").build(),
                                        ToolResultBlock.builder()
                                                .id("call-model-fail-inner")
                                                .name("nested")
                                                .output(
                                                        List.of(
                                                                ImageBlock.builder()
                                                                        .source(
                                                                                Base64Source
                                                                                        .builder()
                                                                                        .mediaType(
                                                                                                "image/png")
                                                                                        .data(
                                                                                                base64Marker)
                                                                                        .build())
                                                                        .build(),
                                                                AudioBlock.builder()
                                                                        .source(
                                                                                URLSource.builder()
                                                                                        .url(
                                                                                                signedUrl)
                                                                                        .build())
                                                                        .build()))
                                                .build()))
                        .build();
        ModelCallInput input =
                new ModelCallInput(
                        List.of(new ToolResultMessage(List.of(mediaResult))),
                        null,
                        null,
                        new StubModel("gpt-4o"));

        Flux<AgentEvent> result =
                middleware.onModelCall(
                        agent, null, input, in -> Flux.error(new RuntimeException("boom")));
        try {
            result.collectList().block();
        } catch (Exception ignored) {
        }

        SpanData span = spanExporter.getFinishedSpanItems().get(0);
        assertClassOnlyFailure(span);
        String inputMessagesJson = span.getAttributes().get(GEN_AI_INPUT_MESSAGES);
        assertNotNull(inputMessagesJson);
        assertTrue(inputMessagesJson.contains("request payload"));
        assertTrue(inputMessagesJson.contains("[image]"));
        assertTrue(inputMessagesJson.contains("[audio]"));
        assertNull(span.getAttributes().get(GEN_AI_OUTPUT_MESSAGES));
        assertNoSensitiveLeak(span, base64Marker, signedUrl);
    }

    @Test
    void onModelCall_recordsClassOnlyErrorWithoutContentWhenContentDisabled() {
        OtelTracingMiddleware defaultMiddleware = new OtelTracingMiddleware();
        Agent agent = stubAgent("model-fail-private", "agent-model-fail-private");
        String base64Marker = "BASE64-MODEL-PRIVATE-SECRET";
        ToolResultBlock mediaResult =
                ToolResultBlock.builder()
                        .id("call-private-model")
                        .name("fetch")
                        .output(
                                List.of(
                                        ImageBlock.builder()
                                                .source(
                                                        Base64Source.builder()
                                                                .mediaType("image/png")
                                                                .data(base64Marker)
                                                                .build())
                                                .build()))
                        .build();
        ModelCallInput input =
                new ModelCallInput(
                        List.of(new ToolResultMessage(List.of(mediaResult))),
                        null,
                        null,
                        new StubModel("gpt-4o"));

        Flux<AgentEvent> result =
                defaultMiddleware.onModelCall(
                        agent, null, input, in -> Flux.error(new RuntimeException("boom")));
        try {
            result.collectList().block();
        } catch (Exception ignored) {
        }

        SpanData span = spanExporter.getFinishedSpanItems().get(0);
        assertClassOnlyFailure(span);
        assertNull(span.getAttributes().get(GEN_AI_INPUT_MESSAGES));
        assertNull(span.getAttributes().get(GEN_AI_OUTPUT_MESSAGES));
        assertNoSensitiveLeak(span, base64Marker);
    }

    @Test
    void onAgent_recordsClassOnlyErrorAndSanitizesNestedMediaInputWhenContentEnabled() {
        Agent agent = stubAgent("agent-fail-media", "agent-fail-media-id");
        String base64Marker = "BASE64-AGENT-FAIL-SECRET";
        String signedUrl = "https://media.example/agent?sig=fail";
        ToolResultBlock mediaResult =
                ToolResultBlock.builder()
                        .id("call-agent-fail")
                        .name("fetch")
                        .output(
                                List.of(
                                        TextBlock.builder().text("agent request").build(),
                                        ToolResultBlock.builder()
                                                .id("call-agent-fail-inner")
                                                .name("nested")
                                                .output(
                                                        List.of(
                                                                ImageBlock.builder()
                                                                        .source(
                                                                                Base64Source
                                                                                        .builder()
                                                                                        .mediaType(
                                                                                                "image/png")
                                                                                        .data(
                                                                                                base64Marker)
                                                                                        .build())
                                                                        .build(),
                                                                AudioBlock.builder()
                                                                        .source(
                                                                                URLSource.builder()
                                                                                        .url(
                                                                                                signedUrl)
                                                                                        .build())
                                                                        .build()))
                                                .build()))
                        .build();
        AgentInput input =
                new AgentInput(
                        List.of(
                                new UserMessage(
                                        List.of(
                                                TextBlock.builder()
                                                        .text("agent fail input")
                                                        .build(),
                                                ImageBlock.builder()
                                                        .source(
                                                                Base64Source.builder()
                                                                        .mediaType("image/png")
                                                                        .data(base64Marker)
                                                                        .build())
                                                        .build())),
                                new ToolResultMessage(List.of(mediaResult))));

        Flux<AgentEvent> result =
                middleware.onAgent(
                        agent, null, input, in -> Flux.error(new RuntimeException("boom")));
        try {
            result.collectList().block();
        } catch (Exception ignored) {
        }

        SpanData span = spanExporter.getFinishedSpanItems().get(0);
        assertClassOnlyFailure(span);
        String inputMessagesJson = span.getAttributes().get(GEN_AI_INPUT_MESSAGES);
        assertNotNull(inputMessagesJson);
        assertTrue(inputMessagesJson.contains("agent fail input"));
        assertTrue(inputMessagesJson.contains("agent request"));
        assertTrue(inputMessagesJson.contains("[image]"));
        assertTrue(inputMessagesJson.contains("[audio]"));
        assertNull(span.getAttributes().get(GEN_AI_OUTPUT_MESSAGES));
        assertNoSensitiveLeak(span, base64Marker, signedUrl);
    }

    @Test
    void onAgent_defaultMiddlewareFailureDoesNotRecordContent() {
        OtelTracingMiddleware defaultMiddleware = new OtelTracingMiddleware();
        Agent agent = stubAgent("agent-fail-private", "agent-fail-private-id");

        Flux<AgentEvent> result =
                defaultMiddleware.onAgent(
                        agent,
                        null,
                        new AgentInput(List.of(new UserMessage("secret input"))),
                        in -> Flux.error(new RuntimeException("boom")));
        try {
            result.collectList().block();
        } catch (Exception ignored) {
        }

        SpanData span = spanExporter.getFinishedSpanItems().get(0);
        assertClassOnlyFailure(span);
        assertNull(span.getAttributes().get(GEN_AI_INPUT_MESSAGES));
        assertNull(span.getAttributes().get(GEN_AI_OUTPUT_MESSAGES));
        assertFalse(span.getAttributes().asMap().toString().contains("secret input"));
    }

    @Test
    void onAgent_replacesMediaRecursivelyInOutputMessagesWhenContentEnabled() {
        Agent agent = stubAgent("output-media-agent", "agent-output-media");
        String base64Marker = "BASE64-OUTPUT-SECRET";
        String signedUrl = "https://media.example/output?sig=xyz";
        ToolResultBlock nested =
                ToolResultBlock.builder()
                        .id("out-inner")
                        .name("render")
                        .output(
                                List.of(
                                        TextBlock.builder().text("rendered").build(),
                                        ImageBlock.builder()
                                                .source(
                                                        Base64Source.builder()
                                                                .mediaType("image/png")
                                                                .data(base64Marker)
                                                                .build())
                                                .build(),
                                        AudioBlock.builder()
                                                .source(URLSource.builder().url(signedUrl).build())
                                                .build()))
                        .build();

        middleware
                .onAgent(
                        agent,
                        null,
                        new AgentInput(List.of()),
                        in ->
                                Flux.just(
                                        new AgentResultEvent(
                                                new AssistantMessage(
                                                        List.of(
                                                                TextBlock.builder()
                                                                        .text("rendered done")
                                                                        .build(),
                                                                nested)))))
                .collectList()
                .block();

        SpanData span = spanExporter.getFinishedSpanItems().get(0);
        String outputMessagesJson = span.getAttributes().get(GEN_AI_OUTPUT_MESSAGES);
        assertNotNull(outputMessagesJson);
        assertTrue(outputMessagesJson.contains("rendered done"));
        assertTrue(outputMessagesJson.contains("[image]"));
        assertTrue(outputMessagesJson.contains("[audio]"));
        assertNoSensitiveLeak(span, base64Marker, signedUrl);
    }

    @Test
    void onActing_defaultMiddlewareDoesNotRecordToolContent() {
        OtelTracingMiddleware defaultMiddleware = new OtelTracingMiddleware();
        Agent agent = stubAgent("private-tool-agent", "agent-private-tool");
        ToolUseBlock toolCall =
                ToolUseBlock.builder()
                        .id("call-private")
                        .name("search")
                        .input(Map.of("query", "secret"))
                        .build();

        defaultMiddleware
                .onActing(
                        agent,
                        null,
                        new ActingInput(List.of(toolCall)),
                        in ->
                                Flux.just(
                                        new ToolResultTextDeltaEvent(
                                                "reply-1",
                                                "call-private",
                                                "search",
                                                "secret result"),
                                        new ToolResultEndEvent(
                                                "reply-1",
                                                "call-private",
                                                "search",
                                                ToolResultState.SUCCESS)))
                .collectList()
                .block();

        var attributes = spanExporter.getFinishedSpanItems().get(0).getAttributes();
        assertNull(
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "gen_ai.tool.call.arguments")));
        assertNull(
                attributes.get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "gen_ai.tool.call.result")));
    }

    @Test
    void nestedSpans_linkedAsParentAndChild() {
        Agent agent = stubAgent("nest-agent", "agent-100");
        AgentInput agentIn = new AgentInput(List.of());
        ModelCallInput modelIn = new ModelCallInput(List.of(), null, null, new StubModel("gpt-4o"));
        ToolUseBlock toolCall =
                ToolUseBlock.builder().id("call-x").name("search").input(Map.of()).build();
        ActingInput actingIn = new ActingInput(List.of(toolCall));

        Flux<AgentEvent> result =
                middleware.onAgent(
                        agent,
                        null,
                        agentIn,
                        in ->
                                middleware
                                        .onModelCall(
                                                agent,
                                                null,
                                                modelIn,
                                                m ->
                                                        Flux.just(
                                                                new ModelCallEndEvent(
                                                                        "reply-1",
                                                                        new ChatUsage(1, 1, 0.1))))
                                        .thenMany(
                                                middleware.onActing(
                                                        agent,
                                                        null,
                                                        actingIn,
                                                        a ->
                                                                Flux.just(
                                                                        new ToolResultEndEvent(
                                                                                "reply-1",
                                                                                "call-x",
                                                                                "search",
                                                                                ToolResultState
                                                                                        .SUCCESS)))));
        result.collectList().block();

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(3, spans.size());

        SpanData invoke =
                spans.stream()
                        .filter(s -> s.getName().startsWith("invoke_agent"))
                        .findFirst()
                        .orElseThrow();
        SpanData chat =
                spans.stream()
                        .filter(s -> s.getName().startsWith("chat"))
                        .findFirst()
                        .orElseThrow();
        SpanData exec =
                spans.stream()
                        .filter(s -> s.getName().startsWith("execute_tool"))
                        .findFirst()
                        .orElseThrow();

        // Same trace
        assertEquals(invoke.getTraceId(), chat.getTraceId());
        assertEquals(invoke.getTraceId(), exec.getTraceId());
        // Children point at invoke_agent as parent
        assertEquals(invoke.getSpanId(), chat.getParentSpanId());
        assertEquals(invoke.getSpanId(), exec.getParentSpanId());
    }

    @Test
    void nestedSpans_preservedAcrossThreadHop() {
        Agent agent = stubAgent("async-agent", "agent-101");
        AgentInput agentIn = new AgentInput(List.of());
        ModelCallInput modelIn = new ModelCallInput(List.of(), null, null, new StubModel("gpt-4o"));

        // publishOn() between the two middlewares forces a thread hop — the
        // OTel ThreadLocal-only approach would lose the parent here.
        Flux<AgentEvent> result =
                middleware.onAgent(
                        agent,
                        null,
                        agentIn,
                        in ->
                                Flux.just(new AgentStartEvent("s", "r", "async-agent"))
                                        .publishOn(Schedulers.parallel())
                                        .thenMany(
                                                middleware.onModelCall(
                                                        agent,
                                                        null,
                                                        modelIn,
                                                        m ->
                                                                Flux.just(
                                                                                (AgentEvent)
                                                                                        new ModelCallEndEvent(
                                                                                                "r",
                                                                                                new ChatUsage(
                                                                                                        1,
                                                                                                        1,
                                                                                                        0.1)))
                                                                        .publishOn(
                                                                                Schedulers
                                                                                        .parallel()))));
        result.collectList().block(Duration.ofSeconds(5));

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(2, spans.size());

        SpanData invoke =
                spans.stream()
                        .filter(s -> s.getName().startsWith("invoke_agent"))
                        .findFirst()
                        .orElseThrow();
        SpanData chat =
                spans.stream()
                        .filter(s -> s.getName().startsWith("chat"))
                        .findFirst()
                        .orElseThrow();

        assertEquals(invoke.getTraceId(), chat.getTraceId());
        assertEquals(
                invoke.getSpanId(),
                chat.getParentSpanId(),
                "chat span must remain a child of invoke_agent across publishOn");
    }

    @Test
    void onAgent_endsSpanOnCancel() throws InterruptedException {
        Agent agent = stubAgent("cancel-agent", "agent-102");
        AgentInput input = new AgentInput(List.of());

        CountDownLatch subscribed = new CountDownLatch(1);
        Flux<AgentEvent> upstream =
                Flux.<AgentEvent>never().doOnSubscribe(s -> subscribed.countDown());

        AtomicReference<reactor.core.Disposable> disposableRef = new AtomicReference<>();
        disposableRef.set(middleware.onAgent(agent, null, input, in -> upstream).subscribe());

        assertTrue(subscribed.await(2, TimeUnit.SECONDS), "subscription should occur");
        disposableRef.get().dispose();

        // Span ends synchronously inside doOnCancel, but allow a brief moment.
        Thread.sleep(50);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size(), "cancelled stream must still end the span");
        assertEquals(StatusCode.ERROR, spans.get(0).getStatus().getStatusCode());
        assertEquals("cancelled", spans.get(0).getStatus().getDescription());
    }

    @Test
    void onActing_aggregatesAllToolCallIds() {
        Agent agent = stubAgent("multi-tool-agent", "agent-103");
        ToolUseBlock t1 =
                ToolUseBlock.builder().id("call-a").name("search").input(Map.of()).build();
        ToolUseBlock t2 = ToolUseBlock.builder().id("call-b").name("calc").input(Map.of()).build();
        ActingInput input = new ActingInput(List.of(t1, t2));

        ToolResultEndEvent r1 =
                new ToolResultEndEvent("reply-1", "call-a", "search", ToolResultState.SUCCESS);
        ToolResultEndEvent r2 =
                new ToolResultEndEvent("reply-1", "call-b", "calc", ToolResultState.SUCCESS);

        middleware
                .onActing(
                        agent,
                        null,
                        input,
                        in ->
                                Flux.just(
                                        new ToolResultTextDeltaEvent(
                                                "reply-1", "call-a", "search", "search result"),
                                        r1,
                                        new ToolResultTextDeltaEvent(
                                                "reply-1", "call-b", "calc", "calc result"),
                                        r2))
                .collectList()
                .block();

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());

        String callIds =
                spans.get(0)
                        .getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "gen_ai.tool.call.id"));
        assertNotNull(callIds);
        assertTrue(callIds.contains("call-a"));
        assertTrue(callIds.contains("call-b"));
        String arguments =
                spans.get(0)
                        .getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "gen_ai.tool.call.arguments"));
        String toolResults =
                spans.get(0)
                        .getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "gen_ai.tool.call.result"));
        assertNotNull(arguments);
        assertTrue(arguments.contains("call-a"));
        assertTrue(arguments.contains("call-b"));
        assertNotNull(toolResults);
        assertTrue(toolResults.contains("search result"));
        assertTrue(toolResults.contains("calc result"));
        assertTrue(toolResults.contains("\"state\":\"success\""));
    }

    @Test
    void onActing_multiTool_spanNameTruncatedButAttributeContainsAll() {
        Agent agent = stubAgent("multi-tool-agent2", "agent-104");
        ToolUseBlock t1 = ToolUseBlock.builder().id("c1").name("search").input(Map.of()).build();
        ToolUseBlock t2 = ToolUseBlock.builder().id("c2").name("calc").input(Map.of()).build();
        ToolUseBlock t3 = ToolUseBlock.builder().id("c3").name("fetchUrl").input(Map.of()).build();
        ActingInput input = new ActingInput(List.of(t1, t2, t3));

        ToolResultEndEvent r1 =
                new ToolResultEndEvent("reply-1", "c1", "search", ToolResultState.SUCCESS);
        ToolResultEndEvent r2 =
                new ToolResultEndEvent("reply-1", "c2", "calc", ToolResultState.SUCCESS);
        ToolResultEndEvent r3 =
                new ToolResultEndEvent("reply-1", "c3", "fetchUrl", ToolResultState.SUCCESS);

        middleware.onActing(agent, null, input, in -> Flux.just(r1, r2, r3)).collectList().block();

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());

        SpanData span = spans.get(0);
        // span name uses first tool + count suffix to cap cardinality
        assertEquals("execute_tool search (+2 more)", span.getName());
        // gen_ai.tool.name attribute still has all names
        String toolNames =
                span.getAttributes()
                        .get(
                                io.opentelemetry.api.common.AttributeKey.stringKey(
                                        "gen_ai.tool.name"));
        assertNotNull(toolNames);
        assertTrue(toolNames.contains("search"));
        assertTrue(toolNames.contains("calc"));
        assertTrue(toolNames.contains("fetchUrl"));
    }

    @Test
    void onActing_singleTool_spanNameIsToolName() {
        Agent agent = stubAgent("single-tool-agent", "agent-105");
        ToolUseBlock t1 = ToolUseBlock.builder().id("c1").name("myTool").input(Map.of()).build();
        ActingInput input = new ActingInput(List.of(t1));

        ToolResultEndEvent r1 =
                new ToolResultEndEvent("reply-1", "c1", "myTool", ToolResultState.SUCCESS);

        middleware.onActing(agent, null, input, in -> Flux.just(r1)).collectList().block();

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(1, spans.size());
        assertEquals("execute_tool myTool", spans.get(0).getName());
    }

    @Test
    void injectedSdk_recordsHooksOnSdkBAndLeavesGlobalSdkAUntouched() {
        InMemorySpanExporter exporterB = InMemorySpanExporter.create();
        OpenTelemetrySdk sdkB = buildSdk(exporterB, false);
        try {
            OtelTracingMiddleware injected = new OtelTracingMiddleware(sdkB);
            Agent agent = stubAgent("injected-agent", "agent-b");
            AgentStartEvent start = new AgentStartEvent("sess-b", "reply-b", "injected-agent");
            ModelCallEndEvent modelEnd = new ModelCallEndEvent("reply-b", new ChatUsage(1, 1, 0.1));
            ToolUseBlock toolCall =
                    ToolUseBlock.builder().id("call-b").name("search").input(Map.of()).build();
            ToolResultEndEvent toolEnd =
                    new ToolResultEndEvent("reply-b", "call-b", "search", ToolResultState.SUCCESS);

            injected.onAgent(agent, null, new AgentInput(List.of()), in -> Flux.just(start))
                    .collectList()
                    .block();
            injected.onModelCall(
                            agent,
                            null,
                            new ModelCallInput(List.of(), null, null, new StubModel("gpt-4o")),
                            in -> Flux.just(modelEnd))
                    .collectList()
                    .block();
            injected.onActing(
                            agent,
                            null,
                            new ActingInput(List.of(toolCall)),
                            in -> Flux.just(toolEnd))
                    .collectList()
                    .block();

            List<SpanData> spansB = exporterB.getFinishedSpanItems();
            assertEquals(3, spansB.size());
            assertNotNull(spanNamed(spansB, "invoke_agent"));
            assertNotNull(spanNamed(spansB, "chat"));
            assertNotNull(spanNamed(spansB, "execute_tool"));
            for (SpanData span : spansB) {
                assertEquals("io.agentscope", span.getInstrumentationScopeInfo().getName());
                assertTrue(span.hasEnded());
            }
            assertEquals(0, spanExporter.getFinishedSpanItems().size());
            assertGlobalStillExports(openTelemetrySdk, spanExporter, exporterB);
        } finally {
            sdkB.close();
        }
    }

    @Test
    void injectedSdk_preservesNestedParentsAcrossSchedulerHopsAndUpstreamContext() {
        InMemorySpanExporter exporterB = InMemorySpanExporter.create();
        OpenTelemetrySdk sdkB = buildSdk(exporterB, false);
        Span upstream = sdkB.getTracer("io.agentscope").spanBuilder("upstream").startSpan();
        Context upstreamContext = Context.root().with(upstream);
        try {
            OtelTracingMiddleware injected = new OtelTracingMiddleware(sdkB);
            Agent agent = stubAgent("hop-agent", "agent-hop");
            ModelCallInput modelIn =
                    new ModelCallInput(List.of(), null, null, new StubModel("gpt-4o"));
            ToolUseBlock toolCall =
                    ToolUseBlock.builder().id("call-hop").name("search").input(Map.of()).build();
            ActingInput actingIn = new ActingInput(List.of(toolCall));

            injected.onAgent(
                            agent,
                            null,
                            new AgentInput(List.of()),
                            in ->
                                    injected.onModelCall(
                                                    agent,
                                                    null,
                                                    modelIn,
                                                    m ->
                                                            Flux.just(
                                                                    new ModelCallEndEvent(
                                                                            "reply-hop",
                                                                            new ChatUsage(
                                                                                    1, 1, 0.1))))
                                            .subscribeOn(Schedulers.boundedElastic())
                                            .publishOn(Schedulers.parallel())
                                            .thenMany(
                                                    injected.onActing(
                                                                    agent,
                                                                    null,
                                                                    actingIn,
                                                                    a ->
                                                                            Flux.just(
                                                                                    new ToolResultEndEvent(
                                                                                            "reply-hop",
                                                                                            "call-hop",
                                                                                            "search",
                                                                                            ToolResultState
                                                                                                    .SUCCESS)))
                                                            .subscribeOn(
                                                                    Schedulers.boundedElastic())
                                                            .publishOn(Schedulers.parallel())))
                    .contextWrite(
                            reactorCtx ->
                                    ContextPropagationOperator.storeOpenTelemetryContext(
                                            reactorCtx, upstreamContext))
                    .collectList()
                    .block(Duration.ofSeconds(5));

            List<SpanData> spans = exporterB.getFinishedSpanItems();
            assertEquals(3, spans.size());
            SpanData invoke = spanNamed(spans, "invoke_agent");
            SpanData chat = spanNamed(spans, "chat");
            SpanData exec = spanNamed(spans, "execute_tool");

            assertEquals(upstream.getSpanContext().getTraceId(), invoke.getTraceId());
            assertEquals(upstream.getSpanContext().getSpanId(), invoke.getParentSpanId());
            assertEquals(invoke.getTraceId(), chat.getTraceId());
            assertEquals(invoke.getTraceId(), exec.getTraceId());
            assertEquals(invoke.getSpanId(), chat.getParentSpanId());
            assertEquals(invoke.getSpanId(), exec.getParentSpanId());
            assertEquals(0, spanExporter.getFinishedSpanItems().size());
            assertGlobalStillExports(openTelemetrySdk, spanExporter, exporterB);
        } finally {
            upstream.end();
            sdkB.close();
        }
    }

    @Test
    void defaultConstructor_resolvesGlobalSdkRegisteredAfterConstruction() {
        openTelemetrySdk.close();
        openTelemetrySdk = null;
        GlobalOpenTelemetry.resetForTest();

        OtelTracingMiddleware early = new OtelTracingMiddleware();
        InMemorySpanExporter lateExporter = InMemorySpanExporter.create();
        OpenTelemetrySdk lateSdk = registerGlobalSdk(lateExporter);
        try {
            early.onAgent(
                            stubAgent("late-agent", "agent-late"),
                            null,
                            new AgentInput(List.of()),
                            in ->
                                    Flux.just(
                                            new AgentStartEvent(
                                                    "sess", "reply-late", "late-agent")))
                    .collectList()
                    .block();

            List<SpanData> spans = lateExporter.getFinishedSpanItems();
            assertEquals(1, spans.size());
            assertEquals("invoke_agent late-agent", spans.get(0).getName());
            assertEquals("io.agentscope", spans.get(0).getInstrumentationScopeInfo().getName());
            assertEquals(StatusCode.OK, spans.get(0).getStatus().getStatusCode());
            assertGlobalStillExports(lateSdk, lateExporter, null);
        } finally {
            lateSdk.close();
        }
    }

    @Test
    void injectedSdk_rejectsNullAndNoopRunsDownstreamWithoutSpans() {
        NullPointerException rejected =
                assertThrows(NullPointerException.class, () -> new OtelTracingMiddleware(null));
        assertEquals("openTelemetry must not be null", rejected.getMessage());

        OtelTracingMiddleware noopMiddleware = new OtelTracingMiddleware(OpenTelemetry.noop());
        Agent agent = stubAgent("noop-agent", "agent-noop");
        AgentStartEvent start = new AgentStartEvent("sess-n", "reply-n", "noop-agent");
        ModelCallEndEvent modelEnd = new ModelCallEndEvent("reply-n", new ChatUsage(2, 3, 0.2));
        ToolUseBlock toolCall =
                ToolUseBlock.builder().id("call-n").name("search").input(Map.of()).build();
        ToolResultEndEvent toolEnd =
                new ToolResultEndEvent("reply-n", "call-n", "search", ToolResultState.SUCCESS);

        List<AgentEvent> agentEvents =
                noopMiddleware
                        .onAgent(agent, null, new AgentInput(List.of()), in -> Flux.just(start))
                        .collectList()
                        .block();
        List<AgentEvent> modelEvents =
                noopMiddleware
                        .onModelCall(
                                agent,
                                null,
                                new ModelCallInput(List.of(), null, null, new StubModel("gpt-4o")),
                                in -> Flux.just(modelEnd))
                        .collectList()
                        .block();
        List<AgentEvent> toolEvents =
                noopMiddleware
                        .onActing(
                                agent,
                                null,
                                new ActingInput(List.of(toolCall)),
                                in -> Flux.just(toolEnd))
                        .collectList()
                        .block();

        assertEquals(List.of(start), agentEvents);
        assertEquals(List.of(modelEnd), modelEvents);
        assertEquals(List.of(toolEnd), toolEvents);
        assertEquals(0, spanExporter.getFinishedSpanItems().size());
        assertGlobalStillExports(openTelemetrySdk, spanExporter, null);
    }

    @Test
    void injectedSdk_resolvesTracerOnceAndReusesItOnEachHook() {
        AtomicInteger lookups = new AtomicInteger();
        OpenTelemetry counting =
                new OpenTelemetry() {
                    @Override
                    public TracerProvider getTracerProvider() {
                        lookups.incrementAndGet();
                        return OpenTelemetry.noop().getTracerProvider();
                    }

                    @Override
                    public ContextPropagators getPropagators() {
                        return OpenTelemetry.noop().getPropagators();
                    }
                };
        OtelTracingMiddleware cached = new OtelTracingMiddleware(counting);
        assertEquals(1, lookups.get(), "app-owned tracer is resolved once at construction");

        Agent agent = stubAgent("cache-agent", "agent-cache");
        AgentStartEvent start = new AgentStartEvent("sess-c", "reply-c", "cache-agent");
        assertEquals(
                List.of(start),
                cached.onAgent(agent, null, new AgentInput(List.of()), in -> Flux.just(start))
                        .collectList()
                        .block());
        ModelCallEndEvent modelEnd = new ModelCallEndEvent("reply-c", new ChatUsage(1, 1, 0.1));
        assertEquals(
                List.of(modelEnd),
                cached.onModelCall(
                                agent,
                                null,
                                new ModelCallInput(List.of(), null, null, new StubModel("gpt-4o")),
                                in -> Flux.just(modelEnd))
                        .collectList()
                        .block());
        assertEquals(
                List.of(),
                cached.onActing(agent, null, new ActingInput(List.of()), in -> Flux.empty())
                        .collectList()
                        .block());
        assertEquals(1, lookups.get(), "hooks reuse the tracer cached for this OpenTelemetry");
        assertEquals(0, spanExporter.getFinishedSpanItems().size());
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static OpenTelemetrySdk registerGlobalSdk(InMemorySpanExporter exporter) {
        return buildSdk(exporter, true);
    }

    private static OpenTelemetrySdk buildSdk(
            InMemorySpanExporter exporter, boolean registerGlobal) {
        SdkTracerProvider tracerProvider =
                SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                        .build();
        var builder = OpenTelemetrySdk.builder().setTracerProvider(tracerProvider);
        return registerGlobal ? builder.buildAndRegisterGlobal() : builder.build();
    }

    private static SpanData spanNamed(List<SpanData> spans, String prefix) {
        return spans.stream()
                .filter(span -> span.getName().startsWith(prefix))
                .findFirst()
                .orElseThrow();
    }

    private static void assertClassOnlyFailure(SpanData span) {
        assertEquals(StatusCode.ERROR, span.getStatus().getStatusCode());
        assertEquals("RuntimeException", span.getStatus().getDescription());
        assertFalse(span.getStatus().getDescription().contains("boom"));
        assertTrue(
                span.getEvents().stream().noneMatch(event -> "exception".equals(event.getName())),
                "raw exceptions must not be recorded on the span");
        assertFalse(span.getAttributes().asMap().toString().contains("boom"));
    }

    private static void assertNoSensitiveLeak(SpanData span, String... markers) {
        span.getAttributes()
                .forEach(
                        (key, value) -> {
                            String text = String.valueOf(value);
                            for (String marker : markers) {
                                assertFalse(
                                        text.contains(marker),
                                        "attribute " + key.getKey() + " leaked " + marker);
                            }
                        });
    }

    /**
     * GlobalOpenTelemetry.get() wraps the registered SDK, so identity is the tracer provider plus a
     * span that only the global exporter receives.
     */
    private static void assertGlobalStillExports(
            OpenTelemetrySdk sdk,
            InMemorySpanExporter globalExporter,
            InMemorySpanExporter otherExporter) {
        assertSame(sdk.getTracerProvider(), GlobalOpenTelemetry.get().getTracerProvider());
        int globalBefore = globalExporter.getFinishedSpanItems().size();
        int otherBefore = otherExporter == null ? 0 : otherExporter.getFinishedSpanItems().size();
        GlobalOpenTelemetry.getTracer("io.agentscope")
                .spanBuilder("global-probe")
                .startSpan()
                .end();
        List<SpanData> globalSpans = globalExporter.getFinishedSpanItems();
        assertEquals(globalBefore + 1, globalSpans.size());
        SpanData probe = globalSpans.get(globalSpans.size() - 1);
        assertEquals("global-probe", probe.getName());
        assertEquals("io.agentscope", probe.getInstrumentationScopeInfo().getName());
        if (otherExporter != null) {
            assertEquals(otherBefore, otherExporter.getFinishedSpanItems().size());
        }
    }

    private static Agent stubAgent(String name, String agentId) {
        return new StubAgent(name, agentId);
    }

    private static class StubAgent implements Agent {
        private final String name;
        private final String agentId;

        StubAgent(String name, String agentId) {
            this.name = name;
            this.agentId = agentId;
        }

        @Override
        public String getAgentId() {
            return agentId;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public void interrupt() {}

        @Override
        public void interrupt(Msg msg) {}

        @Override
        public Mono<Msg> call(List<Msg> msgs) {
            return Mono.empty();
        }

        @Override
        public Mono<Msg> call(List<Msg> msgs, Class<?> structuredModel) {
            return Mono.empty();
        }

        @Override
        public Mono<Msg> call(List<Msg> msgs, JsonNode schema) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> observe(Msg msg) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> observe(List<Msg> msgs) {
            return Mono.empty();
        }

        @Override
        @SuppressWarnings("deprecation")
        public Flux<Event> stream(List<Msg> msgs, StreamOptions options) {
            return Flux.empty();
        }

        @Override
        @SuppressWarnings("deprecation")
        public Flux<Event> stream(List<Msg> msgs, StreamOptions options, Class<?> structuredModel) {
            return Flux.empty();
        }

        @Override
        @SuppressWarnings("deprecation")
        public Flux<Event> stream(List<Msg> msgs, StreamOptions options, JsonNode schema) {
            return Flux.empty();
        }
    }

    private static final class StubModel implements io.agentscope.core.model.Model {
        private final String name;

        StubModel(String name) {
            this.name = name;
        }

        @Override
        public String getModelName() {
            return name;
        }

        @Override
        public Flux<io.agentscope.core.model.ChatResponse> stream(
                List<Msg> messages,
                List<io.agentscope.core.model.ToolSchema> tools,
                io.agentscope.core.model.GenerateOptions options) {
            return Flux.empty();
        }
    }
}
