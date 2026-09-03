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
package io.agentscope.builder.web.managed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentscope.builder.web.api.DataSessionApiController;
import io.agentscope.builder.web.managed.service.HandsMetrics;
import io.agentscope.builder.web.managed.service.SessionEventLog;
import io.agentscope.builder.web.toolbus.ToolConfirmationCoordinator;
import io.agentscope.core.event.AgentEndEvent;
import io.agentscope.core.event.AgentResultEvent;
import io.agentscope.core.event.ModelCallEndEvent;
import io.agentscope.core.event.ModelCallStartEvent;
import io.agentscope.core.event.TextBlockDeltaEvent;
import io.agentscope.core.event.TextOutputDisposition;
import io.agentscope.core.event.TextOutputDispositionEvent;
import io.agentscope.core.event.ThinkingBlockDeltaEvent;
import io.agentscope.core.event.ToolCallDeltaEvent;
import io.agentscope.core.event.ToolCallEndEvent;
import io.agentscope.core.event.ToolCallStartEvent;
import io.agentscope.core.event.ToolResultEndEvent;
import io.agentscope.core.event.ToolResultTextDeltaEvent;
import io.agentscope.core.message.GenerateReason;
import io.agentscope.core.message.MessageMetadataKeys;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ToolResultState;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.security.core.Authentication;
import reactor.core.publisher.Flux;

class SessionEventMapperTest {

    private SessionEventMapper mapper;
    private SessionEventMapper.PreviewIds previewIds;

    @BeforeEach
    void setUp() {
        mapper = new SessionEventMapper(new ObjectMapper());
        previewIds = new SessionEventMapper.PreviewIds();
    }

    @Test
    void thinkingBlockDeltaMapsToAgentThinkingPreviewOnly() {
        SessionEventMapper.MappingResult result =
                mapper.map(
                        new ThinkingBlockDeltaEvent("reply-1", "block-1", "reasoning chunk"),
                        previewIds);

        assertThat(result.persisted()).isEmpty();
        assertThat(result.preview()).isPresent();
        SessionEventMapper.PreviewFrame frame = result.preview().get();
        assertThat(frame.streamType()).isEqualTo(SessionEventTypes.EVENT_DELTA);
        assertThat(frame.targetType()).isEqualTo(SessionEventTypes.AGENT_THINKING);
        assertThat(frame.delta()).isEqualTo("reasoning chunk");
        assertThat(frame.eventId()).startsWith("evt_");
    }

    @Test
    void agentResultIsBufferedUntilAgentEndAndReusesPreviewMessageEventId() {
        SessionEventMapper.MappingResult delta =
                mapper.map(new TextBlockDeltaEvent("r", "b", "Hel"), previewIds);
        String previewId = delta.preview().orElseThrow().eventId();

        Msg msg =
                Msg.builder()
                        .role(MsgRole.ASSISTANT)
                        .content(
                                TextBlock.builder().text("Hello").build(),
                                TextBlock.builder().text("world").build())
                        .metadata(Map.of("trace", "kept"))
                        .generateReason(GenerateReason.MODEL_STOP)
                        .build();
        SessionEventMapper.MappingResult buffered =
                mapper.map(new AgentResultEvent(msg), previewIds);

        assertThat(buffered.persisted()).isEmpty();
        assertThat(buffered.preview()).isEmpty();

        SessionEventMapper.MappingResult result = mapper.map(new AgentEndEvent("r"), previewIds);

        assertThat(result.preview()).isEmpty();
        assertThat(result.persisted()).isPresent();
        SessionEventMapper.PersistedEvent persisted = result.persisted().get();
        assertThat(persisted.type()).isEqualTo(SessionEventTypes.AGENT_MESSAGE);
        assertThat(persisted.payload().get("text")).isEqualTo("Hello\nworld");
        List<?> content = (List<?>) persisted.payload().get("content");
        assertThat(content).hasSize(2);
        assertThat(((Map<?, ?>) content.get(0)).get("type")).isEqualTo("text");
        assertThat(((Map<?, ?>) content.get(0)).get("text")).isEqualTo("Hello");
        assertThat(((Map<?, ?>) content.get(1)).get("text")).isEqualTo("world");
        assertThat(((Map<?, ?>) persisted.payload().get("metadata")).get("trace"))
                .isEqualTo("kept");
        assertThat(persisted.payload().get("generateReason")).isEqualTo("MODEL_STOP");
        assertThat(persisted.eventId()).isEqualTo(previewId);
    }

    /**
     * The harness emits ModelCallEnd while finishing the model request, and AgentResult only at the
     * very end of the turn. Closing the request must not discard the preview id, or the client
     * renders the typewriter preview and the persisted message as two separate bubbles.
     */
    @Test
    void agentResultReusesPreviewMessageEventIdAcrossModelCallEnd() {
        mapper.map(new ModelCallStartEvent("r"), previewIds);
        SessionEventMapper.MappingResult delta =
                mapper.map(new TextBlockDeltaEvent("r", "b", "Hel"), previewIds);
        String previewId = delta.preview().orElseThrow().eventId();
        mapper.map(new ModelCallEndEvent("r", null), previewIds);

        Msg msg = Msg.builder().role(MsgRole.ASSISTANT).textContent("Hello").build();
        assertThat(mapper.map(new AgentResultEvent(msg), previewIds).persisted()).isEmpty();
        SessionEventMapper.MappingResult result = mapper.map(new AgentEndEvent("r"), previewIds);

        assertThat(result.persisted().orElseThrow().eventId()).isEqualTo(previewId);
    }

    /** Each model request opens a fresh preview window; the result reconciles with the last one. */
    @Test
    void multiRoundTurnReusesLastRoundPreviewMessageEventId() {
        mapper.map(new ModelCallStartEvent("r"), previewIds);
        String firstRoundId =
                mapper.map(new TextBlockDeltaEvent("r", "b", "thinking"), previewIds)
                        .preview()
                        .orElseThrow()
                        .eventId();
        mapper.map(new ModelCallEndEvent("r", null), previewIds);

        mapper.map(new ModelCallStartEvent("r"), previewIds);
        String secondRoundId =
                mapper.map(new TextBlockDeltaEvent("r", "b", "answer"), previewIds)
                        .preview()
                        .orElseThrow()
                        .eventId();
        mapper.map(new ModelCallEndEvent("r", null), previewIds);

        assertThat(secondRoundId).isNotEqualTo(firstRoundId);

        Msg msg = Msg.builder().role(MsgRole.ASSISTANT).textContent("answer").build();
        assertThat(mapper.map(new AgentResultEvent(msg), previewIds).persisted()).isEmpty();
        SessionEventMapper.MappingResult result = mapper.map(new AgentEndEvent("r"), previewIds);

        assertThat(result.persisted().orElseThrow().eventId()).isEqualTo(secondRoundId);
    }

    @Test
    void intermediateDispositionUpdatesPreviewWithoutPersistingAndKeepsReplyId() {
        String previewId =
                mapper.map(new TextBlockDeltaEvent("r", "b-1", "working"), previewIds)
                        .preview()
                        .orElseThrow()
                        .eventId();

        SessionEventMapper.MappingResult disposition =
                mapper.map(
                        new TextOutputDispositionEvent(
                                "r", TextOutputDisposition.INTERMEDIATE, null),
                        previewIds);

        assertThat(disposition.persisted()).isEmpty();
        SessionEventMapper.PreviewFrame update = disposition.preview().orElseThrow();
        assertThat(update.streamType()).isEqualTo(SessionEventTypes.EVENT_UPDATE);
        assertThat(update.eventId()).isEqualTo(previewId);
        assertThat(update.attributes()).containsEntry("disposition", "INTERMEDIATE");

        mapper.map(new ToolCallStartEvent("r", "tool-1", "bash"), previewIds);
        String afterToolId =
                mapper.map(new TextBlockDeltaEvent("r", "b-2", "still working"), previewIds)
                        .preview()
                        .orElseThrow()
                        .eventId();
        assertThat(afterToolId).isEqualTo(previewId);
    }

    @Test
    void terminalDispositionOnlyClosesPreviewAndAgentEndCommitsResult() {
        String previewId =
                mapper.map(new TextBlockDeltaEvent("r", "b", "preview"), previewIds)
                        .preview()
                        .orElseThrow()
                        .eventId();
        Msg msg =
                Msg.builder()
                        .role(MsgRole.ASSISTANT)
                        .textContent("authoritative")
                        .generateReason(GenerateReason.MODEL_STOP)
                        .build();
        mapper.map(new AgentResultEvent(msg), previewIds);

        SessionEventMapper.MappingResult terminal =
                mapper.map(
                        new TextOutputDispositionEvent(
                                "r", TextOutputDisposition.TERMINAL, GenerateReason.MODEL_STOP),
                        previewIds);

        assertThat(terminal.persisted()).isEmpty();
        assertThat(terminal.preview().orElseThrow().eventId()).isEqualTo(previewId);
        assertThat(terminal.preview().orElseThrow().attributes())
                .containsEntry("disposition", "TERMINAL")
                .containsEntry("generateReason", "MODEL_STOP");

        SessionEventMapper.MappingResult end = mapper.map(new AgentEndEvent("r"), previewIds);
        assertThat(end.persisted().orElseThrow().payload().get("text")).isEqualTo("authoritative");
        assertThat(end.persisted().orElseThrow().eventId()).isEqualTo(previewId);
    }

    @Test
    void structuredOnlyResultGetsFreshPersistedEventIdAndKeepsStructuredOutput() {
        Map<String, Object> structured = Map.of("city", "Hangzhou", "temperature", 28);
        Msg msg =
                Msg.builder()
                        .role(MsgRole.ASSISTANT)
                        .metadata(Map.of(MessageMetadataKeys.STRUCTURED_OUTPUT, structured))
                        .generateReason(GenerateReason.STRUCTURED_OUTPUT)
                        .build();

        assertThat(mapper.map(new AgentResultEvent(msg), previewIds).persisted()).isEmpty();
        SessionEventMapper.PersistedEvent persisted =
                mapper.map(new AgentEndEvent("structured-reply"), previewIds)
                        .persisted()
                        .orElseThrow();

        assertThat(persisted.eventId()).startsWith("evt_");
        assertThat(persisted.payload().get("text")).isEqualTo("");
        assertThat(persisted.payload().get("content")).isEqualTo(List.of());
        assertThat(persisted.payload().get("structuredOutput")).isEqualTo(structured);
        assertThat(persisted.payload().get("generateReason")).isEqualTo("STRUCTURED_OUTPUT");
    }

    @Test
    void authoritativeEmptyResultSendsUpdateThatRemovesPreview() {
        String previewId =
                mapper.map(new TextBlockDeltaEvent("r", "b", "stale preview"), previewIds)
                        .preview()
                        .orElseThrow()
                        .eventId();
        Msg empty =
                Msg.builder()
                        .role(MsgRole.ASSISTANT)
                        .textContent("")
                        .generateReason(GenerateReason.MODEL_STOP)
                        .build();
        mapper.map(new AgentResultEvent(empty), previewIds);
        mapper.map(
                new TextOutputDispositionEvent(
                        "r", TextOutputDisposition.TERMINAL, GenerateReason.MODEL_STOP),
                previewIds);

        SessionEventMapper.MappingResult end = mapper.map(new AgentEndEvent("r"), previewIds);

        assertThat(end.persisted()).isEmpty();
        SessionEventMapper.PreviewFrame update = end.preview().orElseThrow();
        assertThat(update.streamType()).isEqualTo(SessionEventTypes.EVENT_UPDATE);
        assertThat(update.eventId()).isEqualTo(previewId);
        assertThat(update.attributes())
                .containsEntry("authoritative", true)
                .containsEntry("hasOutput", false);
    }

    @Test
    void suspendedAndPausedResultsDoNotCommitOrdinaryAgentMessage() {
        for (GenerateReason reason :
                List.of(
                        GenerateReason.PERMISSION_ASKING,
                        GenerateReason.TOOL_SUSPENDED,
                        GenerateReason.MIDDLEWARE_STOP_REQUESTED)) {
            SessionEventMapper.PreviewIds ids = new SessionEventMapper.PreviewIds();
            Msg msg =
                    Msg.builder()
                            .role(MsgRole.ASSISTANT)
                            .textContent("not a final answer")
                            .generateReason(reason)
                            .build();

            assertThat(mapper.map(new AgentResultEvent(msg), ids).persisted()).isEmpty();
            assertThat(mapper.map(new AgentEndEvent("r-" + reason), ids).persisted())
                    .as(reason.name())
                    .isEmpty();
        }
    }

    @Test
    void previewBusAndControllerPassUpdateAttributesThroughToSse() throws Exception {
        SessionEventPreviewBus previewBus = new SessionEventPreviewBus();
        DataSessionService sessionService = mock(DataSessionService.class);
        SessionEventLog eventLog = mock(SessionEventLog.class);
        Authentication authentication = mock(Authentication.class);
        when(authentication.getPrincipal()).thenReturn("user-1");
        when(eventLog.subscribe("session-1", 0L)).thenReturn(Flux.never());

        DataSessionApiController controller =
                new DataSessionApiController(
                        sessionService,
                        eventLog,
                        previewBus,
                        mock(ToolConfirmationCoordinator.class),
                        mock(SessionTurnRunner.class),
                        new ObjectMapper(),
                        mock(HandsMetrics.class));
        CompletableFuture<ServerSentEvent<String>> next =
                controller
                        .streamEvents(
                                "session-1",
                                null,
                                List.of(SessionEventTypes.AGENT_MESSAGE),
                                authentication)
                        .next()
                        .toFuture();

        previewBus.emitUpdate(
                "session-1",
                SessionEventTypes.AGENT_MESSAGE,
                "evt_preview",
                Map.of("disposition", "INTERMEDIATE", "custom", Map.of("nested", true)));

        ServerSentEvent<String> sse = next.get(5, TimeUnit.SECONDS);
        assertThat(sse.event()).isEqualTo(SessionEventTypes.EVENT_UPDATE);
        @SuppressWarnings("unchecked")
        Map<String, Object> json = new ObjectMapper().readValue(sse.data(), Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = (Map<String, Object>) json.get("payload");
        assertThat(payload)
                .containsEntry("event_id", "evt_preview")
                .containsEntry("type", SessionEventTypes.AGENT_MESSAGE)
                .containsEntry("disposition", "INTERMEDIATE");
        assertThat(((Map<?, ?>) payload.get("custom")).get("nested")).isEqualTo(true);
    }

    @Test
    void toolCallAccumulatesInputAndPersistsOnEnd() {
        assertThat(mapper.map(new ToolCallStartEvent("r", "tool-1", "bash"), previewIds).preview())
                .isPresent();

        SessionEventMapper.MappingResult d1 =
                mapper.map(new ToolCallDeltaEvent("r", "tool-1", "bash", "{\"cmd\":"), previewIds);
        assertThat(d1.persisted()).isEmpty();
        assertThat(d1.preview()).isPresent();

        mapper.map(new ToolCallDeltaEvent("r", "tool-1", "bash", "\"ls\"}"), previewIds);

        SessionEventMapper.MappingResult end =
                mapper.map(new ToolCallEndEvent("r", "tool-1", "bash"), previewIds);
        assertThat(end.persisted()).isPresent();
        SessionEventMapper.PersistedEvent persisted = end.persisted().get();
        assertThat(persisted.type()).isEqualTo(SessionEventTypes.AGENT_TOOL_USE);
        assertThat(persisted.eventId()).isEqualTo(d1.preview().get().eventId());
        @SuppressWarnings("unchecked")
        Map<String, Object> input = (Map<String, Object>) persisted.payload().get("input");
        assertThat(input.get("cmd")).isEqualTo("ls");
    }

    @Test
    void toolResultAccumulatesOutputAndPersistsOnEnd() {
        mapper.map(new ToolResultTextDeltaEvent("r", "tool-1", "bash", "file1\n"), previewIds);
        mapper.map(new ToolResultTextDeltaEvent("r", "tool-1", "bash", "file2\n"), previewIds);

        SessionEventMapper.MappingResult end =
                mapper.map(
                        new ToolResultEndEvent("r", "tool-1", "bash", ToolResultState.SUCCESS),
                        previewIds);

        assertThat(end.persisted()).isPresent();
        SessionEventMapper.PersistedEvent persisted = end.persisted().get();
        assertThat(persisted.type()).isEqualTo(SessionEventTypes.AGENT_TOOL_RESULT);
        assertThat(persisted.payload().get("output")).isEqualTo("file1\nfile2\n");
        assertThat(persisted.payload().get("text")).isEqualTo("file1\nfile2\n");
        assertThat(persisted.eventId()).startsWith("evt_");
    }
}
