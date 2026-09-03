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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentscope.core.event.AgentEndEvent;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.event.AgentResultEvent;
import io.agentscope.core.event.AgentStartEvent;
import io.agentscope.core.event.ModelCallEndEvent;
import io.agentscope.core.event.ModelCallStartEvent;
import io.agentscope.core.event.TextBlockDeltaEvent;
import io.agentscope.core.event.TextOutputDisposition;
import io.agentscope.core.event.TextOutputDispositionEvent;
import io.agentscope.core.event.ThinkingBlockDeltaEvent;
import io.agentscope.core.event.ToolCallDeltaEvent;
import io.agentscope.core.event.ToolCallEndEvent;
import io.agentscope.core.event.ToolCallStartEvent;
import io.agentscope.core.event.ToolResultDataDeltaEvent;
import io.agentscope.core.event.ToolResultEndEvent;
import io.agentscope.core.event.ToolResultTextDeltaEvent;
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.GenerateReason;
import io.agentscope.core.message.MessageMetadataKeys;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.TextBlock;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * Maps harness {@link AgentEvent}s onto persisted session event types / payloads and stream-only
 * preview frames.
 *
 * <p>Streaming token/arg deltas are never persisted as rows. Tool call input and tool result bodies
 * are accumulated across delta events and persisted on End. Preview {@code event_id} values are
 * stable {@code evt_*} ids reused when the matching buffered event is appended, so clients can
 * reconcile typewriter previews with the authoritative record.
 */
@Component
public class SessionEventMapper {

    private static final int MAX_TOOL_PAYLOAD_CHARS = 64 * 1024;
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final ObjectMapper objectMapper;

    public SessionEventMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /** Outcome of mapping one harness event. */
    public record MappingResult(
            Optional<PersistedEvent> persisted, Optional<PreviewFrame> preview) {

        public static MappingResult empty() {
            return new MappingResult(Optional.empty(), Optional.empty());
        }

        public static MappingResult persist(PersistedEvent event) {
            return new MappingResult(Optional.of(event), Optional.empty());
        }

        public static MappingResult persist(String type, Map<String, Object> payload) {
            return persist(new PersistedEvent(type, payload, null));
        }

        public static MappingResult persist(
                String type, Map<String, Object> payload, String eventId) {
            return persist(new PersistedEvent(type, payload, eventId));
        }

        public static MappingResult previewOnly(PreviewFrame frame) {
            return new MappingResult(Optional.empty(), Optional.of(frame));
        }

        public static MappingResult both(PersistedEvent persisted, PreviewFrame preview) {
            return new MappingResult(Optional.of(persisted), Optional.of(preview));
        }
    }

    /**
     * Persisted event. When {@code eventId} is non-null, {@link
     * io.agentscope.builder.web.managed.service.SessionEventLog} must reuse it so previews reconcile.
     */
    public record PersistedEvent(String type, Map<String, Object> payload, String eventId) {
        public PersistedEvent(String type, Map<String, Object> payload) {
            this(type, payload, null);
        }
    }

    /**
     * Stream-only preview frame ({@code event_start}, {@code event_delta} or {@code event_update}).
     *
     * <p>When {@code delta} is null, callers should emit start only (no delta frame).
     */
    public record PreviewFrame(
            String streamType,
            String targetType,
            String eventId,
            String delta,
            Map<String, Object> attributes) {
        public PreviewFrame(String streamType, String targetType, String eventId, String delta) {
            this(streamType, targetType, eventId, delta, Map.of());
        }

        public PreviewFrame {
            attributes = attributes != null ? attributes : Map.of();
        }
    }

    /**
     * Maps a harness event. Text/thinking/tool deltas produce preview frames only; complete
     * messages and tool End boundaries produce persisted events with full payloads.
     */
    public MappingResult map(AgentEvent event, PreviewIds previewIds) {
        if (event instanceof TextBlockDeltaEvent delta) {
            if (delta.getDelta() == null || delta.getDelta().isEmpty()) {
                return MappingResult.empty();
            }
            String eventId = previewIds.messageEventId(delta);
            return MappingResult.previewOnly(
                    new PreviewFrame(
                            SessionEventTypes.EVENT_DELTA,
                            SessionEventTypes.AGENT_MESSAGE,
                            eventId,
                            delta.getDelta()));
        }
        if (event instanceof ThinkingBlockDeltaEvent thinking) {
            if (thinking.getDelta() == null || thinking.getDelta().isEmpty()) {
                return MappingResult.empty();
            }
            String eventId = previewIds.thinkingEventId(thinking);
            return MappingResult.previewOnly(
                    new PreviewFrame(
                            SessionEventTypes.EVENT_DELTA,
                            SessionEventTypes.AGENT_THINKING,
                            eventId,
                            thinking.getDelta()));
        }
        if (event instanceof TextOutputDispositionEvent disposition) {
            String eventId = previewIds.messageEventIdIfPresent(disposition);
            if (eventId == null) {
                return MappingResult.empty();
            }
            if (disposition.getDisposition() == TextOutputDisposition.INTERMEDIATE) {
                previewIds.markIntermediate(disposition);
            }
            Map<String, Object> attributes = new LinkedHashMap<>();
            attributes.put("replyId", disposition.getReplyId());
            attributes.put("disposition", disposition.getDisposition().name());
            if (disposition.getGenerateReason() != null) {
                attributes.put("generateReason", disposition.getGenerateReason().name());
            }
            return MappingResult.previewOnly(
                    new PreviewFrame(
                            SessionEventTypes.EVENT_UPDATE,
                            SessionEventTypes.AGENT_MESSAGE,
                            eventId,
                            null,
                            attributes));
        }
        if (event instanceof AgentResultEvent result) {
            if (previewIds.isTopLevel(result)) {
                previewIds.rememberTopLevelResult(result);
            }
            return MappingResult.empty();
        }
        if (event instanceof ToolCallStartEvent toolUse) {
            previewIds.beginToolUse(toolUse);
            String eventId = previewIds.toolUseEventId(toolUse);
            // Start announces the upcoming tool_use; args arrive via deltas and persist on End.
            return MappingResult.previewOnly(
                    new PreviewFrame(
                            SessionEventTypes.EVENT_START,
                            SessionEventTypes.AGENT_TOOL_USE,
                            eventId,
                            null));
        }
        if (event instanceof ToolCallDeltaEvent toolDelta) {
            if (toolDelta.getDelta() == null || toolDelta.getDelta().isEmpty()) {
                return MappingResult.empty();
            }
            previewIds.appendToolInput(toolDelta);
            String eventId = previewIds.toolUseEventId(toolDelta);
            return MappingResult.previewOnly(
                    new PreviewFrame(
                            SessionEventTypes.EVENT_DELTA,
                            SessionEventTypes.AGENT_TOOL_USE,
                            eventId,
                            toolDelta.getDelta()));
        }
        if (event instanceof ToolCallEndEvent toolEnd) {
            ToolBuffers.ToolUseBuffer buf = previewIds.finishToolUse(toolEnd);
            Map<String, Object> input = parseToolInput(buf.inputJson());
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("id", toolEnd.getToolCallId());
            payload.put("name", toolEnd.getToolCallName());
            payload.put("input", input);
            payload.put("toolCallId", toolEnd.getToolCallId());
            payload.put("toolName", toolEnd.getToolCallName());
            if (buf.truncated()) {
                payload.put("truncated", true);
                payload.put("originalSize", buf.originalInputSize());
            }
            return MappingResult.persist(SessionEventTypes.AGENT_TOOL_USE, payload, buf.eventId());
        }
        if (event instanceof ToolResultTextDeltaEvent textDelta) {
            if (textDelta.getDelta() == null || textDelta.getDelta().isEmpty()) {
                return MappingResult.empty();
            }
            previewIds.appendToolResultText(textDelta);
            return MappingResult.empty();
        }
        if (event instanceof ToolResultDataDeltaEvent dataDelta) {
            String fragment = stringifyContentBlock(dataDelta.getData());
            if (fragment == null || fragment.isEmpty()) {
                return MappingResult.empty();
            }
            previewIds.appendToolResultText(dataDelta, fragment);
            return MappingResult.empty();
        }
        if (event instanceof ToolResultEndEvent toolResult) {
            ToolBuffers.ToolResultBuffer buf = previewIds.finishToolResult(toolResult);
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("tool_use_id", toolResult.getToolCallId());
            payload.put("id", toolResult.getToolCallId());
            payload.put("name", toolResult.getToolCallName());
            payload.put("toolCallId", toolResult.getToolCallId());
            payload.put("toolName", toolResult.getToolCallName());
            if (toolResult.getState() != null) {
                payload.put("state", toolResult.getState().name());
            }
            String output = buf.outputText();
            payload.put("output", output);
            payload.put("text", output);
            payload.put("content", List.of(Map.of("type", "text", "text", output)));
            if (buf.truncated()) {
                payload.put("truncated", true);
                payload.put("originalSize", buf.originalOutputSize());
            }
            return MappingResult.persist(
                    SessionEventTypes.AGENT_TOOL_RESULT, payload, buf.eventId());
        }
        if (event instanceof ModelCallStartEvent modelStart) {
            // Opening a model request opens a fresh preview window. The previous window must stay
            // readable until then: AgentResultEvent arrives only at the end of the turn and needs
            // the last window's id to reconcile with the streamed preview.
            previewIds.beginModelCall(modelStart);
            previewIds.resetThinking(modelStart);
            return MappingResult.persist(SessionEventTypes.SPAN_MODEL_REQUEST_START, Map.of());
        }
        if (event instanceof ModelCallEndEvent modelEnd) {
            previewIds.finishModelCall(modelEnd);
            Map<String, Object> payload = new LinkedHashMap<>();
            if (modelEnd.getUsage() != null) {
                payload.put("usage", modelEnd.getUsage());
            }
            return MappingResult.persist(SessionEventTypes.SPAN_MODEL_REQUEST_END, payload);
        }
        if (event instanceof AgentEndEvent end) {
            if (!previewIds.isTopLevel(end)) {
                return MappingResult.empty();
            }
            return commitTopLevelResult(end, previewIds);
        }
        if (event instanceof AgentStartEvent) {
            return MappingResult.empty();
        }
        return MappingResult.empty();
    }

    private MappingResult commitTopLevelResult(AgentEndEvent end, PreviewIds previewIds) {
        AgentResultEvent resultEvent = previewIds.takeTopLevelResult(end);
        String previewId = previewIds.consumeMessageEventId(end);
        if (resultEvent == null) {
            return MappingResult.empty();
        }

        Msg result = resultEvent.getResult();
        if (result == null) {
            if (previewId == null) {
                return MappingResult.empty();
            }
            Map<String, Object> attributes = new LinkedHashMap<>();
            attributes.put("authoritative", true);
            attributes.put("hasOutput", false);
            return MappingResult.previewOnly(
                    new PreviewFrame(
                            SessionEventTypes.EVENT_UPDATE,
                            SessionEventTypes.AGENT_MESSAGE,
                            previewId,
                            null,
                            attributes));
        }

        GenerateReason reason = result.getGenerateReason();
        if (!commitsOrdinaryMessage(reason)) {
            return MappingResult.empty();
        }

        if (!hasOutput(result)) {
            if (previewId == null) {
                return MappingResult.empty();
            }
            Map<String, Object> attributes = new LinkedHashMap<>();
            attributes.put("authoritative", true);
            attributes.put("hasOutput", false);
            attributes.put("generateReason", reason.name());
            return MappingResult.previewOnly(
                    new PreviewFrame(
                            SessionEventTypes.EVENT_UPDATE,
                            SessionEventTypes.AGENT_MESSAGE,
                            previewId,
                            null,
                            attributes));
        }

        String eventId = previewId != null ? previewId : PreviewIds.newEventId();
        return MappingResult.persist(
                SessionEventTypes.AGENT_MESSAGE, messagePayload(result, reason), eventId);
    }

    private Map<String, Object> messagePayload(Msg message, GenerateReason reason) {
        Map<String, Object> messageDto = objectMapper.convertValue(message, MAP_TYPE);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("text", message.getTextContent());
        payload.put("content", messageDto.getOrDefault("content", List.of()));
        Map<String, Object> metadata =
                message.getMetadata() != null
                        ? new LinkedHashMap<>(message.getMetadata())
                        : Map.of();
        payload.put("metadata", metadata);
        payload.put("generateReason", reason.name());
        if (metadata.containsKey(MessageMetadataKeys.STRUCTURED_OUTPUT)) {
            payload.put("structuredOutput", metadata.get(MessageMetadataKeys.STRUCTURED_OUTPUT));
        }
        return payload;
    }

    private static boolean hasOutput(Msg message) {
        if (message.hasStructuredData()) {
            return true;
        }
        for (ContentBlock block : message.getContent()) {
            if (block instanceof TextBlock text) {
                if (!text.getText().isEmpty()) {
                    return true;
                }
            } else {
                return true;
            }
        }
        return false;
    }

    private static boolean commitsOrdinaryMessage(GenerateReason reason) {
        return reason == GenerateReason.MODEL_STOP
                || reason == GenerateReason.STRUCTURED_OUTPUT
                || reason == GenerateReason.MAX_ITERATIONS;
    }

    private Map<String, Object> parseToolInput(String raw) {
        if (raw == null || raw.isBlank()) {
            return Map.of();
        }
        try {
            Map<String, Object> parsed = objectMapper.readValue(raw, MAP_TYPE);
            return parsed != null ? parsed : Map.of("_raw", raw);
        } catch (Exception ex) {
            return Map.of("_raw", raw);
        }
    }

    private static String stringifyContentBlock(ContentBlock block) {
        if (block == null) {
            return null;
        }
        if (block instanceof TextBlock text) {
            return text.getText();
        }
        return String.valueOf(block);
    }

    /** Allocates stable preview / persist event ids for a turn and accumulates tool buffers. */
    public static final class PreviewIds {
        private record InvocationKey(String source, String taskId) {}

        private record PreviewKey(InvocationKey invocation, String replyId) {}

        private record ToolKey(InvocationKey invocation, String toolCallId) {}

        private final Map<PreviewKey, String> messageIdsByReply = new LinkedHashMap<>();
        private final Set<PreviewKey> intermediateReplies = new HashSet<>();
        private final Map<InvocationKey, String> thinkingIds = new LinkedHashMap<>();
        private final Map<InvocationKey, AgentResultEvent> topLevelResults = new LinkedHashMap<>();
        private final Map<ToolKey, ToolBuffers.ToolUseBuffer> toolUses = new LinkedHashMap<>();
        private final Map<ToolKey, ToolBuffers.ToolResultBuffer> toolResults =
                new LinkedHashMap<>();

        public String messageEventId(TextBlockDeltaEvent event) {
            PreviewKey previewKey = previewKey(event, event.getReplyId());
            return messageIdsByReply.computeIfAbsent(previewKey, ignored -> newEventId());
        }

        public String messageEventIdIfPresent(TextOutputDispositionEvent event) {
            return messageIdsByReply.get(previewKey(event, event.getReplyId()));
        }

        /** Returns and clears the preview id for this invocation and reply. */
        public String consumeMessageEventId(AgentEndEvent event) {
            PreviewKey previewKey = previewKey(event, event.getReplyId());
            intermediateReplies.remove(previewKey);
            return messageIdsByReply.remove(previewKey);
        }

        public void markIntermediate(TextOutputDispositionEvent event) {
            intermediateReplies.add(previewKey(event, event.getReplyId()));
        }

        public void beginModelCall(ModelCallStartEvent event) {
            InvocationKey invocationKey = invocationKey(event);
            for (PreviewKey completed : List.copyOf(intermediateReplies)) {
                if (completed.invocation().equals(invocationKey)) {
                    messageIdsByReply.remove(completed);
                    intermediateReplies.remove(completed);
                }
            }
            messageIdsByReply.remove(previewKey(event, event.getReplyId()));
        }

        public void finishModelCall(ModelCallEndEvent event) {
            PreviewKey previewKey = previewKey(event, event.getReplyId());
            if (intermediateReplies.remove(previewKey)) {
                messageIdsByReply.remove(previewKey);
            }
        }

        public boolean isTopLevel(AgentEvent event) {
            return invocationKey(event).equals(new InvocationKey("", ""));
        }

        public void rememberTopLevelResult(AgentResultEvent result) {
            topLevelResults.put(invocationKey(result), result);
        }

        public AgentResultEvent takeTopLevelResult(AgentEndEvent end) {
            return topLevelResults.remove(invocationKey(end));
        }

        public String thinkingEventId(ThinkingBlockDeltaEvent event) {
            return thinkingIds.computeIfAbsent(invocationKey(event), ignored -> newEventId());
        }

        public String toolUseEventId(ToolCallStartEvent event) {
            return beginToolUse(event).eventId();
        }

        public String toolUseEventId(ToolCallDeltaEvent event) {
            return beginToolUse(event, event.getToolCallId(), event.getToolCallName()).eventId();
        }

        public ToolBuffers.ToolUseBuffer beginToolUse(ToolCallStartEvent event) {
            return beginToolUse(event, event.getToolCallId(), event.getToolCallName());
        }

        private ToolBuffers.ToolUseBuffer beginToolUse(
                AgentEvent event, String toolCallId, String toolName) {
            ToolKey toolKey = toolKey(event, toolCallId);
            return toolUses.computeIfAbsent(
                    toolKey, ignored -> new ToolBuffers.ToolUseBuffer(newEventId(), toolName));
        }

        public void appendToolInput(ToolCallDeltaEvent event) {
            ToolBuffers.ToolUseBuffer buf =
                    beginToolUse(event, event.getToolCallId(), event.getToolCallName());
            if (event.getToolCallName() != null) {
                buf.setToolName(event.getToolCallName());
            }
            buf.appendInput(event.getDelta(), MAX_TOOL_PAYLOAD_CHARS);
        }

        public ToolBuffers.ToolUseBuffer finishToolUse(ToolCallEndEvent event) {
            ToolKey toolKey = toolKey(event, event.getToolCallId());
            ToolBuffers.ToolUseBuffer buf =
                    beginToolUse(event, event.getToolCallId(), event.getToolCallName());
            if (event.getToolCallName() != null) {
                buf.setToolName(event.getToolCallName());
            }
            toolUses.remove(toolKey);
            return buf;
        }

        public void appendToolResultText(ToolResultTextDeltaEvent event) {
            appendToolResultText(
                    event, event.getToolCallId(), event.getToolCallName(), event.getDelta());
        }

        public void appendToolResultText(ToolResultDataDeltaEvent event, String delta) {
            appendToolResultText(event, event.getToolCallId(), event.getToolCallName(), delta);
        }

        private void appendToolResultText(
                AgentEvent event, String toolCallId, String toolName, String delta) {
            ToolBuffers.ToolResultBuffer buf =
                    toolResults.computeIfAbsent(
                            toolKey(event, toolCallId),
                            ignored -> new ToolBuffers.ToolResultBuffer(newEventId(), toolName));
            if (toolName != null) {
                buf.setToolName(toolName);
            }
            buf.appendOutput(delta, MAX_TOOL_PAYLOAD_CHARS);
        }

        public ToolBuffers.ToolResultBuffer finishToolResult(ToolResultEndEvent event) {
            ToolKey toolKey = toolKey(event, event.getToolCallId());
            ToolBuffers.ToolResultBuffer buf =
                    toolResults.computeIfAbsent(
                            toolKey,
                            ignored ->
                                    new ToolBuffers.ToolResultBuffer(
                                            newEventId(), event.getToolCallName()));
            if (event.getToolCallName() != null) {
                buf.setToolName(event.getToolCallName());
            }
            toolResults.remove(toolKey);
            return buf;
        }

        public void resetThinking(ModelCallStartEvent event) {
            thinkingIds.remove(invocationKey(event));
        }

        private static PreviewKey previewKey(AgentEvent event, String replyId) {
            return new PreviewKey(invocationKey(event), key(replyId));
        }

        private static ToolKey toolKey(AgentEvent event, String toolCallId) {
            return new ToolKey(invocationKey(event), key(toolCallId));
        }

        private static InvocationKey invocationKey(AgentEvent event) {
            Object taskId =
                    event.getMetadata() == null
                            ? null
                            : event.getMetadata().get(AgentEvent.METADATA_TASK_ID);
            return new InvocationKey(
                    normalize(event.getSource()),
                    normalize(taskId == null ? null : taskId.toString()));
        }

        private static String normalize(String value) {
            return value == null ? "" : value;
        }

        private static String key(String value) {
            return value == null || value.isBlank() ? "_" : value;
        }

        private static String newEventId() {
            return "evt_" + UUID.randomUUID().toString().replace("-", "");
        }
    }

    /** Mutable accumulation buffers for tool input / output within a turn. */
    static final class ToolBuffers {
        private ToolBuffers() {}

        static final class ToolUseBuffer {
            private final String eventId;
            private final StringBuilder input = new StringBuilder();
            private String toolName;
            private boolean truncated;
            private int originalInputSize;

            ToolUseBuffer(String eventId, String toolName) {
                this.eventId = eventId;
                this.toolName = toolName;
            }

            String eventId() {
                return eventId;
            }

            String inputJson() {
                return input.toString();
            }

            boolean truncated() {
                return truncated;
            }

            int originalInputSize() {
                return originalInputSize;
            }

            void setToolName(String toolName) {
                this.toolName = toolName;
            }

            void appendInput(String delta, int maxChars) {
                originalInputSize += delta.length();
                if (input.length() >= maxChars) {
                    truncated = true;
                    return;
                }
                int remaining = maxChars - input.length();
                if (delta.length() > remaining) {
                    input.append(delta, 0, remaining);
                    truncated = true;
                } else {
                    input.append(delta);
                }
            }
        }

        static final class ToolResultBuffer {
            private final String eventId;
            private final StringBuilder output = new StringBuilder();
            private String toolName;
            private boolean truncated;
            private int originalOutputSize;

            ToolResultBuffer(String eventId, String toolName) {
                this.eventId = eventId;
                this.toolName = toolName;
            }

            String eventId() {
                return eventId;
            }

            String outputText() {
                return output.toString();
            }

            boolean truncated() {
                return truncated;
            }

            int originalOutputSize() {
                return originalOutputSize;
            }

            void setToolName(String toolName) {
                this.toolName = toolName;
            }

            void appendOutput(String delta, int maxChars) {
                originalOutputSize += delta.length();
                if (output.length() >= maxChars) {
                    truncated = true;
                    return;
                }
                int remaining = maxChars - output.length();
                if (delta.length() > remaining) {
                    output.append(delta, 0, remaining);
                    truncated = true;
                } else {
                    output.append(delta);
                }
            }
        }
    }
}
