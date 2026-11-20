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
package io.agentscope.harness.agent.memory.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Base64Source;
import io.agentscope.core.message.ImageBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ToolResultBlock;
import io.agentscope.core.message.ToolResultState;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.Model;
import io.agentscope.core.model.ToolSchema;
import io.agentscope.harness.agent.attachment.AttachmentReference;
import io.agentscope.harness.agent.memory.MemoryFlushManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Verifies media and attachment references are never removed from history by summarization. */
class ConversationCompactorMediaRetentionTest {

    private static final String PAD = " earlier conversation detail".repeat(40);

    /** Top-level media moves the cutoff before its message; raw bytes never reach the prompt. */
    @Test
    void keepsTopLevelMediaOutOfSummaryPrefix() {
        Fixture fixture = new Fixture();
        String imageData =
                "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=";
        Msg media = imageMessage("look at this", imageData);

        List<Msg> compacted =
                fixture.compact(List.of(padded("M1"), media, message("TAIL_1"), message("TAIL_2")));

        String prompt = fixture.model.prompt(0);
        assertTrue(prompt.contains("M1"));
        assertFalse(prompt.contains("look at this"));
        assertFalse(prompt.contains(imageData));
        assertEquals(4, compacted.size());
        assertSame(media, compacted.get(1));
        assertTrue(compacted.get(1).getContent().get(1) instanceof ImageBlock);
    }

    /** Media nested in a tool result pulls its whole assistant/tool group into the kept tail. */
    @Test
    void keepsNestedToolMediaGroupOutOfSummaryPrefix() {
        Fixture fixture = new Fixture();
        Msg assistant =
                Msg.builder()
                        .role(MsgRole.ASSISTANT)
                        .content(ToolUseBlock.builder().id("call:img").name("screenshot").build())
                        .build();
        Msg tool =
                Msg.builder()
                        .role(MsgRole.TOOL)
                        .content(
                                ToolResultBlock.builder()
                                        .id("call:img")
                                        .name("screenshot")
                                        .state(ToolResultState.SUCCESS)
                                        .output(
                                                List.of(
                                                        TextBlock.builder()
                                                                .text("captured")
                                                                .build(),
                                                        ImageBlock.builder()
                                                                .source(
                                                                        Base64Source.builder()
                                                                                .mediaType(
                                                                                        "image/png")
                                                                                .data(
                                                                                        "iVBORw0KGgo=")
                                                                                .build())
                                                                .build()))
                                        .build())
                        .build();

        List<Msg> compacted =
                fixture.compact(
                        List.of(
                                padded("M1"),
                                assistant,
                                tool,
                                message("TAIL_1"),
                                message("TAIL_2")));

        String prompt = fixture.model.prompt(0);
        assertTrue(prompt.contains("M1"));
        assertFalse(prompt.contains("captured"));
        assertEquals(5, compacted.size());
        assertSame(assistant, compacted.get(1));
        assertSame(tool, compacted.get(2));
    }

    /** A message carrying attachment references keeps its machine-readable metadata. */
    @Test
    void keepsAttachmentReferenceMessageOutOfSummaryPrefix() {
        Fixture fixture = new Fixture();
        Msg attach = attachmentMessage("see attached", "ref-1", "ref-2");

        List<Msg> compacted =
                fixture.compact(
                        List.of(padded("M1"), attach, message("TAIL_1"), message("TAIL_2")));

        String prompt = fixture.model.prompt(0);
        assertTrue(prompt.contains("M1"));
        assertFalse(prompt.contains("see attached"));
        assertSame(attach, compacted.get(1));
        assertEquals(
                2, AttachmentReference.listFromMetadata(compacted.get(1).getMetadata()).size());
    }

    /** An empty keep window cannot summarize the current input away when it has attachments. */
    @Test
    void protectsCurrentInputAttachmentsWhenKeepWindowIsEmpty() {
        Fixture fixture = new Fixture();
        Msg attach = attachmentMessage("current input", "ref-1", "ref-2");
        CompactionConfig zeroKeep =
                CompactionConfig.builder()
                        .triggerMessages(3)
                        .triggerTokens(0)
                        .keepMessages(0)
                        .keepTokens(0)
                        .summaryPrompt("SUMMARY_INPUT:\n{messages}")
                        .flushBeforeCompact(false)
                        .truncateArgs(null)
                        .prune(null)
                        .build();

        Optional<List<Msg>> result =
                fixture.compactor
                        .compactIfNeeded(
                                fixture.runtimeContext,
                                List.of(padded("M1"), message("M2"), attach),
                                zeroKeep,
                                "agent-id",
                                "session-id")
                        .block();

        List<Msg> compacted = result.orElseThrow();
        assertEquals(2, compacted.size());
        assertSame(attach, compacted.get(1));
        assertFalse(fixture.model.prompt(0).contains("current input"));
    }

    private static Msg message(String text) {
        return Msg.builder()
                .role(MsgRole.USER)
                .content(TextBlock.builder().text(text).build())
                .build();
    }

    private static Msg padded(String label) {
        return message(label + PAD);
    }

    private static Msg imageMessage(String text, String data) {
        return Msg.builder()
                .role(MsgRole.USER)
                .content(
                        List.of(
                                TextBlock.builder().text(text).build(),
                                ImageBlock.builder()
                                        .source(
                                                Base64Source.builder()
                                                        .mediaType("image/png")
                                                        .data(data)
                                                        .build())
                                        .build()))
                .build();
    }

    private static Msg attachmentMessage(String text, String... referenceIds) {
        List<Map<String, Object>> references = new ArrayList<>();
        for (int i = 0; i < referenceIds.length; i++) {
            references.add(
                    new AttachmentReference(
                                    referenceIds[i],
                                    "image/png",
                                    "f" + i + ".png",
                                    "sha-" + i,
                                    10,
                                    i)
                            .toMetadata());
        }
        return message(text)
                .withMetadata(Map.of(AttachmentReference.METADATA_KEY, List.copyOf(references)));
    }

    private static CompactionConfig config() {
        return CompactionConfig.builder()
                .triggerMessages(3)
                .triggerTokens(0)
                .keepMessages(2)
                .keepTokens(0)
                .summaryPrompt("SUMMARY_INPUT:\n{messages}")
                .flushBeforeCompact(false)
                .truncateArgs(null)
                .prune(null)
                .build();
    }

    private static final class Fixture {
        private final RecordingModel model = new RecordingModel();
        private final MemoryFlushManager flushManager = mock(MemoryFlushManager.class);
        private final RuntimeContext runtimeContext = mock(RuntimeContext.class);
        private final ConversationCompactor compactor;
        private final CompactionConfig config = config();

        private Fixture() {
            when(flushManager.flushMemories(any(RuntimeContext.class), anyList()))
                    .thenReturn(Mono.empty());
            compactor = new ConversationCompactor(model, flushManager);
        }

        private List<Msg> compact(List<Msg> messages) {
            Optional<List<Msg>> result =
                    compactor
                            .compactIfNeeded(
                                    runtimeContext, messages, config, "agent-id", "session-id")
                            .block();
            assertTrue(result.isPresent());
            return result.orElseThrow();
        }
    }

    private static final class RecordingModel implements Model {
        private final List<List<Msg>> inputs = new ArrayList<>();

        @Override
        public Flux<ChatResponse> stream(
                List<Msg> messages, List<ToolSchema> tools, GenerateOptions options) {
            inputs.add(List.copyOf(messages));
            return Flux.just(
                    ChatResponse.builder()
                            .id("summary-" + inputs.size())
                            .content(
                                    List.of(
                                            TextBlock.builder()
                                                    .text("ROUND_" + inputs.size())
                                                    .build()))
                            .build());
        }

        @Override
        public String getModelName() {
            return "recording-model";
        }

        private String prompt(int index) {
            return inputs.get(index).stream()
                    .map(
                            msg ->
                                    msg.getContent() == null
                                            ? ""
                                            : msg.getContent().stream()
                                                    .filter(TextBlock.class::isInstance)
                                                    .map(b -> ((TextBlock) b).getText())
                                                    .reduce("", String::concat))
                    .reduce("", String::concat);
        }
    }
}
