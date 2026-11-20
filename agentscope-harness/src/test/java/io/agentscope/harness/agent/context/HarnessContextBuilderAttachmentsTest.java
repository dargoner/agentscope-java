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
package io.agentscope.harness.agent.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.agentscope.core.agent.Agent;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.DataBlock;
import io.agentscope.core.message.ImageBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.middleware.ModelCallInput;
import io.agentscope.core.middleware.ModelRequestPreparer;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.Model;
import io.agentscope.core.state.AgentState;
import io.agentscope.harness.agent.attachment.AttachmentReference;
import io.agentscope.harness.agent.attachment.AttachmentRequestCompiler;
import io.agentscope.harness.agent.attachment.AttachmentResolver;
import io.agentscope.harness.agent.attachment.ResolvedAttachment;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/**
 * Preparer-path contract for attachment compilation: the request-scoped view gets a structured
 * marker when a reference cannot be resolved, while the canonical history keeps its reference
 * entries untouched — including when the resolution is cancelled mid-flight.
 */
class HarnessContextBuilderAttachmentsTest {

    @Test
    void unavailableResolutionMarksTheRequestAndKeepsCanonicalReferences() {
        AttachmentResolver resolver =
                (reference, context) ->
                        Mono.just(
                                new ResolvedAttachment.Unavailable(
                                        ResolvedAttachment.Unavailable.Category.NOT_FOUND,
                                        "missing"));
        var state = AgentState.builder().build();
        Msg canonical = attachmentMessage();
        state.contextMutable().add(canonical);
        var rc = RuntimeContext.builder().agentState(state).userId("u").sessionId("s").build();

        ModelCallInput prepared =
                builder(resolver)
                        .prepare(
                                mock(Agent.class),
                                rc,
                                request(canonical),
                                "c",
                                ModelRequestPreparer.Purpose.REASONING)
                        .block();

        Msg preparedMessage = userMessage(prepared);
        assertEquals(List.of("[attachment unavailable: NOT_FOUND]"), texts(preparedMessage));
        Map<String, Object> preparedMetadata = preparedMessage.getMetadata();
        assertTrue(
                preparedMetadata == null
                        || !preparedMetadata.containsKey(AttachmentReference.METADATA_KEY),
                "the request-scoped marker must replace the attachment metadata");

        assertEquals(List.of("see attached"), texts(canonical));
        assertNotNull(canonical.getMetadata());
        assertTrue(canonical.getMetadata().containsKey(AttachmentReference.METADATA_KEY));
        assertEquals(List.of(canonical), state.contextMutable());
    }

    @Test
    void cancelledResolutionDeliversNoFallbackAndKeepsCanonicalReferences() throws Exception {
        var subscribed = new CountDownLatch(1);
        var cancelled = new CountDownLatch(1);
        AttachmentResolver resolver =
                (reference, context) -> {
                    subscribed.countDown();
                    return Mono.<ResolvedAttachment>never().doOnCancel(cancelled::countDown);
                };
        var state = AgentState.builder().build();
        Msg canonical = attachmentMessage();
        state.contextMutable().add(canonical);
        var rc = RuntimeContext.builder().agentState(state).userId("u").sessionId("s").build();

        List<ModelCallInput> delivered = new CopyOnWriteArrayList<>();
        Disposable subscription =
                builder(resolver)
                        .prepare(
                                mock(Agent.class),
                                rc,
                                request(canonical),
                                "c",
                                ModelRequestPreparer.Purpose.REASONING)
                        .subscribe(delivered::add);
        try {
            assertTrue(subscribed.await(5, TimeUnit.SECONDS), "the resolver must be subscribed");
            subscription.dispose();
            assertTrue(cancelled.await(5, TimeUnit.SECONDS), "the resolution must be cancelled");
            assertTrue(delivered.isEmpty(), "a cancelled resolution must deliver nothing");

            assertEquals(List.of("see attached"), texts(canonical));
            assertNotNull(canonical.getMetadata());
            assertTrue(canonical.getMetadata().containsKey(AttachmentReference.METADATA_KEY));
            assertEquals(List.of(canonical), state.contextMutable());
        } finally {
            subscription.dispose();
        }
    }

    @Test
    void attachmentExpansionChargesEachMediaOnceAndDropsOnlyPlaceholderText() {
        AttachmentResolver resolver =
                (reference, context) ->
                        "att_1".equals(reference.referenceId())
                                ? Mono.just(new ResolvedAttachment.Inline("image/png", "aW1n"))
                                : Mono.just(
                                        new ResolvedAttachment.Inline("application/pdf", "cGRm"));
        var state = AgentState.builder().build();
        Msg canonical = twoAttachmentMessage();
        state.contextMutable().add(canonical);
        var rc = RuntimeContext.builder().agentState(state).userId("u").sessionId("s").build();

        ModelCallInput prepared =
                builder(resolver)
                        .prepare(
                                mock(Agent.class),
                                rc,
                                request(canonical, 200_000),
                                "c",
                                ModelRequestPreparer.Purpose.REASONING)
                        .block();

        Msg preparedMessage = userMessage(prepared);
        List<ContentBlock> blocks = preparedMessage.getContent();
        assertTrue(blocks.get(0) instanceof ImageBlock);
        assertTrue(blocks.get(1) instanceof DataBlock);
        DataBlock data = (DataBlock) blocks.get(1);
        assertEquals("att_2", data.getId());
        assertEquals("b.pdf", data.getName());
        Map<String, Object> preparedMetadata = preparedMessage.getMetadata();
        assertTrue(
                preparedMetadata == null
                        || !preparedMetadata.containsKey(AttachmentReference.METADATA_KEY),
                "expanded media must not keep the reference metadata");

        TokenAccountingService service = TokenAccountingService.defaults();
        var canonicalEstimate =
                service.estimate(
                        new TokenAccountingService.TokenSurface(List.of(canonical), List.of()));
        var preparedEstimate =
                service.estimate(
                        new TokenAccountingService.TokenSurface(
                                List.of(preparedMessage), List.of()));

        assertEquals(
                2 * MediaTokenEstimator.DEFAULT_MEDIA_TOKENS,
                canonicalEstimate.mediaTokens(),
                "each unresolved reference is charged once before expansion");
        assertEquals(
                2 * MediaTokenEstimator.DEFAULT_MEDIA_TOKENS,
                preparedEstimate.mediaTokens(),
                "each expanded media block is charged once, not twice");
        assertEquals(
                canonicalEstimate.totalTokens()
                        - service.countText("[att at_1]")
                        - service.countText("[att at_2]"),
                preparedEstimate.totalTokens(),
                "expansion only replaces placeholder text with media blocks");

        assertEquals(List.of("[att at_1]", "[att at_2]"), texts(canonical));
        assertNotNull(canonical.getMetadata());
        assertTrue(canonical.getMetadata().containsKey(AttachmentReference.METADATA_KEY));
    }

    private static HarnessContextBuilder builder(AttachmentResolver resolver) {
        return new HarnessContextBuilder(
                ContextPolicy.defaults(),
                null,
                null,
                ContextSources.empty(),
                ContextSelectionPolicy.defaults(),
                new AttachmentRequestCompiler(resolver));
    }

    private static ModelCallInput request(Msg message) {
        return request(message, 1000);
    }

    private static ModelCallInput request(Msg message, int windowSize) {
        Model model = mock(Model.class);
        when(model.getContextWindowSize()).thenReturn(windowSize);
        when(model.getModelName()).thenReturn("test");
        return new ModelCallInput(
                List.of(message),
                List.of(),
                GenerateOptions.builder().maxTokens(20).build(),
                model);
    }

    private static Msg twoAttachmentMessage() {
        AttachmentReference first =
                new AttachmentReference("att_1", "image/png", "a.png", "sha1", 4, 0);
        AttachmentReference second =
                new AttachmentReference("att_2", "application/pdf", "b.pdf", "sha2", 3, 1);
        return Msg.builder()
                .role(MsgRole.USER)
                .content(
                        TextBlock.builder().text("[att at_1]").build(),
                        TextBlock.builder().text("[att at_2]").build())
                .metadata(
                        Map.of(
                                AttachmentReference.METADATA_KEY,
                                List.of(first.toMetadata(), second.toMetadata())))
                .build();
    }

    private static Msg attachmentMessage() {
        AttachmentReference reference =
                new AttachmentReference("att_ref", "image/png", "photo.png", null, 4, 0);
        return Msg.builder()
                .role(MsgRole.USER)
                .content(TextBlock.builder().text("see attached").build())
                .metadata(Map.of(AttachmentReference.METADATA_KEY, List.of(reference.toMetadata())))
                .build();
    }

    private static Msg userMessage(ModelCallInput input) {
        return input.messages().stream()
                .filter(message -> message.getRole() == MsgRole.USER)
                .findFirst()
                .orElseThrow();
    }

    private static List<String> texts(Msg message) {
        return message.getContentBlocks(TextBlock.class).stream().map(TextBlock::getText).toList();
    }
}
