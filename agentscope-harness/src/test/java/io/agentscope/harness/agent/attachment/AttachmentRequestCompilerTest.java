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
package io.agentscope.harness.agent.attachment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.message.Base64Source;
import io.agentscope.core.message.ContentBlock;
import io.agentscope.core.message.DataBlock;
import io.agentscope.core.message.ImageBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.URLSource;
import io.agentscope.core.middleware.ModelCallInput;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class AttachmentRequestCompilerTest {

    @Test
    void inlineReferenceCompilesToImageBlockWithBase64AndDropsMetadata() {
        AttachmentReference reference = reference("att_1", "image/png", 0);
        ModelCallInput input = input(placeholder(reference, "att_1"));

        ModelCallInput compiled =
                compile(input, inlineResolver("image/png", "QUJD"), context("s1"));

        ContentBlock block = compiled.messages().get(0).getContent().get(0);
        ImageBlock image = assertInstanceOf(ImageBlock.class, block);
        assertEquals("QUJD", ((Base64Source) image.getSource()).getData());
        assertFalse(
                compiled.messages()
                        .get(0)
                        .getMetadata()
                        .containsKey(AttachmentReference.METADATA_KEY));
    }

    @Test
    void remoteReferenceCompilesToUrlSource() {
        AttachmentReference reference = reference("att_2", "image/jpeg", 0);
        ModelCallInput input = input(placeholder(reference, "att_2"));

        ModelCallInput compiled =
                compile(
                        input,
                        (r, c) ->
                                Mono.just(
                                        new ResolvedAttachment.Remote(
                                                "https://cdn.example.com/a.jpg", "image/jpeg")),
                        context("s1"));

        ImageBlock image =
                assertInstanceOf(ImageBlock.class, compiled.messages().get(0).getContent().get(0));
        URLSource source = assertInstanceOf(URLSource.class, image.getSource());
        assertEquals("https://cdn.example.com/a.jpg", source.getUrl());
    }

    @Test
    void nonMediaReferenceCompilesToDataBlockWithIdentity() {
        AttachmentReference reference =
                new AttachmentReference("att_3", "application/pdf", "spec.pdf", "sha", 12, 0);
        ModelCallInput input = input(placeholder(reference, "att_3"));

        ModelCallInput compiled =
                compile(input, inlineResolver("application/pdf", "UERG"), context("s1"));

        DataBlock data =
                assertInstanceOf(DataBlock.class, compiled.messages().get(0).getContent().get(0));
        assertEquals("att_3", data.getId());
        assertEquals("spec.pdf", data.getName());
    }

    @Test
    void unavailableReferenceBecomesStructuredErrorMarkerNotSilentText() {
        AttachmentReference reference = reference("att_4", "image/png", 0);
        ModelCallInput input = input(placeholder(reference, "att_4"));

        ModelCallInput compiled =
                compile(
                        input,
                        (r, c) ->
                                Mono.just(
                                        new ResolvedAttachment.Unavailable(
                                                ResolvedAttachment.Unavailable.Category.TOO_LARGE,
                                                "oversize")),
                        context("s1"));

        TextBlock marker =
                assertInstanceOf(TextBlock.class, compiled.messages().get(0).getContent().get(0));
        assertTrue(marker.getText().contains("TOO_LARGE"));
        assertFalse(marker.getText().contains("att_4"));
    }

    @Test
    void resolverErrorBecomesFailedMarkerWithoutLeaking() {
        AttachmentReference reference = reference("att_5", "image/png", 0);
        ModelCallInput input = input(placeholder(reference, "att_5"));

        ModelCallInput compiled =
                compile(
                        input,
                        (r, c) -> Mono.error(new IllegalStateException("secret token=abc")),
                        context("s1"));

        TextBlock marker =
                assertInstanceOf(TextBlock.class, compiled.messages().get(0).getContent().get(0));
        assertTrue(marker.getText().contains("FAILED"));
        assertFalse(marker.getText().contains("secret"));
    }

    @Test
    void emptyResolverResultBecomesFailedMarkerAndStripsMetadata() {
        AttachmentReference reference = reference("att_empty", "image/png", 0);
        ModelCallInput input = input(placeholder(reference, "att_empty"));

        ModelCallInput compiled = compile(input, (r, c) -> Mono.empty(), context("s1"));

        TextBlock marker =
                assertInstanceOf(TextBlock.class, compiled.messages().get(0).getContent().get(0));
        assertTrue(marker.getText().contains("FAILED"));
        assertFalse(
                compiled.messages()
                        .get(0)
                        .getMetadata()
                        .containsKey(AttachmentReference.METADATA_KEY));
    }

    @Test
    void malformedMetadataEntryBecomesMarkerWithoutFailingTheRequest() {
        Msg message =
                Msg.builder()
                        .role(MsgRole.USER)
                        .content(TextBlock.builder().text("[attachment id=broken]").build())
                        .metadata(Map.of(AttachmentReference.METADATA_KEY, List.of("not-a-map")))
                        .build();

        ModelCallInput compiled =
                compile(input(message), inlineResolver("image/png", "QUJD"), context("s1"));

        TextBlock marker =
                assertInstanceOf(TextBlock.class, compiled.messages().get(0).getContent().get(1));
        assertTrue(marker.getText().contains("FAILED"));
        assertFalse(
                compiled.messages()
                        .get(0)
                        .getMetadata()
                        .containsKey(AttachmentReference.METADATA_KEY));
    }

    @Test
    void duplicateOrdinalsKeepBothBlocks() {
        AttachmentReference first = reference("att_dup1", "image/png", 1);
        AttachmentReference second = reference("att_dup2", "image/png", 1);
        Msg message =
                Msg.builder()
                        .role(MsgRole.USER)
                        .content(
                                TextBlock.builder().text("lead").build(),
                                TextBlock.builder().text("[attachment id=att_dup1]").build())
                        .metadata(
                                Map.of(
                                        AttachmentReference.METADATA_KEY,
                                        List.of(first.toMetadata(), second.toMetadata())))
                        .build();

        ModelCallInput compiled =
                compile(input(message), inlineResolver("image/png", "QUJD"), context("s1"));

        List<ContentBlock> content = compiled.messages().get(0).getContent();
        assertEquals(3, content.size());
        assertInstanceOf(TextBlock.class, content.get(0));
        assertInstanceOf(ImageBlock.class, content.get(1));
        assertInstanceOf(ImageBlock.class, content.get(2));
    }

    @Test
    void repeatedReferenceResolvesOnce() {
        AttachmentReference reference = reference("att_same", "image/png", 0);
        Msg message =
                Msg.builder()
                        .role(MsgRole.USER)
                        .content(TextBlock.builder().text("[attachment id=att_same]").build())
                        .metadata(
                                Map.of(
                                        AttachmentReference.METADATA_KEY,
                                        List.of(reference.toMetadata(), reference.toMetadata())))
                        .build();
        java.util.concurrent.atomic.AtomicInteger calls =
                new java.util.concurrent.atomic.AtomicInteger();
        AttachmentResolver counting =
                (r, c) -> {
                    calls.incrementAndGet();
                    return Mono.just(new ResolvedAttachment.Inline("image/png", "QUJD"));
                };

        compile(input(message), counting, context("s1"));

        assertEquals(1, calls.get());
    }

    @Test
    void systemMessageAttachmentsAreNotCompiled() {
        AttachmentReference reference = reference("att_sys", "image/png", 0);
        Msg system =
                Msg.builder()
                        .role(MsgRole.SYSTEM)
                        .content(TextBlock.builder().text("[attachment id=att_sys]").build())
                        .metadata(
                                Map.of(
                                        AttachmentReference.METADATA_KEY,
                                        List.of(reference.toMetadata())))
                        .build();

        ModelCallInput compiled =
                compile(input(system), inlineResolver("image/png", "QUJD"), context("s1"));

        assertInstanceOf(TextBlock.class, compiled.messages().get(0).getContent().get(0));
    }

    @Test
    void messageWithoutReferenceMetadataIsPassedThrough() {
        Msg plain = Msg.builder().role(MsgRole.USER).textContent("hello").build();
        ModelCallInput input = input(plain);

        ModelCallInput compiled =
                compile(input, inlineResolver("image/png", "QUJD"), context("s1"));

        assertSame(input.messages().get(0), compiled.messages().get(0));
    }

    @Test
    void cancellationFromResolverPropagates() {
        AttachmentReference reference = reference("att_6", "image/png", 0);
        ModelCallInput input = input(placeholder(reference, "att_6"));

        StepVerifier.create(
                        new AttachmentRequestCompiler(
                                        (r, c) ->
                                                Mono.error(new CancellationException("cancelled")))
                                .compile(input, context("s1")))
                .expectError(CancellationException.class)
                .verify();
    }

    @Test
    void disposingRequestCancelsTheInFlightResolverSubscription() {
        AttachmentReference reference = reference("att_7", "image/png", 0);
        ModelCallInput input = input(placeholder(reference, "att_7"));
        java.util.concurrent.atomic.AtomicBoolean resolverCancelled =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        AttachmentResolver resolver =
                (r, c) ->
                        Mono.<ResolvedAttachment>never()
                                .doOnCancel(() -> resolverCancelled.set(true));

        reactor.core.Disposable subscription =
                new AttachmentRequestCompiler(resolver).compile(input, context("s1")).subscribe();
        subscription.dispose();

        assertTrue(
                resolverCancelled.get(),
                "cancelling the request must propagate to the in-flight resolver subscription");
    }

    @Test
    void multipleReferencesReplaceTheirOwnOrdinals() {
        AttachmentReference first = reference("att_a", "image/png", 1);
        AttachmentReference second = reference("att_b", "image/png", 2);
        Msg message =
                Msg.builder()
                        .role(MsgRole.USER)
                        .content(
                                TextBlock.builder().text("lead").build(),
                                TextBlock.builder().text("[attachment id=att_a]").build(),
                                TextBlock.builder().text("[attachment id=att_b]").build())
                        .metadata(
                                Map.of(
                                        AttachmentReference.METADATA_KEY,
                                        List.of(first.toMetadata(), second.toMetadata())))
                        .build();

        ModelCallInput compiled =
                compile(input(message), inlineResolver("image/png", "QUJD"), context("s1"));

        List<ContentBlock> content = compiled.messages().get(0).getContent();
        assertInstanceOf(TextBlock.class, content.get(0));
        assertInstanceOf(ImageBlock.class, content.get(1));
        assertInstanceOf(ImageBlock.class, content.get(2));
    }

    private ModelCallInput compile(
            ModelCallInput input, AttachmentResolver resolver, AttachmentResolveContext context) {
        return new AttachmentRequestCompiler(resolver).compile(input, context).block();
    }

    private static ModelCallInput input(Msg message) {
        return new ModelCallInput(List.of(message), List.of(), null, null);
    }

    private static Msg placeholder(AttachmentReference reference, String idText) {
        return Msg.builder()
                .role(MsgRole.USER)
                .content(TextBlock.builder().text("[attachment id=" + idText + "]").build())
                .metadata(Map.of(AttachmentReference.METADATA_KEY, List.of(reference.toMetadata())))
                .build();
    }

    private static AttachmentReference reference(String id, String mediaType, int ordinal) {
        return new AttachmentReference(id, mediaType, "file", "sha", 10, ordinal);
    }

    private static AttachmentResolver inlineResolver(String mediaType, String data) {
        return (r, c) -> Mono.just(new ResolvedAttachment.Inline(mediaType, data));
    }

    private static AttachmentResolveContext context(String sessionId) {
        return new AttachmentResolveContext("user", sessionId, "agent", "call");
    }
}
