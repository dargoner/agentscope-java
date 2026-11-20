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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.message.ImageBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.middleware.ModelCallInput;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

/**
 * Verifies the resolver contract the harness relies on: scope is always supplied, resolution is
 * session-isolated, and an out-of-scope reference yields a structured error rather than content.
 */
class AttachmentResolverContractTest {

    @Test
    void resolverReceivesFullScopeForEveryReference() {
        AtomicReference<AttachmentResolveContext> seen = new AtomicReference<>();
        AttachmentResolver resolver =
                (reference, context) -> {
                    seen.set(context);
                    return Mono.just(new ResolvedAttachment.Inline("image/png", "QUJD"));
                };

        compile(resolver, context("user-7", "session-7", "agent-7", "call-7"), "att_x");

        assertEquals("user-7", seen.get().userId());
        assertEquals("session-7", seen.get().sessionId());
        assertEquals("agent-7", seen.get().agentId());
        assertEquals("call-7", seen.get().callId());
    }

    @Test
    void anotherSessionCannotReadThisSessionsAttachment() {
        AtomicReference<AttachmentResolveContext> seen = new AtomicReference<>();
        AttachmentResolver scopedResolver =
                (reference, context) -> {
                    seen.set(context);
                    if (!"owner-session".equals(context.sessionId()))
                        return Mono.just(
                                new ResolvedAttachment.Unavailable(
                                        ResolvedAttachment.Unavailable.Category.FORBIDDEN,
                                        "scope"));
                    return Mono.just(new ResolvedAttachment.Inline("image/png", "QUJD"));
                };

        Msg message = message("att_x");
        ModelCallInput compiled =
                new AttachmentRequestCompiler(scopedResolver)
                        .compile(
                                new ModelCallInput(List.of(message), List.of(), null, null),
                                context("owner", "intruder-session", "agent", "call"))
                        .block();

        assertEquals("intruder-session", seen.get().sessionId());
        assertTrue(
                message.getContent().get(0) instanceof TextBlock,
                "source history must stay a placeholder");
        assertTrue(
                compiled.messages().get(0).getContent().get(0) instanceof TextBlock,
                "out-of-scope reference must not become media");
        assertTrue(
                ((TextBlock) compiled.messages().get(0).getContent().get(0))
                        .getText()
                        .contains("FORBIDDEN"));
    }

    @Test
    void ownerSessionCompilesTheSameReferenceToMedia() {
        AttachmentResolver scopedResolver =
                (reference, context) -> {
                    if (!"owner-session".equals(context.sessionId()))
                        return Mono.just(
                                new ResolvedAttachment.Unavailable(
                                        ResolvedAttachment.Unavailable.Category.FORBIDDEN,
                                        "scope"));
                    return Mono.just(new ResolvedAttachment.Inline("image/png", "QUJD"));
                };

        ModelCallInput compiled =
                compile(
                        scopedResolver,
                        context("owner", "owner-session", "agent", "call"),
                        "att_x");

        assertTrue(compiled.messages().get(0).getContent().get(0) instanceof ImageBlock);
    }

    private ModelCallInput compile(
            AttachmentResolver resolver, AttachmentResolveContext context, String referenceId) {
        Msg message = message(referenceId);
        return new AttachmentRequestCompiler(resolver)
                .compile(new ModelCallInput(List.of(message), List.of(), null, null), context)
                .block();
    }

    private static Msg message(String referenceId) {
        AttachmentReference reference = reference(referenceId);
        return Msg.builder()
                .role(MsgRole.USER)
                .content(TextBlock.builder().text("[attachment id=" + referenceId + "]").build())
                .metadata(Map.of(AttachmentReference.METADATA_KEY, List.of(reference.toMetadata())))
                .build();
    }

    private static AttachmentReference reference(String referenceId) {
        return new AttachmentReference(referenceId, "image/png", "file", "sha", 10, 0);
    }

    private static AttachmentResolveContext context(
            String userId, String sessionId, String agentId, String callId) {
        return new AttachmentResolveContext(userId, sessionId, agentId, callId);
    }
}
