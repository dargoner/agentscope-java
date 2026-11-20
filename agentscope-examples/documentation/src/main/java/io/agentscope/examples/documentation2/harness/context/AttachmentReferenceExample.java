/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.agentscope.examples.documentation2.harness.context;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.state.AgentState;
import io.agentscope.extensions.model.dashscope.DashScopeChatModel;
import io.agentscope.extensions.model.dashscope.formatter.DashScopeChatFormatter;
import io.agentscope.harness.agent.HarnessAgent;
import io.agentscope.harness.agent.attachment.AttachmentReference;
import io.agentscope.harness.agent.attachment.AttachmentResolveContext;
import io.agentscope.harness.agent.attachment.AttachmentResolver;
import io.agentscope.harness.agent.attachment.ResolvedAttachment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import reactor.core.publisher.Mono;

/**
 * AttachmentReferenceExample — Demonstrates request-scoped attachment compilation.
 *
 * <p>What this example shows:
 * <ol>
 *   <li><b>Placeholder plus reference in history</b> — the canonical user message keeps a text
 *       placeholder and records the attachment as an {@link AttachmentReference} under the fixed
 *       {@code agentscope.attachments} metadata key; no bytes live in the conversation.</li>
 *   <li><b>Compilation at the request boundary</b> — the registered {@link AttachmentResolver}
 *       expands the reference into a media block for one model request only; an unresolved
 *       reference becomes a structured {@code [attachment unavailable: ...]} marker instead of a
 *       silent placeholder.</li>
 *   <li><b>Per-request resolution</b> — the resolver runs again for every prepared request,
 *       including requests that re-send older messages.</li>
 *   <li><b>History stays untouched</b> — after the calls, canonical history still carries the
 *       placeholder and the reference, never the resolved Base64.</li>
 * </ol>
 *
 * <p>Without {@code attachmentResolver(...)}, references are not expanded at all and the model
 * only sees the placeholder text.
 *
 * <p><b>Run:</b>
 * <pre>
 *   export DASHSCOPE_API_KEY=your_key
 *   mvn exec:java -pl agentscope-examples/documentation \
 *       -Dexec.mainClass=io.agentscope.examples.documentation2.harness.context.AttachmentReferenceExample
 * </pre>
 */
public class AttachmentReferenceExample {

    /** Minimal 20x20 red square PNG. A production resolver reads bytes from its own storage. */
    private static final String RED_SQUARE_PNG =
            "iVBORw0KGgoAAAANSUhEUgAAABQAAAAUCAIAAAAC64paAAAAGklEQVR42mP4z8BANmIY1TyqeVTz"
                    + "qOaB1QwAMc2OgIQLi4wAAAAASUVORK5CYII=";

    public static void main(String[] args) throws Exception {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("Attachments — Request-Scoped Reference Compilation");
        System.out.println("=".repeat(60) + "\n");

        Path workspace = Files.createTempDirectory("agentscope-attachment-example");

        HarnessAgent agent =
                HarnessAgent.builder()
                        .name("attachment-demo")
                        .sysPrompt("You are a concise assistant. Keep answers under two sentences.")
                        .model(
                                DashScopeChatModel.builder()
                                        .apiKey(System.getenv("DASHSCOPE_API_KEY"))
                                        .modelName("qwen-vl-max")
                                        .stream(true)
                                        .formatter(new DashScopeChatFormatter())
                                        .defaultOptions(GenerateOptions.builder().build())
                                        .build())
                        .workspace(workspace)
                        .attachmentResolver(AttachmentReferenceExample::resolve)
                        .build();

        RuntimeContext ctx = RuntimeContext.builder().sessionId("attachment-demo").build();

        // ── A reference the resolver can expand into an image block ─────────

        Msg withImage =
                attachmentMessage(
                        "What color is the image in the attachment?",
                        new AttachmentReference(
                                "att_red_square", "image/png", "red-square.png", null, 0, 1));
        Msg reply = agent.call(withImage, ctx).block();
        System.out.println(
                "\nAgent: " + (reply != null ? reply.getTextContent() : "(null)") + "\n");

        // ── A reference the resolver cannot satisfy ─────────────────────────

        Msg missing =
                attachmentMessage(
                        "Can you open the report attachment?",
                        new AttachmentReference(
                                "att_report_missing", "application/pdf", "report.pdf", null, 0, 1));
        reply = agent.call(missing, ctx).block();
        System.out.println(
                "\nAgent: " + (reply != null ? reply.getTextContent() : "(null)") + "\n");

        printCanonicalHistory(agent, ctx);

        System.out.println("Workspace: " + workspace);
        System.out.println("\n" + "=".repeat(60));
        System.out.println("Done.");
        System.out.println("=".repeat(60));
    }

    /**
     * Demo resolver: inline content for the known id, a structured {@code NOT_FOUND} otherwise. A
     * production resolver must authorize the read against {@code context.userId()} and
     * {@code context.sessionId()} and return bounded inline content or an Unavailable category —
     * never a raw URL, token, or exception message.
     */
    private static Mono<ResolvedAttachment> resolve(
            AttachmentReference reference, AttachmentResolveContext context) {
        System.out.println(
                "  resolver called: "
                        + reference.referenceId()
                        + " (user="
                        + context.userId()
                        + ", session="
                        + context.sessionId()
                        + ")");
        if ("att_red_square".equals(reference.referenceId())) {
            return Mono.just(new ResolvedAttachment.Inline("image/png", RED_SQUARE_PNG));
        }
        return Mono.just(
                new ResolvedAttachment.Unavailable(
                        ResolvedAttachment.Unavailable.Category.NOT_FOUND, "demo-catalog"));
    }

    /** Builds a message that keeps the question text plus a placeholder for ordinal 1. */
    private static Msg attachmentMessage(String question, AttachmentReference reference) {
        return Msg.builder()
                .role(MsgRole.USER)
                .content(
                        TextBlock.builder().text(question).build(),
                        TextBlock.builder()
                                .text("[attachment placeholder: " + reference.name() + "]")
                                .build())
                .metadata(Map.of(AttachmentReference.METADATA_KEY, List.of(reference.toMetadata())))
                .build();
    }

    /** Shows that canonical history keeps placeholders and references, not resolved content. */
    private static void printCanonicalHistory(HarnessAgent agent, RuntimeContext ctx) {
        AgentState state = agent.getDelegate().getAgentState(ctx);
        System.out.println("── Canonical history (placeholder + reference only) ──\n");
        if (state == null) {
            System.out.println("  (no state)");
            return;
        }
        for (Msg message : state.getContext()) {
            if (message.getRole() != MsgRole.USER) {
                continue;
            }
            Map<String, Object> metadata = message.getMetadata();
            boolean hasReference =
                    metadata != null && metadata.containsKey(AttachmentReference.METADATA_KEY);
            System.out.printf(
                    "  user: %s%n         reference=%s%n", message.getTextContent(), hasReference);
        }
        System.out.println();
    }
}
