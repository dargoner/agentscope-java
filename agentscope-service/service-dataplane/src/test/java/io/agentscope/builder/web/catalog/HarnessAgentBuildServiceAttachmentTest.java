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
package io.agentscope.builder.web.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.agentscope.builder.control.ControlPlaneClient;
import io.agentscope.builder.control.SessionResolveResult;
import io.agentscope.builder.web.managed.DataSessionService;
import io.agentscope.builder.web.managed.EnvironmentSpecFactory;
import io.agentscope.builder.web.managed.ManagedSessionDto;
import io.agentscope.builder.web.managed.MemoryMountService;
import io.agentscope.builder.web.managed.SessionAgentBuildSpec;
import io.agentscope.builder.web.managed.SessionAttachmentStore;
import io.agentscope.builder.web.managed.SessionResourceMountService;
import io.agentscope.builder.web.managed.VaultCredentialResolver;
import io.agentscope.builder.web.toolbus.ToolConfirmationMiddleware;
import io.agentscope.builder.web.toolbus.ToolEventBus;
import io.agentscope.builder.web.workspace.SharedWorkspacePaths;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Base64Source;
import io.agentscope.core.message.ImageBlock;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.model.ChatModelBase;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.GenerateOptions;
import io.agentscope.core.model.Model;
import io.agentscope.core.model.ToolSchema;
import io.agentscope.core.session.JournalSessionLog;
import io.agentscope.core.state.InMemoryAgentStateStore;
import io.agentscope.harness.agent.HarnessAgent;
import io.agentscope.harness.agent.attachment.AttachmentReference;
import io.agentscope.harness.agent.attachment.AttachmentRegistration;
import io.agentscope.harness.agent.attachment.AttachmentStoreScope;
import io.agentscope.harness.agent.filesystem.remote.store.InMemoryStore;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

/**
 * Wiring contract for the session/user-bound attachment resolver in {@link
 * HarnessAgentBuildService}: each built agent resolves references against its own session scope,
 * and a service without an {@code AttachmentStore} leaves references untouched so the embedding
 * product keeps full control of the request shape.
 */
class HarnessAgentBuildServiceAttachmentTest {

    @TempDir Path workspace;

    @Test
    void resolvesReferencesOnlyForTheOwningSessionAndMarksOtherSessions() {
        var model = new CapturingModel();
        var control = mock(ControlPlaneClient.class);
        when(control.resolveSession("s1")).thenReturn(resolveResult(session("s1")));
        when(control.resolveSession("s2")).thenReturn(resolveResult(session("s2")));
        var sessions = mock(DataSessionService.class);
        when(sessions.resolve(anyString()))
                .thenAnswer(call -> control.resolveSession(call.getArgument(0)));
        var builds = buildService(model, sessions, control);
        var attachmentStore = new SessionAttachmentStore(new InMemoryStore(), 4096);
        builds.setAttachmentStore(attachmentStore);
        byte[] body = "png-body".getBytes(StandardCharsets.UTF_8);
        String referenceId =
                attachmentStore.register(
                        new AttachmentStoreScope("owner-a", "s1"),
                        new AttachmentRegistration(
                                "upload-1",
                                "image/png",
                                "photo.png",
                                JournalSessionLog.hash(body),
                                body.length,
                                new ByteArrayInputStream(body)));
        AttachmentReference reference =
                new AttachmentReference(
                        referenceId,
                        "image/png",
                        "photo.png",
                        JournalSessionLog.hash(body),
                        body.length,
                        0);
        try {
            HarnessAgent owner = builds.getOrBuildAgent(session("s1"), spec());
            HarnessAgent sibling = builds.getOrBuildAgent(session("s2"), spec());
            assertThat(owner).isNotSameAs(sibling);

            owner.call(List.of(attachmentMessage(reference)), context("s1"))
                    .block(Duration.ofSeconds(10));
            ImageBlock image = onlyImageBlock(model.allMessages());
            assertThat(image).isNotNull();
            assertThat(image.getSource()).isInstanceOf(Base64Source.class);
            Base64Source source = (Base64Source) image.getSource();
            assertThat(source.getMediaType()).isEqualTo("image/png");
            assertThat(source.getData()).isEqualTo(Base64.getEncoder().encodeToString(body));
            assertThat(imageMessage(model.allMessages()).getMetadata())
                    .doesNotContainKey(AttachmentReference.METADATA_KEY);

            model.calls.clear();
            sibling.call(List.of(attachmentMessage(reference)), context("s2"))
                    .block(Duration.ofSeconds(10));
            assertThat(onlyImageBlock(model.allMessages())).isNull();
            assertThat(userTexts(model.allMessages()))
                    .anyMatch(text -> text.contains("[attachment unavailable: NOT_FOUND]"));
        } finally {
            builds.close();
        }
    }

    @Test
    void serviceWithoutAttachmentStoreLeavesReferencesUncompiled() {
        var model = new CapturingModel();
        var control = mock(ControlPlaneClient.class);
        when(control.resolveSession("s1")).thenReturn(resolveResult(session("s1")));
        var sessions = mock(DataSessionService.class);
        when(sessions.resolve(anyString()))
                .thenAnswer(call -> control.resolveSession(call.getArgument(0)));
        var builds = buildService(model, sessions, control);
        AttachmentReference reference =
                new AttachmentReference("att_unregistered", "image/png", "photo.png", null, 8, 0);
        try {
            HarnessAgent agent = builds.getOrBuildAgent(session("s1"), spec());
            agent.call(List.of(attachmentMessage(reference)), context("s1"))
                    .block(Duration.ofSeconds(10));

            assertThat(onlyImageBlock(model.allMessages())).isNull();
            assertThat(userTexts(model.allMessages())).contains("see attached");
            assertThat(userTexts(model.allMessages()))
                    .noneMatch(text -> text.contains("[attachment unavailable"));
        } finally {
            builds.close();
        }
    }

    private HarnessAgentBuildService buildService(
            Model model, DataSessionService sessions, ControlPlaneClient control) {
        return new HarnessAgentBuildService(
                Optional.of(model),
                mock(ToolEventBus.class),
                new SharedWorkspacePaths(workspace),
                mock(EnvironmentSpecFactory.class),
                new ToolConfirmationMiddleware(null, sessions),
                mock(MemoryMountService.class),
                mock(VaultCredentialResolver.class),
                new InMemoryAgentStateStore(),
                mock(SessionResourceMountService.class),
                mock(DefinitionStore.class),
                control);
    }

    private static ManagedSessionDto session(String sessionId) {
        return new ManagedSessionDto(
                sessionId, "owner-a", "agent-a", "owner-a", 1, "latest", null, "local", null, null,
                null, null, "idle", null, 0L, 0L, null);
    }

    private static SessionResolveResult resolveResult(ManagedSessionDto session) {
        return new SessionResolveResult(
                session,
                Map.of("name", "attachment-test", "system", "Answer the user.", "maxIters", 2),
                null,
                null,
                null,
                Map.of(),
                null,
                List.of(),
                List.of(),
                null,
                null);
    }

    private static SessionAgentBuildSpec spec() {
        return new SessionAgentBuildSpec(1, "local", null, null, null, null);
    }

    private static RuntimeContext context(String sessionId) {
        return RuntimeContext.builder().sessionId(sessionId).userId("owner-a").build();
    }

    private static Msg attachmentMessage(AttachmentReference reference) {
        return Msg.builder()
                .role(MsgRole.USER)
                .content(TextBlock.builder().text("see attached").build())
                .metadata(Map.of(AttachmentReference.METADATA_KEY, List.of(reference.toMetadata())))
                .build();
    }

    private static ImageBlock onlyImageBlock(List<Msg> messages) {
        return messages.stream()
                .flatMap(message -> message.getContentBlocks(ImageBlock.class).stream())
                .findFirst()
                .orElse(null);
    }

    private static Msg imageMessage(List<Msg> messages) {
        return messages.stream()
                .filter(message -> !message.getContentBlocks(ImageBlock.class).isEmpty())
                .findFirst()
                .orElseThrow();
    }

    private static List<String> userTexts(List<Msg> messages) {
        return messages.stream()
                .filter(message -> message.getRole() == MsgRole.USER)
                .flatMap(message -> message.getContentBlocks(TextBlock.class).stream())
                .map(TextBlock::getText)
                .toList();
    }

    /** Records every model request so the test can inspect what reached the provider boundary. */
    private static final class CapturingModel extends ChatModelBase {

        private final List<List<Msg>> calls = new CopyOnWriteArrayList<>();

        @Override
        public String getModelName() {
            return "capture";
        }

        @Override
        protected Flux<ChatResponse> doStream(
                List<Msg> messages, List<ToolSchema> tools, GenerateOptions options) {
            calls.add(List.copyOf(messages));
            return Flux.just(
                    ChatResponse.builder()
                            .content(List.of(TextBlock.builder().text("done").build()))
                            .build());
        }

        List<Msg> allMessages() {
            return calls.stream().flatMap(List::stream).toList();
        }
    }
}
