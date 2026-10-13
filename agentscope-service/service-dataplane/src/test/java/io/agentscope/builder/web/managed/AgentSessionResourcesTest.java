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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.agentscope.builder.web.managed.service.SessionEventLog;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.session.InMemorySessionLogStore;
import io.agentscope.core.session.SessionKey;
import io.agentscope.harness.agent.attachment.AttachmentReference;
import io.agentscope.harness.agent.attachment.AttachmentStoreException;
import io.agentscope.harness.agent.filesystem.remote.store.InMemoryStore;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

class AgentSessionResourcesTest {
    @Test
    void immutableFilesResolveIntoStructuredInputAndRemainSessionScoped() {
        var store = new InMemoryStore();
        var files = new SessionFileService(store, 1000);
        var attachments =
                new AttachmentInputNormalizer(files, new SessionAttachmentStore(store, 1000));
        var body = "document".getBytes(StandardCharsets.UTF_8);
        var uploaded = files.upload("s", "key", "report.pdf", "application/pdf", body);
        assertThat(files.upload("s", "key", "report.pdf", "application/pdf", body))
                .isEqualTo(uploaded);
        String id = (String) uploaded.get("file_id");
        var input =
                new AgentSessionInput(
                        null,
                        List.of(
                                new AgentSessionInput.Message(
                                        "user", List.of(Map.of("type", "file", "file_id", id)))));

        List<Msg> messages =
                AgentSessionInput.messages(
                        attachments.normalize("u", "s", input.normalized()), "c");

        assertThat(messages).hasSize(1);
        Msg message = messages.get(0);
        assertThat(message.getContent()).hasSize(1);
        assertThat(message.getContent().get(0)).isInstanceOf(TextBlock.class);
        assertThat(((TextBlock) message.getContent().get(0)).getText())
                .startsWith("[attachment id=att_");
        assertThat(message.getMetadata()).containsKey(AttachmentReference.METADATA_KEY);
        @SuppressWarnings("unchecked")
        var reference =
                (Map<String, Object>)
                        ((List<?>) message.getMetadata().get(AttachmentReference.METADATA_KEY))
                                .get(0);
        assertThat(reference.get("mediaType")).isEqualTo("application/pdf");
        assertThat(reference.get("name")).isEqualTo("report.pdf");
        assertThat(reference.get("sha256")).isEqualTo(uploaded.get("sha256"));
        assertThat(((Number) reference.get("size")).longValue()).isEqualTo(body.length);
        assertThat(reference.get("blockOrdinal")).isEqualTo(0);

        assertThat(files.content("s", id)).isEqualTo(body);
        assertThatThrownBy(() -> files.get("other", id))
                .isInstanceOf(ResponseStatusException.class);
        assertThatThrownBy(
                        () ->
                                files.upload(
                                        "s",
                                        "key",
                                        "report.pdf",
                                        "application/pdf",
                                        new byte[] {1}))
                .isInstanceOf(ResponseStatusException.class);
        assertThatThrownBy(() -> new AgentSessionInput("a", List.of()).normalized())
                .isInstanceOf(ResponseStatusException.class);
        assertThatThrownBy(
                        () ->
                                new AgentSessionInput(
                                                null,
                                                List.of(
                                                        new AgentSessionInput.Message(
                                                                "system",
                                                                List.of(
                                                                        Map.of(
                                                                                "type", "text",
                                                                                "text",
                                                                                "inject")))))
                                        .normalized())
                .isInstanceOf(ResponseStatusException.class);
    }

    @Test
    void storedReferenceIsIdempotentAndNotVisibleToAnotherScope() {
        var store = new InMemoryStore();
        var files = new SessionFileService(store, 1000);
        var attachments =
                new AttachmentInputNormalizer(files, new SessionAttachmentStore(store, 1000));
        var bytes = "image".getBytes(StandardCharsets.UTF_8);
        String id = (String) files.upload("s", "key", "a.png", "image/png", bytes).get("file_id");
        var input =
                new AgentSessionInput(
                        null,
                        List.of(
                                new AgentSessionInput.Message(
                                        "user", List.of(Map.of("type", "file", "file_id", id)))));

        List<Msg> first =
                AgentSessionInput.messages(
                        attachments.normalize("u", "s", input.normalized()), "c1");
        List<Msg> second =
                AgentSessionInput.messages(
                        attachments.normalize("u", "s", input.normalized()), "c2");
        String referenceId = ((TextBlock) first.get(0).getContent().get(0)).getText();
        assertThat(((TextBlock) second.get(0).getContent().get(0)).getText())
                .isEqualTo(referenceId)
                .as("re-normalizing the same file must return the same reference");

        var scope = new io.agentscope.harness.agent.attachment.AttachmentStoreScope("u", "s");
        var otherSession =
                new io.agentscope.harness.agent.attachment.AttachmentStoreScope("u", "other");
        SessionAttachmentStore attachmentStore = new SessionAttachmentStore(store, 1000);
        assertThat(attachmentStore.resolve(scope, referenceIdToId(referenceId))).isNotNull();
        assertThatThrownBy(
                        () -> attachmentStore.resolve(otherSession, referenceIdToId(referenceId)))
                .isInstanceOf(AttachmentStoreException.class);
    }

    private static String referenceIdToId(String placeholder) {
        return placeholder.substring("[attachment id=".length(), placeholder.length() - 1);
    }

    @Test
    void concurrentBudgetAdmissionNeverExceedsTheModelCallQuota() {
        var session = mock(ManagedSessionDto.class);
        when(session.id()).thenReturn("s");
        var logs = mock(SessionNativeLogService.class);
        var log =
                new InMemorySessionLogStore()
                        .open(
                                new SessionKey("u", "a", "s"),
                                RuntimeContext.builder().userId("u").sessionId("s").build());
        when(logs.open(session)).thenReturn(log);
        when(logs.descendants(session)).thenReturn(Map.of());
        var service =
                new SessionBudgetService(
                        new InMemoryStore(),
                        (model, usage) -> new SessionUsagePricer.Quote(BigDecimal.ZERO, "USD"),
                        mock(SessionEventLog.class),
                        logs);
        service.configure("s", new SessionBudgetService.Budget(3L, null, null, null));
        var policy = service.policy(session);
        var admitted = new AtomicInteger();
        var tasks =
                IntStream.range(0, 20)
                        .mapToObj(
                                i ->
                                        CompletableFuture.runAsync(
                                                () -> {
                                                    try {
                                                        policy.beforeCall(
                                                                "m" + i,
                                                                "test",
                                                                RuntimeContext.builder()
                                                                        .sessionId("s")
                                                                        .build());
                                                        admitted.incrementAndGet();
                                                    } catch (
                                                            SessionBudgetService
                                                                            .BudgetExceededException
                                                                    expected) {
                                                    }
                                                }))
                        .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(tasks).join();
        assertThat(admitted.get()).isEqualTo(3);
    }

    @Test
    void configuredPricingReportsUnknownModelsRatherThanInventingZeroCost() {
        var pricer =
                new SessionUsagePricingConfiguration()
                        .sessionUsagePricer(
                                "{\"test\":{\"input_per_million\":2,\"output_per_million\":4}}",
                                "USD");
        assertThat(pricer.quote("test", Map.of("inputTokens", 1000, "outputTokens", 500)).amount())
                .isEqualByComparingTo("0.004");
        assertThat(pricer.quote("unknown", Map.of())).isNull();
    }

    @Test
    void webhookRegistrationIsDurableIdempotentScopedAndAllowlisted() {
        var store = new InMemoryStore();
        var events = mock(SessionEventLog.class);
        var sessions = mock(DataSessionService.class);
        var hooks = new SessionWebhookService(store, events, sessions, "notify.example.com");
        var input =
                new SessionWebhookService.Registration(
                        "https://notify.example.com/events", List.of("turn.*"));
        var created = hooks.register("u", "s", "key", input);
        assertThat(hooks.register("u", "s", "key", input)).isEqualTo(created);
        assertThat(hooks.list("u", "s", 0, 10).toString()).doesNotContain("signing_secret");
        assertThatThrownBy(
                        () ->
                                hooks.register(
                                        "u",
                                        "s",
                                        "bad",
                                        new SessionWebhookService.Registration(
                                                "http://127.0.0.1", null)))
                .isInstanceOf(ResponseStatusException.class);
        assertThatThrownBy(() -> hooks.update("u", "other", (String) created.get("id"), true))
                .isInstanceOf(ResponseStatusException.class);
        assertThat(SessionWebhookService.signature("secret", 123, "{}"))
                .isEqualTo("4468c5e304ca107335ae2982d764fdf304214175c7dc3ad06921848d275abb92");
    }
}
