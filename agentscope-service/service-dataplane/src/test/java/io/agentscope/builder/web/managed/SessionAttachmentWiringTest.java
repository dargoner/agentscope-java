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

import io.agentscope.core.session.JournalSessionLog;
import io.agentscope.harness.agent.attachment.AttachmentReference;
import io.agentscope.harness.agent.attachment.AttachmentRegistration;
import io.agentscope.harness.agent.attachment.AttachmentResolveContext;
import io.agentscope.harness.agent.attachment.AttachmentStoreCategory;
import io.agentscope.harness.agent.attachment.AttachmentStoreException;
import io.agentscope.harness.agent.attachment.AttachmentStoreScope;
import io.agentscope.harness.agent.attachment.ResolvedAttachment;
import io.agentscope.harness.agent.filesystem.remote.store.BaseStore;
import io.agentscope.harness.agent.filesystem.remote.store.InMemoryStore;
import io.agentscope.harness.agent.filesystem.remote.store.StoreItem;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

class SessionAttachmentWiringTest {

    private static AttachmentRegistration registration(
            String key, String mediaType, String name, byte[] body) {
        return new AttachmentRegistration(
                key,
                mediaType,
                name,
                JournalSessionLog.hash(body),
                body.length,
                new ByteArrayInputStream(body));
    }

    private static AttachmentStoreCategory categoryOf(Throwable error) {
        return ((AttachmentStoreException) error).category();
    }

    private static ResolvedAttachment.Unavailable.Category unavailable(ResolvedAttachment value) {
        return ((ResolvedAttachment.Unavailable) value).category();
    }

    @Test
    void hexNamespaceDefeatsInBandDelimiterCollision() {
        var store = new InMemoryStore();
        var attachmentStore = new SessionAttachmentStore(store, 4096);
        byte[] body = "payload".getBytes(StandardCharsets.UTF_8);
        var scopeA = new AttachmentStoreScope("a\u0000b", "c");
        var scopeB = new AttachmentStoreScope("a", "b\u0000c");

        String id = attachmentStore.register(scopeA, registration("k", "text/plain", "n", body));

        assertThat(attachmentStore.resolve(scopeA, id)).isNotNull();
        assertThatThrownBy(() -> attachmentStore.resolve(scopeB, id))
                .isInstanceOf(AttachmentStoreException.class)
                .satisfies(
                        e ->
                                assertThat(categoryOf(e))
                                        .isEqualTo(AttachmentStoreCategory.NOT_FOUND));
    }

    @Test
    void registerIsIdempotentConflictsAndDoesNotConsumeKeyOnSizeMismatch() {
        var store = new InMemoryStore();
        var attachmentStore = new SessionAttachmentStore(store, 4096);
        var scope = new AttachmentStoreScope("u", "s");
        byte[] body = "hello".getBytes(StandardCharsets.UTF_8);

        String first = attachmentStore.register(scope, registration("k", "text/plain", "n", body));
        String again = attachmentStore.register(scope, registration("k", "text/plain", "n", body));
        assertThat(again).isEqualTo(first);
        assertThat(first).startsWith("att_");

        assertThatThrownBy(
                        () ->
                                attachmentStore.register(
                                        scope,
                                        registration("k", "text/plain", "n", "changed".getBytes())))
                .isInstanceOf(AttachmentStoreException.class)
                .satisfies(
                        e -> assertThat(categoryOf(e)).isEqualTo(AttachmentStoreCategory.CONFLICT));

        assertThatThrownBy(
                        () ->
                                attachmentStore.register(
                                        scope,
                                        new AttachmentRegistration(
                                                "retry",
                                                "text/plain",
                                                "n",
                                                JournalSessionLog.hash(body),
                                                body.length + 3,
                                                new ByteArrayInputStream(body))))
                .isInstanceOf(AttachmentStoreException.class)
                .satisfies(
                        e -> assertThat(categoryOf(e)).isEqualTo(AttachmentStoreCategory.FAILED));

        assertThat(attachmentStore.register(scope, registration("retry", "text/plain", "n", body)))
                .startsWith("att_");
    }

    @Test
    void openRejectsOversizedRecordBeforeDecoding() {
        var store = new InMemoryStore();
        var scope = new AttachmentStoreScope("u", "s");
        var large = new SessionAttachmentStore(store, 4096);
        byte[] body = new byte[4096];
        Arrays.fill(body, (byte) 7);
        String id = large.register(scope, registration("k", "application/octet-stream", "n", body));

        var small = new SessionAttachmentStore(store, 64);
        assertThatThrownBy(() -> small.open(scope, id))
                .isInstanceOf(AttachmentStoreException.class)
                .satisfies(
                        e ->
                                assertThat(categoryOf(e))
                                        .isEqualTo(AttachmentStoreCategory.TOO_LARGE));
    }

    @Test
    void backendFailureIsSanitizedWithoutLeakingCause() {
        var backing = new ThrowingStore(new InMemoryStore());
        var attachmentStore = new SessionAttachmentStore(backing, 4096);
        backing.fail = true;
        var scope = new AttachmentStoreScope("u", "s");

        assertThatThrownBy(() -> attachmentStore.resolve(scope, "att_missing"))
                .isInstanceOf(AttachmentStoreException.class)
                .satisfies(
                        e -> {
                            var error = (AttachmentStoreException) e;
                            assertThat(error.category()).isEqualTo(AttachmentStoreCategory.FAILED);
                            assertThat(error.getMessage()).doesNotContain("backend-secret");
                            assertThat(error.getCause()).isNull();
                        });
    }

    @Test
    void resolverEnforcesScopeAndReturnsUnavailableNeverThrows() {
        var store = new InMemoryStore();
        var attachmentStore = new SessionAttachmentStore(store, 4096);
        var scope = new AttachmentStoreScope("u", "s");
        byte[] body = "abc".getBytes(StandardCharsets.UTF_8);
        String id = attachmentStore.register(scope, registration("k", "text/plain", "n", body));
        var resolver = new SessionAttachmentResolver(attachmentStore, "u", "s");
        var reference =
                new AttachmentReference(
                        id, "text/plain", "n", JournalSessionLog.hash(body), body.length, 0);

        assertThat(
                        unavailable(
                                resolver.resolve(
                                                reference,
                                                new AttachmentResolveContext(
                                                        "u", "other", null, null))
                                        .block()))
                .isEqualTo(ResolvedAttachment.Unavailable.Category.FORBIDDEN);
        assertThat(
                        resolver.resolve(
                                        reference,
                                        new AttachmentResolveContext("u", "s", null, null))
                                .block())
                .isInstanceOf(ResolvedAttachment.Inline.class);
        assertThat(
                        unavailable(
                                resolver.resolve(
                                                new AttachmentReference(
                                                        "att_missing",
                                                        "text/plain",
                                                        "n",
                                                        JournalSessionLog.hash(body),
                                                        body.length,
                                                        0),
                                                new AttachmentResolveContext("u", "s", null, null))
                                        .block()))
                .isEqualTo(ResolvedAttachment.Unavailable.Category.NOT_FOUND);
        assertThat(
                        unavailable(
                                resolver.resolve(
                                                new AttachmentReference(
                                                        id,
                                                        "text/plain",
                                                        "n",
                                                        JournalSessionLog.hash(body),
                                                        body.length + 1,
                                                        0),
                                                new AttachmentResolveContext("u", "s", null, null))
                                        .block()))
                .isEqualTo(ResolvedAttachment.Unavailable.Category.FAILED);
    }

    @Test
    void concurrentRegistrationsConvergeOnOneReference() throws Exception {
        var store = new InMemoryStore();
        var attachmentStore = new SessionAttachmentStore(store, 4096);
        var scope = new AttachmentStoreScope("u", "s");
        byte[] body = "shared".getBytes(StandardCharsets.UTF_8);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            var tasks =
                    IntStream.range(0, 16)
                            .mapToObj(
                                    i ->
                                            CompletableFuture.supplyAsync(
                                                    () ->
                                                            attachmentStore.register(
                                                                    scope,
                                                                    registration(
                                                                            "same",
                                                                            "text/plain",
                                                                            "n",
                                                                            body)),
                                                    pool))
                            .toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(tasks).get(30, TimeUnit.SECONDS);
            var ids =
                    Arrays.stream(tasks)
                            .map(
                                    task -> {
                                        try {
                                            return (String) task.get();
                                        } catch (Exception error) {
                                            throw new IllegalStateException(error);
                                        }
                                    })
                            .distinct()
                            .toList();
            assertThat(ids).hasSize(1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void normalizerPreservesExistingMessageMetadataAndCallerMap() {
        var store = new InMemoryStore();
        var files = new SessionFileService(store, 4096);
        var normalizer =
                new AttachmentInputNormalizer(files, new SessionAttachmentStore(store, 4096));
        byte[] body = "doc".getBytes(StandardCharsets.UTF_8);
        String fileId =
                (String) files.upload("s", "key", "a.txt", "text/plain", body).get("file_id");
        var message = new LinkedHashMap<String, Object>();
        message.put("role", "user");
        message.put("content", List.of(Map.of("type", "file", "file_id", fileId)));
        message.put("metadata", Map.of("trace", "keep-me"));
        var request = new LinkedHashMap<String, Object>();
        request.put("input", List.of(message));

        Map<String, Object> normalized = normalizer.normalize("u", "s", request);

        @SuppressWarnings("unchecked")
        var outMessage = (Map<String, Object>) ((List<?>) normalized.get("input")).get(0);
        @SuppressWarnings("unchecked")
        var metadata = (Map<String, Object>) outMessage.get("metadata");
        assertThat(metadata).containsEntry("trace", "keep-me");
        assertThat(metadata).containsKey(AttachmentReference.METADATA_KEY);

        @SuppressWarnings("unchecked")
        var originalContent = (List<Map<String, Object>>) message.get("content");
        assertThat(originalContent.get(0)).containsEntry("type", "file");
    }

    @Test
    void malformedUnicodeScopesDoNotCollide() {
        var store = new InMemoryStore();
        var attachmentStore = new SessionAttachmentStore(store, 4096);
        byte[] body = "x".getBytes(StandardCharsets.UTF_8);
        var surrogate = new AttachmentStoreScope("\uD800", "s");
        var question = new AttachmentStoreScope("?", "s");

        String id = attachmentStore.register(surrogate, registration("k", "text/plain", "n", body));

        assertThatThrownBy(() -> attachmentStore.resolve(question, id))
                .isInstanceOf(AttachmentStoreException.class)
                .satisfies(
                        e ->
                                assertThat(categoryOf(e))
                                        .isEqualTo(AttachmentStoreCategory.NOT_FOUND));
    }

    @Test
    void openChecksEncodedLengthBeforeDecoding() {
        var stub = new StubStore();
        // Declared size is within the limit, but the (invalid) encoded payload is over the limit:
        // only a pre-decode bound yields TOO_LARGE; decoding first would report a decode FAILED.
        stub.item =
                new StoreItem(
                        "att_x", record("att_x", "text/plain", "n", null, 4L, "!".repeat(200)), 1L);
        var attachmentStore = new SessionAttachmentStore(stub, 64);

        assertThatThrownBy(() -> attachmentStore.open(new AttachmentStoreScope("u", "s"), "att_x"))
                .isInstanceOf(AttachmentStoreException.class)
                .satisfies(
                        e ->
                                assertThat(categoryOf(e))
                                        .isEqualTo(AttachmentStoreCategory.TOO_LARGE));
    }

    @Test
    void malformedBase64WithinLimitIsSanitizedFailed() {
        var stub = new StubStore();
        stub.item =
                new StoreItem("att_x", record("att_x", "text/plain", "n", null, 4L, "!!!!"), 1L);
        var attachmentStore = new SessionAttachmentStore(stub, 64);

        assertThatThrownBy(() -> attachmentStore.open(new AttachmentStoreScope("u", "s"), "att_x"))
                .isInstanceOf(AttachmentStoreException.class)
                .satisfies(
                        e -> {
                            assertThat(categoryOf(e)).isEqualTo(AttachmentStoreCategory.FAILED);
                            assertThat(((AttachmentStoreException) e).getMessage())
                                    .doesNotContain("!!!!");
                            assertThat(e.getCause()).isNull();
                        });
    }

    @Test
    void nullPersistedRecordIsFailedNotNpe() {
        var stub = new StubStore();
        stub.item = new StoreItem("att_x", null, 1L);
        var attachmentStore = new SessionAttachmentStore(stub, 64);

        assertThatThrownBy(
                        () -> attachmentStore.resolve(new AttachmentStoreScope("u", "s"), "att_x"))
                .isInstanceOf(AttachmentStoreException.class)
                .satisfies(
                        e -> assertThat(categoryOf(e)).isEqualTo(AttachmentStoreCategory.FAILED));
    }

    @Test
    void registerSanitizesStreamFailure() {
        var attachmentStore = new SessionAttachmentStore(new InMemoryStore(), 4096);
        InputStream failing =
                new InputStream() {
                    @Override
                    public int read() {
                        throw new AttachmentStoreException(
                                AttachmentStoreCategory.FAILED, "backend-secret");
                    }

                    @Override
                    public int read(byte[] buffer, int offset, int length) {
                        throw new AttachmentStoreException(
                                AttachmentStoreCategory.FAILED, "backend-secret");
                    }
                };

        assertThatThrownBy(
                        () ->
                                attachmentStore.register(
                                        new AttachmentStoreScope("u", "s"),
                                        new AttachmentRegistration(
                                                "k", "text/plain", "n", null, 4L, failing)))
                .isInstanceOf(AttachmentStoreException.class)
                .satisfies(
                        e -> {
                            assertThat(categoryOf(e)).isEqualTo(AttachmentStoreCategory.FAILED);
                            assertThat(e.getMessage()).doesNotContain("backend-secret");
                            assertThat(e.getCause()).isNull();
                        });
    }

    @Test
    void registerPropagatesCancellation() {
        var attachmentStore = new SessionAttachmentStore(new InMemoryStore(), 4096);
        InputStream cancelling =
                new InputStream() {
                    @Override
                    public int read() {
                        throw new CancellationException();
                    }

                    @Override
                    public int read(byte[] buffer, int offset, int length) {
                        throw new CancellationException();
                    }
                };

        assertThatThrownBy(
                        () ->
                                attachmentStore.register(
                                        new AttachmentStoreScope("u", "s"),
                                        new AttachmentRegistration(
                                                "k", "text/plain", "n", null, 4L, cancelling)))
                .isInstanceOf(CancellationException.class);
    }

    @Test
    void zeroByteAttachmentResolvesToUnsupported() {
        var store = new InMemoryStore();
        var attachmentStore = new SessionAttachmentStore(store, 4096);
        var scope = new AttachmentStoreScope("u", "s");
        String id =
                attachmentStore.register(
                        scope, registration("k", "application/octet-stream", "n", new byte[0]));
        var resolver = new SessionAttachmentResolver(attachmentStore, "u", "s");

        ResolvedAttachment resolved =
                resolver.resolve(
                                new AttachmentReference(
                                        id,
                                        "application/octet-stream",
                                        "n",
                                        JournalSessionLog.hash(new byte[0]),
                                        0,
                                        0),
                                new AttachmentResolveContext("u", "s", null, null))
                        .block();

        assertThat(unavailable(resolved))
                .isEqualTo(ResolvedAttachment.Unavailable.Category.UNSUPPORTED);
    }

    @Test
    void normalizerMergesExistingReferenceListAndKeepsOrdinals() {
        var store = new InMemoryStore();
        var files = new SessionFileService(store, 4096);
        var normalizer =
                new AttachmentInputNormalizer(files, new SessionAttachmentStore(store, 4096));
        byte[] body = "doc".getBytes(StandardCharsets.UTF_8);
        String fileId =
                (String) files.upload("s", "key", "a.txt", "text/plain", body).get("file_id");
        var existingReference = new LinkedHashMap<String, Object>();
        existingReference.put("referenceId", "att_existing");
        existingReference.put("blockOrdinal", 0);
        existingReference.put("size", 0L);
        var priorMetadata = new LinkedHashMap<String, Object>();
        priorMetadata.put("trace", "keep");
        priorMetadata.put(AttachmentReference.METADATA_KEY, List.of(existingReference));
        var message = new LinkedHashMap<String, Object>();
        message.put("role", "user");
        message.put(
                "content",
                List.of(
                        Map.of("type", "text", "text", "hi"),
                        Map.of("type", "file", "file_id", fileId)));
        message.put("metadata", priorMetadata);
        var request = new LinkedHashMap<String, Object>();
        request.put("input", List.of(message));

        Map<String, Object> normalized = normalizer.normalize("u", "s", request);

        @SuppressWarnings("unchecked")
        var outMessage = (Map<String, Object>) ((List<?>) normalized.get("input")).get(0);
        @SuppressWarnings("unchecked")
        var metadata = (Map<String, Object>) outMessage.get("metadata");
        assertThat(metadata).containsEntry("trace", "keep");
        @SuppressWarnings("unchecked")
        var references = (List<Map<String, Object>>) metadata.get(AttachmentReference.METADATA_KEY);
        assertThat(references).hasSize(2);
        assertThat(references.get(0).get("referenceId")).isEqualTo("att_existing");
        assertThat(references.get(1).get("blockOrdinal")).isEqualTo(1);
    }

    private static Map<String, Object> record(
            String id, String mediaType, String name, String sha256, long size, String data) {
        var record = new LinkedHashMap<String, Object>();
        record.put("reference_id", id);
        record.put("media_type", mediaType);
        record.put("name", name);
        record.put("sha256", sha256);
        record.put("size", size);
        record.put("created_at", Instant.now().toString());
        record.put("data", data);
        return record;
    }

    @Test
    void referenceIdsAreDistinctForMalformedUnicodeScopesAndKeys() {
        var attachmentStore = new SessionAttachmentStore(new InMemoryStore(), 4096);
        byte[] body = "x".getBytes(StandardCharsets.UTF_8);

        String surrogateScope =
                attachmentStore.register(
                        new AttachmentStoreScope("\uD800", "s"),
                        registration("same", "text/plain", "n", body));
        String questionScope =
                attachmentStore.register(
                        new AttachmentStoreScope("?", "s"),
                        registration("same", "text/plain", "n", body));
        assertThat(surrogateScope).isNotEqualTo(questionScope);

        String surrogateKey =
                attachmentStore.register(
                        new AttachmentStoreScope("u", "s"),
                        registration("\uD800", "text/plain", "n", body));
        String questionKey =
                attachmentStore.register(
                        new AttachmentStoreScope("u", "s"),
                        registration("?", "text/plain", "n", body));
        assertThat(surrogateKey).isNotEqualTo(questionKey);
    }

    @Test
    void registerTranslatesBackendWriteAndFallbackReadFailures() {
        var writeFailure = new StubStore();
        writeFailure.failOnPut =
                new AttachmentStoreException(AttachmentStoreCategory.FAILED, "backend-secret");
        assertThatThrownBy(
                        () ->
                                new SessionAttachmentStore(writeFailure, 4096)
                                        .register(
                                                new AttachmentStoreScope("u", "s"),
                                                registration(
                                                        "k", "text/plain", "n", "a".getBytes())))
                .isInstanceOf(AttachmentStoreException.class)
                .satisfies(
                        e -> {
                            assertThat(categoryOf(e)).isEqualTo(AttachmentStoreCategory.FAILED);
                            assertThat(e.getMessage()).doesNotContain("backend-secret");
                            assertThat(e.getCause()).isNull();
                        });

        var readFailure = new StubStore();
        readFailure.failOnGet =
                new AttachmentStoreException(AttachmentStoreCategory.FAILED, "backend-secret");
        assertThatThrownBy(
                        () ->
                                new SessionAttachmentStore(readFailure, 4096)
                                        .register(
                                                new AttachmentStoreScope("u", "s"),
                                                registration(
                                                        "k", "text/plain", "n", "a".getBytes())))
                .isInstanceOf(AttachmentStoreException.class)
                .satisfies(
                        e -> {
                            assertThat(categoryOf(e)).isEqualTo(AttachmentStoreCategory.FAILED);
                            assertThat(e.getMessage()).doesNotContain("backend-secret");
                        });
    }

    @Test
    void backendCancellationPropagatesFromStoreAndResolver() {
        var store = new StubStore();
        store.failOnGet = new CancellationException();
        var scope = new AttachmentStoreScope("u", "s");
        var attachmentStore = new SessionAttachmentStore(store, 4096);

        assertThatThrownBy(() -> attachmentStore.resolve(scope, "att_x"))
                .isInstanceOf(CancellationException.class);

        var resolver = new SessionAttachmentResolver(attachmentStore, "u", "s");
        assertThatThrownBy(
                        () ->
                                resolver.resolve(
                                                new AttachmentReference(
                                                        "att_x", "text/plain", "n", null, 0, 0),
                                                new AttachmentResolveContext("u", "s", null, null))
                                        .block())
                .isInstanceOf(CancellationException.class);
    }

    @Test
    void mediaTypeDriftResolvesToMimeMismatch() {
        var store = new InMemoryStore();
        var attachmentStore = new SessionAttachmentStore(store, 4096);
        var scope = new AttachmentStoreScope("u", "s");
        byte[] body = "abc".getBytes(StandardCharsets.UTF_8);
        String id = attachmentStore.register(scope, registration("k", "text/plain", "n", body));
        var resolver = new SessionAttachmentResolver(attachmentStore, "u", "s");

        ResolvedAttachment resolved =
                resolver.resolve(
                                new AttachmentReference(
                                        id,
                                        "image/png",
                                        "n",
                                        JournalSessionLog.hash(body),
                                        body.length,
                                        0),
                                new AttachmentResolveContext("u", "s", null, null))
                        .block();

        assertThat(unavailable(resolved))
                .isEqualTo(ResolvedAttachment.Unavailable.Category.MIME_MISMATCH);
    }

    @Test
    void stalledStoreResolvesToTimeoutCategory() {
        var blocking = new BlockingStore();
        var resolver =
                new SessionAttachmentResolver(
                        new SessionAttachmentStore(blocking, 4096),
                        "u",
                        "s",
                        Duration.ofMillis(50));
        try {
            ResolvedAttachment resolved =
                    resolver.resolve(
                                    new AttachmentReference("att_x", "text/plain", "n", null, 0, 0),
                                    new AttachmentResolveContext("u", "s", null, null))
                            .block(Duration.ofSeconds(5));

            assertThat(unavailable(resolved))
                    .isEqualTo(ResolvedAttachment.Unavailable.Category.TIMEOUT);
        } finally {
            blocking.release.countDown();
        }
    }

    @Test
    void disposedResolutionDeliversNoTimeoutFallback() throws Exception {
        var blocking = new BlockingStore();
        var resolver =
                new SessionAttachmentResolver(
                        new SessionAttachmentStore(blocking, 4096),
                        "u",
                        "s",
                        Duration.ofMillis(50));
        var seen = new AtomicReference<ResolvedAttachment>();
        var error = new AtomicReference<Throwable>();
        try {
            var subscription =
                    resolver.resolve(
                                    new AttachmentReference("att_x", "text/plain", "n", null, 0, 0),
                                    new AttachmentResolveContext("u", "s", null, null))
                            .subscribe(seen::set, error::set);
            subscription.dispose();

            // Give any un-cancelled timeout timer room to fire: a disposed subscription must
            // observe neither a fallback value nor an error.
            Thread.sleep(200);

            assertThat(seen.get()).isNull();
            assertThat(error.get()).isNull();
        } finally {
            blocking.release.countDown();
        }
    }

    @Test
    void fileContentAppliesBoundsAndReadsOneSnapshot() {
        var oversized = new StubStore();
        oversized.item =
                new StoreItem(
                        "file_x",
                        record("file_x", "text/plain", "n", null, 99999L, "YWJjZA=="),
                        1L);
        assertThatThrownBy(() -> new SessionFileService(oversized, 64).content("s", "file_x"))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(
                        e ->
                                assertThat(((ResponseStatusException) e).getStatusCode().value())
                                        .isEqualTo(413));

        var oversizedInvalid = new StubStore();
        oversizedInvalid.item =
                new StoreItem(
                        "file_x",
                        record("file_x", "text/plain", "n", null, 10L, "!".repeat(200)),
                        1L);
        assertThatThrownBy(
                        () -> new SessionFileService(oversizedInvalid, 64).content("s", "file_x"))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(
                        e ->
                                assertThat(((ResponseStatusException) e).getStatusCode().value())
                                        .isEqualTo(413));

        var malformed = new StubStore();
        malformed.item =
                new StoreItem("file_x", record("file_x", "text/plain", "n", null, 4L, "!!!!"), 1L);
        assertThatThrownBy(() -> new SessionFileService(malformed, 64).content("s", "file_x"))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(
                        e ->
                                assertThat(((ResponseStatusException) e).getStatusCode().value())
                                        .isEqualTo(500));

        var mismatch = new StubStore();
        mismatch.item =
                new StoreItem(
                        "file_x", record("file_x", "text/plain", "n", null, 3L, "YWJjZA=="), 1L);
        assertThatThrownBy(() -> new SessionFileService(mismatch, 64).content("s", "file_x"))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(
                        e ->
                                assertThat(((ResponseStatusException) e).getStatusCode().value())
                                        .isEqualTo(500));

        var counted = new StubStore();
        counted.item =
                new StoreItem(
                        "file_x", record("file_x", "text/plain", "n", null, 4L, "YWJjZA=="), 1L);
        assertThat(new SessionFileService(counted, 64).content("s", "file_x")).hasSize(4);
        assertThat(counted.getCount).isEqualTo(1);
    }

    /** Returns a fixed item for any read, to simulate tampered or malformed persisted records. */
    private static final class StubStore implements BaseStore {
        StoreItem item;
        RuntimeException failOnGet;
        RuntimeException failOnPut;
        int getCount;

        @Override
        public StoreItem get(List<String> namespace, String key) {
            getCount++;
            if (failOnGet != null) throw failOnGet;
            return item;
        }

        @Override
        public void put(List<String> namespace, String key, Map<String, Object> value) {}

        @Override
        public boolean putIfVersion(
                List<String> namespace, String key, Map<String, Object> value, long version) {
            if (failOnPut != null) throw failOnPut;
            return false;
        }

        @Override
        public List<StoreItem> search(List<String> namespace, int limit, int offset) {
            return List.of();
        }

        @Override
        public void delete(List<String> namespace, String key) {}
    }

    /** Blocks every read until released, to exercise the resolver timeout and cancel paths. */
    private static final class BlockingStore implements BaseStore {
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public StoreItem get(List<String> namespace, String key) {
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
            throw new AttachmentStoreException(AttachmentStoreCategory.NOT_FOUND, "released");
        }

        @Override
        public void put(List<String> namespace, String key, Map<String, Object> value) {}

        @Override
        public boolean putIfVersion(
                List<String> namespace, String key, Map<String, Object> value, long version) {
            return false;
        }

        @Override
        public List<StoreItem> search(List<String> namespace, int limit, int offset) {
            return List.of();
        }

        @Override
        public void delete(List<String> namespace, String key) {}
    }

    /** Delegates to a backing store, optionally failing every read with a secret-looking message. */
    private static final class ThrowingStore implements BaseStore {
        private final BaseStore delegate;
        boolean fail;

        ThrowingStore(BaseStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public StoreItem get(List<String> namespace, String key) {
            if (fail) throw new IllegalStateException("backend-secret at /var/data");
            return delegate.get(namespace, key);
        }

        @Override
        public void put(List<String> namespace, String key, Map<String, Object> value) {
            delegate.put(namespace, key, value);
        }

        @Override
        public boolean putIfVersion(
                List<String> namespace, String key, Map<String, Object> value, long version) {
            return delegate.putIfVersion(namespace, key, value, version);
        }

        @Override
        public List<StoreItem> search(List<String> namespace, int limit, int offset) {
            return delegate.search(namespace, limit, offset);
        }

        @Override
        public void delete(List<String> namespace, String key) {
            delegate.delete(namespace, key);
        }
    }
}
