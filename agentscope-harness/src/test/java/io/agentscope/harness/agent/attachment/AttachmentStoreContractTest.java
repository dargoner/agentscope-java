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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Pins the contract any {@link AttachmentStore} must satisfy, exercised against the in-memory
 * reference implementation: scope isolation, idempotent registration, conflict on key reuse,
 * structured not-found/forbidden/too-large errors and a byte round-trip. Scenarios are chosen so an
 * implementation that only checks one scope axis, only compares one metadata field, or ignores
 * concurrency cannot pass coincidentally.
 */
class AttachmentStoreContractTest {

    private final InMemoryAttachmentStore store = new InMemoryAttachmentStore();

    @Test
    void registerResolveRoundTripsMetadata() {
        AttachmentStoreScope scope = scope("user-1", "session-1");

        String referenceId = store.register(scope, registration("key-1", "image/png", "a.png", 3));

        AttachmentMetadata metadata = store.resolve(scope, referenceId);
        assertEquals(referenceId, metadata.referenceId());
        assertEquals("image/png", metadata.mediaType());
        assertEquals("a.png", metadata.name());
        assertEquals(3, metadata.size());
    }

    @Test
    void openReturnsTheRegisteredBytes() {
        AttachmentStoreScope scope = scope("user-1", "session-1");

        String referenceId = store.register(scope, registration("key-1", "image/png", "a.png", 3));

        assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), readAll(referenceId, scope));
    }

    @Test
    void registerIsIdempotentForSameKeyAndMetadata() {
        AttachmentStoreScope scope = scope("user-1", "session-1");

        String first = store.register(scope, registration("key-1", "image/png", "a.png", 3));
        String second = store.register(scope, registration("key-1", "image/png", "a.png", 3));

        assertEquals(first, second, "same idempotency key and metadata must return the same id");
    }

    @Test
    void registerConflictsWhenKeyReusedWithDifferentMetadata() {
        AttachmentStoreScope scope = scope("user-1", "session-1");
        String original = store.register(scope, registration("key-1", "image/png", "a.png", 3));

        assertConflict(scope, "key-1", "image/png", "other.png", 3);
        assertConflict(scope, "key-1", "image/jpeg", "a.png", 3);
        assertConflict(scope, "key-1", "image/png", "a.png", 4);
        assertConflictDifferentSha(scope, "key-1");

        AttachmentMetadata after = store.resolve(scope, original);
        assertEquals("a.png", after.name(), "a rejected conflict must not modify the original");
        assertEquals("image/png", after.mediaType());
        assertEquals(3, after.size());
    }

    @Test
    void resolveUnknownReferenceIsNotFound() {
        AttachmentStoreException failure =
                assertThrows(
                        AttachmentStoreException.class,
                        () -> store.resolve(scope("user-1", "session-1"), "att_missing"));

        assertEquals(AttachmentStoreCategory.NOT_FOUND, failure.category());
    }

    @Test
    void anotherScopeCannotResolveOrOpenThisSessionsAttachment() {
        AttachmentStoreScope owner = scope("owner", "owner-session");
        String referenceId = store.register(owner, registration("key-1", "image/png", "a.png", 3));
        AttachmentStoreScope intruder = scope("agent", "intruder-session");

        assertOutOfScope(store, intruder, referenceId);
    }

    @Test
    void sameUserDifferentSessionCannotReadTheAttachment() {
        AttachmentStoreScope owner = scope("user-1", "session-a");
        String referenceId = store.register(owner, registration("key-1", "image/png", "a.png", 3));

        assertOutOfScope(store, scope("user-1", "session-b"), referenceId);
    }

    @Test
    void differentUserSameSessionCannotReadTheAttachment() {
        AttachmentStoreScope owner = scope("user-1", "session-1");
        String referenceId = store.register(owner, registration("key-1", "image/png", "a.png", 3));

        assertOutOfScope(store, scope("user-2", "session-1"), referenceId);
    }

    @Test
    void delimiterCharactersInScopeDoNotCollide() {
        // userId "a\0b" + session "c" and userId "a" + session "b\0c" must stay distinct scopes.
        AttachmentStoreScope left = scope("a\u0000b", "c");
        AttachmentStoreScope right = scope("a", "b\u0000c");

        String leftId = store.register(left, registration("key-1", "image/png", "a.png", 3));
        String rightId = store.register(right, registration("key-1", "image/png", "a.png", 3));

        assertNotEquals(leftId, rightId, "scopes with different field boundaries must not collide");
        assertEquals(leftId, store.resolve(left, leftId).referenceId());
        assertEquals(rightId, store.resolve(right, rightId).referenceId());
    }

    @Test
    void concurrentSameKeyRegistrationIsIdempotent() throws Exception {
        AttachmentStoreScope scope = scope("user-1", "session-1");
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<String>> results = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                results.add(
                        pool.submit(
                                () -> {
                                    start.await();
                                    return store.register(
                                            scope, registration("key-1", "image/png", "a.png", 3));
                                }));
            }
            start.countDown();
            Set<String> ids = new java.util.HashSet<>();
            for (Future<String> result : results) {
                ids.add(result.get(10, TimeUnit.SECONDS));
            }
            assertEquals(1, ids.size(), "concurrent same-key registration must yield one id");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void concurrentConflictingRegistrationLeavesOneConsistentWinner() throws Exception {
        AttachmentStoreScope scope = scope("user-1", "session-1");
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch start = new CountDownLatch(1);
            Future<String> a =
                    pool.submit(
                            () -> {
                                start.await();
                                return store.register(
                                        scope, registration("key-1", "image/png", "a.png", 3));
                            });
            Future<String> b =
                    pool.submit(
                            () -> {
                                start.await();
                                return store.register(
                                        scope, registration("key-1", "image/png", "b.png", 3));
                            });
            start.countDown();

            List<Object> outcomes = new ArrayList<>();
            for (Future<String> f : List.of(a, b)) {
                try {
                    outcomes.add(f.get(10, TimeUnit.SECONDS));
                } catch (java.util.concurrent.ExecutionException e) {
                    outcomes.add(e.getCause());
                }
            }
            long conflictCount =
                    outcomes.stream()
                            .filter(
                                    o ->
                                            o instanceof AttachmentStoreException ex
                                                    && ex.category()
                                                            == AttachmentStoreCategory.CONFLICT)
                            .count();
            long idCount = outcomes.stream().filter(o -> o instanceof String).count();
            assertEquals(
                    2,
                    conflictCount + idCount,
                    "each racer must either win the id or see CONFLICT, never another error");
            assertTrue(idCount >= 1, "at least one racer must succeed");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void openOverReadLimitIsTooLarge() {
        InMemoryAttachmentStore bounded = new InMemoryAttachmentStore(2);
        AttachmentStoreScope scope = scope("user-1", "session-1");
        String referenceId =
                bounded.register(scope, registration("key-1", "image/png", "a.png", 3));

        AttachmentStoreException failure =
                assertThrows(
                        AttachmentStoreException.class, () -> bounded.open(scope, referenceId));

        assertEquals(AttachmentStoreCategory.TOO_LARGE, failure.category());
    }

    @Test
    void registerRejectsContentLargerThanDeclared() {
        AttachmentStoreScope scope = scope("user-1", "session-1");
        AttachmentRegistration oversized =
                new AttachmentRegistration(
                        "key-1", "image/png", "a.png", "sha", 3, content("abcdef"));

        AttachmentStoreException failure =
                assertThrows(
                        AttachmentStoreException.class, () -> store.register(scope, oversized));

        assertEquals(AttachmentStoreCategory.FAILED, failure.category());
    }

    @Test
    void registerRejectsContentShorterThanDeclaredWithoutConsumingTheKey() {
        AttachmentStoreScope scope = scope("user-1", "session-1");
        AttachmentRegistration shortContent =
                new AttachmentRegistration(
                        "key-1", "image/png", "a.png", "sha", 99, content("abc"));

        AttachmentStoreException failure =
                assertThrows(
                        AttachmentStoreException.class, () -> store.register(scope, shortContent));
        assertEquals(AttachmentStoreCategory.FAILED, failure.category());

        String referenceId = store.register(scope, registration("key-1", "image/png", "a.png", 3));
        assertEquals("a.png", store.resolve(scope, referenceId).name());
    }

    @Test
    void registerSanitizesContentReadFailure() {
        AttachmentStoreScope scope = scope("user-1", "session-1");
        String sensitive = "https://store.example.com/blob?token=s3cr3t";
        InputStream failing =
                new InputStream() {
                    @Override
                    public int read() throws IOException {
                        throw new IOException(sensitive);
                    }

                    @Override
                    public int read(byte[] b, int off, int len) throws IOException {
                        throw new IOException(sensitive);
                    }
                };
        AttachmentRegistration registration =
                new AttachmentRegistration("key-1", "image/png", "a.png", "sha", 3, failing);

        AttachmentStoreException failure =
                assertThrows(
                        AttachmentStoreException.class, () -> store.register(scope, registration));

        assertEquals(AttachmentStoreCategory.FAILED, failure.category());
        assertFalse(
                String.valueOf(failure.getMessage()).contains("s3cr3t"),
                "raw content-read failure details must not leak");
    }

    private void assertConflict(
            AttachmentStoreScope scope, String key, String mediaType, String name, int size) {
        AttachmentStoreException failure =
                assertThrows(
                        AttachmentStoreException.class,
                        () -> store.register(scope, registration(key, mediaType, name, size)));
        assertEquals(AttachmentStoreCategory.CONFLICT, failure.category());
    }

    private void assertConflictDifferentSha(AttachmentStoreScope scope, String key) {
        AttachmentRegistration other =
                new AttachmentRegistration(
                        key, "image/png", "a.png", "different-sha", 3, content("abc"));
        AttachmentStoreException failure =
                assertThrows(AttachmentStoreException.class, () -> store.register(scope, other));
        assertEquals(AttachmentStoreCategory.CONFLICT, failure.category());
    }

    private static void assertOutOfScope(
            InMemoryAttachmentStore store, AttachmentStoreScope intruder, String referenceId) {
        AttachmentStoreException onResolve =
                assertThrows(
                        AttachmentStoreException.class, () -> store.resolve(intruder, referenceId));
        assertTrue(
                Set.of(AttachmentStoreCategory.FORBIDDEN, AttachmentStoreCategory.NOT_FOUND)
                        .contains(onResolve.category()),
                "cross-scope resolve must not yield metadata");

        AttachmentStoreException onOpen =
                assertThrows(
                        AttachmentStoreException.class, () -> store.open(intruder, referenceId));
        assertTrue(
                Set.of(AttachmentStoreCategory.FORBIDDEN, AttachmentStoreCategory.NOT_FOUND)
                        .contains(onOpen.category()),
                "cross-scope open must not yield content");
    }

    private byte[] readAll(String referenceId, AttachmentStoreScope scope) {
        try (var in = store.open(scope, referenceId)) {
            return in.readAllBytes();
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static AttachmentRegistration registration(
            String idempotencyKey, String mediaType, String name, int size) {
        return new AttachmentRegistration(
                idempotencyKey,
                mediaType,
                name,
                "sha-" + idempotencyKey,
                size,
                content("abcdefghijklmnopqrstuvwxyz".substring(0, size)));
    }

    private static ByteArrayInputStream content(String text) {
        return new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
    }

    private static AttachmentStoreScope scope(String userId, String sessionId) {
        return new AttachmentStoreScope(userId, sessionId);
    }
}
