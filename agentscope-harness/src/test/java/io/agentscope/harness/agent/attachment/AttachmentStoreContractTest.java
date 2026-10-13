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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Pins the contract any {@link AttachmentStore} must satisfy, exercised against the in-memory
 * reference implementation: scope isolation, idempotent registration, conflict on key reuse,
 * structured not-found/forbidden/too-large errors and a byte round-trip.
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
        store.register(scope, registration("key-1", "image/png", "a.png", 3));

        AttachmentStoreException failure =
                assertThrows(
                        AttachmentStoreException.class,
                        () ->
                                store.register(
                                        scope, registration("key-1", "image/png", "other.png", 3)));

        assertEquals(AttachmentStoreCategory.CONFLICT, failure.category());
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

    @Test
    void sameUserDifferentSessionCannotReadTheAttachment() {
        AttachmentStoreScope owner = scope("user-1", "session-a");
        String referenceId = store.register(owner, registration("key-1", "image/png", "a.png", 3));

        assertThrows(
                AttachmentStoreException.class,
                () -> store.open(scope("user-1", "session-b"), referenceId));
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
    void registerRejectsDeclaredSizeMismatch() {
        AttachmentStoreScope scope = scope("user-1", "session-1");
        AttachmentRegistration mismatch =
                new AttachmentRegistration(
                        "key-1", "image/png", "a.png", "sha", 99, content("abc"));

        AttachmentStoreException failure =
                assertThrows(AttachmentStoreException.class, () -> store.register(scope, mismatch));

        assertNotEquals(AttachmentStoreCategory.CONFLICT, failure.category());
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
                idempotencyKey, mediaType, name, "sha-" + idempotencyKey, size, content("abc"));
    }

    private static ByteArrayInputStream content(String text) {
        return new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
    }

    private static AttachmentStoreScope scope(String userId, String sessionId) {
        return new AttachmentStoreScope(userId, sessionId);
    }
}
