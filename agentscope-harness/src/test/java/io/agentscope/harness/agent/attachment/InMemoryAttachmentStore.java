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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * In-memory {@link AttachmentStore} reference implementation used by the contract test.
 *
 * <p>It is intentionally small and fully scope-enforced: a reference is keyed globally by id but
 * visible only within the {@link AttachmentStoreScope} that registered it. It verifies the declared
 * size against the blob and enforces a configurable read limit on {@link #open}.
 */
final class InMemoryAttachmentStore implements AttachmentStore {

    private record Entry(
            AttachmentStoreScope scope,
            String idempotencyKey,
            AttachmentMetadata metadata,
            byte[] content) {}

    private final long maxBytes;
    private final Map<String, Entry> byReferenceId = new HashMap<>();
    private final Map<String, String> referenceByKey = new HashMap<>();
    private int sequence;

    InMemoryAttachmentStore() {
        this(Long.MAX_VALUE);
    }

    InMemoryAttachmentStore(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    @Override
    public String register(AttachmentStoreScope scope, AttachmentRegistration registration) {
        Objects.requireNonNull(scope, "scope");
        byte[] content = readAll(registration.content());
        if (content.length != registration.size()) {
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.FAILED, "declared size does not match content");
        }
        String key = key(scope, registration.idempotencyKey());
        String existingId = referenceByKey.get(key);
        if (existingId != null) {
            AttachmentMetadata existing = byReferenceId.get(existingId).metadata();
            if (sameMetadata(existing, registration)) return existingId;
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.CONFLICT, "idempotency key reused with new metadata");
        }
        String referenceId = "att_" + (++sequence);
        AttachmentMetadata metadata =
                new AttachmentMetadata(
                        referenceId,
                        registration.mediaType(),
                        registration.name(),
                        registration.sha256(),
                        registration.size(),
                        Instant.now());
        byReferenceId.put(
                referenceId, new Entry(scope, registration.idempotencyKey(), metadata, content));
        referenceByKey.put(key, referenceId);
        return referenceId;
    }

    @Override
    public AttachmentMetadata resolve(AttachmentStoreScope scope, String referenceId) {
        return visibleEntry(scope, referenceId).metadata();
    }

    @Override
    public InputStream open(AttachmentStoreScope scope, String referenceId) {
        Entry entry = visibleEntry(scope, referenceId);
        if (entry.content().length > maxBytes) {
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.TOO_LARGE, "attachment exceeds read limit");
        }
        return new ByteArrayInputStream(entry.content());
    }

    private Entry visibleEntry(AttachmentStoreScope scope, String referenceId) {
        Entry entry = byReferenceId.get(referenceId);
        if (entry == null) {
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.NOT_FOUND, "unknown reference");
        }
        if (!entry.scope().equals(scope)) {
            throw new AttachmentStoreException(AttachmentStoreCategory.FORBIDDEN, "out of scope");
        }
        return entry;
    }

    private static boolean sameMetadata(
            AttachmentMetadata existing, AttachmentRegistration registration) {
        return Objects.equals(existing.mediaType(), registration.mediaType())
                && Objects.equals(existing.name(), registration.name())
                && Objects.equals(existing.sha256(), registration.sha256())
                && existing.size() == registration.size();
    }

    private static String key(AttachmentStoreScope scope, String idempotencyKey) {
        return scope.userId() + "\u0000" + scope.sessionId() + "\u0000" + idempotencyKey;
    }

    private static byte[] readAll(InputStream in) {
        try {
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
