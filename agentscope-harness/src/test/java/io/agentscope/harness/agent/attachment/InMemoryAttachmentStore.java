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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * In-memory {@link AttachmentStore} reference implementation used by the contract test.
 *
 * <p>It is intentionally small and fully scope-enforced: a reference is keyed globally by id but
 * visible only within the {@link AttachmentStoreScope} that registered it. All operations are
 * synchronized so concurrent callers share one atomic idempotency guarantee, and it verifies the
 * declared size while reading (never buffering more than the declared bytes) and enforces a
 * configurable read limit on {@link #open}.
 */
final class InMemoryAttachmentStore implements AttachmentStore {

    /** Upper bound on what a reference implementation is willing to buffer for one register. */
    private static final long REGISTER_CAP_BYTES = 64L * 1024 * 1024;

    private record Entry(AttachmentStoreScope scope, AttachmentMetadata metadata, byte[] content) {}

    private record RegistrationKey(String userId, String sessionId, String idempotencyKey) {
        static RegistrationKey of(AttachmentStoreScope scope, String idempotencyKey) {
            return new RegistrationKey(scope.userId(), scope.sessionId(), idempotencyKey);
        }
    }

    private final long maxBytes;
    private final Map<String, Entry> byReferenceId = new HashMap<>();
    private final Map<RegistrationKey, String> referenceByKey = new HashMap<>();
    private int sequence;

    InMemoryAttachmentStore() {
        this(Long.MAX_VALUE);
    }

    InMemoryAttachmentStore(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    @Override
    public synchronized String register(
            AttachmentStoreScope scope, AttachmentRegistration registration) {
        Objects.requireNonNull(scope, "scope");
        byte[] content = readDeclaredSize(registration.content(), registration.size());
        RegistrationKey key = RegistrationKey.of(scope, registration.idempotencyKey());
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
        byReferenceId.put(referenceId, new Entry(scope, metadata, content));
        referenceByKey.put(key, referenceId);
        return referenceId;
    }

    @Override
    public synchronized AttachmentMetadata resolve(AttachmentStoreScope scope, String referenceId) {
        return visibleEntry(scope, referenceId).metadata();
    }

    @Override
    public synchronized InputStream open(AttachmentStoreScope scope, String referenceId) {
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

    private static byte[] readDeclaredSize(InputStream in, long declaredSize) {
        // Reject an over-capacity declaration before allocating anything, then read at most
        // declaredSize + 1 bytes growing the buffer only by what is actually read. This bounds
        // memory to real content and rejects oversized/unbounded streams without buffering them
        // in full; raw exception details are never surfaced.
        if (declaredSize < 0) {
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.FAILED, "declared size is negative");
        }
        if (declaredSize > REGISTER_CAP_BYTES) {
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.TOO_LARGE,
                    "declared size exceeds registration capacity");
        }
        int limit = (int) declaredSize;
        try {
            ByteArrayOutputStream sink = new ByteArrayOutputStream(Math.min(limit, 8192));
            byte[] chunk = new byte[8192];
            int total = 0;
            while (total <= limit) {
                int want = Math.min(chunk.length, limit + 1 - total);
                int n = in.read(chunk, 0, want);
                if (n < 0) break;
                if (n == 0) continue;
                sink.write(chunk, 0, n);
                total += n;
            }
            if (total != limit) {
                throw new AttachmentStoreException(
                        AttachmentStoreCategory.FAILED, "declared size does not match content");
            }
            return sink.toByteArray();
        } catch (IOException e) {
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.FAILED, "content read failed");
        }
    }
}
