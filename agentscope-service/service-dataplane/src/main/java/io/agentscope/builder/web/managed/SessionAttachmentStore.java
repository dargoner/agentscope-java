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

import io.agentscope.core.session.JournalSessionLog;
import io.agentscope.harness.agent.attachment.AttachmentMetadata;
import io.agentscope.harness.agent.attachment.AttachmentRegistration;
import io.agentscope.harness.agent.attachment.AttachmentStore;
import io.agentscope.harness.agent.attachment.AttachmentStoreCategory;
import io.agentscope.harness.agent.attachment.AttachmentStoreException;
import io.agentscope.harness.agent.attachment.AttachmentStoreScope;
import io.agentscope.harness.agent.filesystem.remote.store.BaseStore;
import io.agentscope.harness.agent.filesystem.remote.store.StoreItem;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Session/user-scoped {@link AttachmentStore} backed by the shared {@link BaseStore}.
 *
 * <p>References live in a dedicated namespace keyed by both user and session, so an attachment
 * registered under one scope is never visible under another without a reverse lookup of the session
 * owner. Each registration persists its own bounded blob, so history replay needs neither the
 * original upload store nor an in-process registry. The reference id is derived deterministically
 * from the scope and idempotency key, so re-registering the same source file returns the same id.
 *
 * <p>Namespace and reference-id components are encoded losslessly over UTF-16 code units, so
 * distinct — even malformed-Unicode — scopes can never collide on a backend that joins namespace
 * components with an in-band separator. All operational failures are surfaced as sanitized {@link
 * AttachmentStoreException}s; no raw backend message or cause is propagated, and cancellation is
 * never swallowed.
 */
@Service
public final class SessionAttachmentStore implements AttachmentStore {

    private final BaseStore store;
    private final int maxBytes;

    public SessionAttachmentStore(
            BaseStore store, @Value("${builder.agent-api.files.max-bytes:16777216}") int maxBytes) {
        this.store = store;
        this.maxBytes = maxBytes;
    }

    private static List<String> namespace(AttachmentStoreScope scope) {
        return List.of(
                "runtime", "agent-api", "attachments", hex(scope.userId()), hex(scope.sessionId()));
    }

    @Override
    public String register(AttachmentStoreScope scope, AttachmentRegistration registration) {
        Objects.requireNonNull(scope, "scope");
        byte[] content = readDeclaredSize(registration.content(), registration.size(), maxBytes);
        String referenceId = referenceId(scope, registration.idempotencyKey());
        var record = new LinkedHashMap<String, Object>();
        record.put("reference_id", referenceId);
        record.put("media_type", registration.mediaType());
        record.put("name", registration.name());
        record.put("sha256", registration.sha256());
        record.put("size", registration.size());
        record.put("created_at", Instant.now().toString());
        record.put("data", Base64.getEncoder().encodeToString(content));

        boolean written;
        try {
            written = store.putIfVersion(namespace(scope), referenceId, record, 0);
        } catch (CancellationException cancelled) {
            throw cancelled;
        } catch (RuntimeException error) {
            throw new AttachmentStoreException(AttachmentStoreCategory.FAILED, "register failed");
        }
        if (written) return referenceId;

        Map<String, Object> existing;
        try {
            StoreItem item = store.get(namespace(scope), referenceId);
            existing = item == null ? null : item.value();
        } catch (CancellationException cancelled) {
            throw cancelled;
        } catch (RuntimeException error) {
            throw new AttachmentStoreException(AttachmentStoreCategory.FAILED, "register failed");
        }
        if (existing == null)
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.FAILED, "store does not support atomic writes");
        if (sameMetadata(existing, registration)) return referenceId;
        throw new AttachmentStoreException(
                AttachmentStoreCategory.CONFLICT, "idempotency key reused with new metadata");
    }

    @Override
    public AttachmentMetadata resolve(AttachmentStoreScope scope, String referenceId) {
        return metadata(raw(scope, referenceId));
    }

    @Override
    public InputStream open(AttachmentStoreScope scope, String referenceId) {
        Map<String, Object> record = raw(scope, referenceId);
        long size = sizeOf(record);
        String encoded = dataOf(record);
        if (size > maxBytes || encoded.length() > maxEncodedLength(maxBytes))
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.TOO_LARGE, "attachment exceeds read limit");
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(encoded);
        } catch (RuntimeException error) {
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.FAILED, "corrupt attachment record");
        }
        if (bytes.length > maxBytes)
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.TOO_LARGE, "attachment exceeds read limit");
        return new ByteArrayInputStream(bytes);
    }

    private Map<String, Object> raw(AttachmentStoreScope scope, String referenceId) {
        StoreItem item;
        try {
            item = store.get(namespace(scope), referenceId);
        } catch (CancellationException cancelled) {
            throw cancelled;
        } catch (RuntimeException error) {
            throw new AttachmentStoreException(AttachmentStoreCategory.FAILED, "store read failed");
        }
        if (item == null)
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.NOT_FOUND, "unknown reference");
        Map<String, Object> value = item.value();
        if (value == null)
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.FAILED, "corrupt attachment record");
        return value;
    }

    private static AttachmentMetadata metadata(Map<String, Object> record) {
        try {
            return new AttachmentMetadata(
                    (String) record.get("reference_id"),
                    (String) record.get("media_type"),
                    (String) record.get("name"),
                    (String) record.get("sha256"),
                    sizeOf(record),
                    Instant.parse((String) record.get("created_at")));
        } catch (AttachmentStoreException error) {
            throw error;
        } catch (RuntimeException error) {
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.FAILED, "corrupt attachment record");
        }
    }

    private static long sizeOf(Map<String, Object> record) {
        Object size = record.get("size");
        if (!(size instanceof Number number))
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.FAILED, "corrupt attachment record");
        return number.longValue();
    }

    private static String dataOf(Map<String, Object> record) {
        Object data = record.get("data");
        if (!(data instanceof String text))
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.FAILED, "corrupt attachment record");
        return text;
    }

    private static boolean sameMetadata(
            Map<String, Object> existing, AttachmentRegistration registration) {
        return Objects.equals(existing.get("media_type"), registration.mediaType())
                && Objects.equals(existing.get("name"), registration.name())
                && Objects.equals(existing.get("sha256"), registration.sha256())
                && sizeOf(existing) == registration.size();
    }

    private static String referenceId(AttachmentStoreScope scope, String idempotencyKey) {
        return "att_" + JournalSessionLog.hash(encodeScope(scope, idempotencyKey));
    }

    // Hex over UTF-16 code units is injective, so the '\u0001' separator can never be forged and
    // distinct (user, session, key) triples never collide — even with unpaired surrogates.
    private static byte[] encodeScope(AttachmentStoreScope scope, String idempotencyKey) {
        String encoded =
                hex(scope.userId())
                        + '\u0001'
                        + hex(scope.sessionId())
                        + '\u0001'
                        + hex(idempotencyKey);
        return encoded.getBytes(StandardCharsets.US_ASCII);
    }

    private static String hex(String value) {
        StringBuilder hex = new StringBuilder(value.length() * 4);
        for (int i = 0; i < value.length(); i++) {
            int unit = value.charAt(i);
            hex.append(Character.forDigit((unit >> 12) & 0xF, 16));
            hex.append(Character.forDigit((unit >> 8) & 0xF, 16));
            hex.append(Character.forDigit((unit >> 4) & 0xF, 16));
            hex.append(Character.forDigit(unit & 0xF, 16));
        }
        return hex.toString();
    }

    private static long maxEncodedLength(int maxBytes) {
        return 4L * ((maxBytes + 2L) / 3L);
    }

    private static byte[] readDeclaredSize(InputStream in, long declaredSize, int cap) {
        if (declaredSize < 0)
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.FAILED, "declared size is negative");
        if (declaredSize > cap)
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.TOO_LARGE, "declared size exceeds capacity");
        int limit = (int) declaredSize;
        ByteArrayOutputStream sink = new ByteArrayOutputStream(Math.min(limit, 8192));
        byte[] chunk = new byte[8192];
        int total = 0;
        try {
            while (total <= limit) {
                int want = Math.min(chunk.length, limit + 1 - total);
                int n = in.read(chunk, 0, want);
                if (n < 0) break;
                if (n == 0) continue;
                sink.write(chunk, 0, n);
                total += n;
            }
        } catch (CancellationException cancelled) {
            throw cancelled;
        } catch (IOException | RuntimeException error) {
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.FAILED, "content read failed");
        }
        if (total != limit)
            throw new AttachmentStoreException(
                    AttachmentStoreCategory.FAILED, "declared size does not match content");
        return sink.toByteArray();
    }
}
