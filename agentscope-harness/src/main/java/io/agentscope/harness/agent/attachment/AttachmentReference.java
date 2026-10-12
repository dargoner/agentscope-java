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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Opaque handle to an attachment stored outside the conversation history.
 *
 * <p>The canonical history never carries file bytes. A message references an attachment by id and
 * records the block ordinal it should be compiled back into at the model boundary. The identifier is
 * intentionally opaque to the harness; only the {@link AttachmentResolver} knows how to turn it back
 * into model-readable content.
 */
public record AttachmentReference(
        String referenceId,
        String mediaType,
        String name,
        String sha256,
        long size,
        int blockOrdinal) {

    /** Fixed message metadata key holding the list of attachment references for a message. */
    public static final String METADATA_KEY = "agentscope.attachments";

    private static final String KEY_BLOCK_ORDINAL = "blockOrdinal";
    private static final String KEY_REFERENCE_ID = "referenceId";
    private static final String KEY_MEDIA_TYPE = "mediaType";
    private static final String KEY_NAME = "name";
    private static final String KEY_SHA256 = "sha256";
    private static final String KEY_SIZE = "size";

    public AttachmentReference {
        Objects.requireNonNull(referenceId, "referenceId");
        if (referenceId.isBlank())
            throw new IllegalArgumentException("referenceId must not be blank");
        if (blockOrdinal < 0)
            throw new IllegalArgumentException("blockOrdinal must not be negative");
        if (size < 0) throw new IllegalArgumentException("size must not be negative");
    }

    /** Serializes this reference into the fixed metadata map shape used by persisted messages. */
    public Map<String, Object> toMetadata() {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put(KEY_BLOCK_ORDINAL, blockOrdinal);
        metadata.put(KEY_REFERENCE_ID, referenceId);
        metadata.put(KEY_MEDIA_TYPE, mediaType);
        metadata.put(KEY_NAME, name);
        metadata.put(KEY_SHA256, sha256);
        metadata.put(KEY_SIZE, size);
        return metadata;
    }

    /** Reads a reference back from the fixed metadata map shape. */
    public static AttachmentReference fromMetadata(Map<String, Object> metadata) {
        if (metadata == null) throw new IllegalArgumentException("attachment metadata is missing");
        Object ordinal = metadata.get(KEY_BLOCK_ORDINAL);
        Object id = metadata.get(KEY_REFERENCE_ID);
        if (!(id instanceof String referenceId) || referenceId.isBlank())
            throw new IllegalArgumentException("attachment referenceId is missing");
        if (!(ordinal instanceof Number number))
            throw new IllegalArgumentException("attachment blockOrdinal is missing");
        Object size = metadata.get(KEY_SIZE);
        return new AttachmentReference(
                referenceId,
                asString(metadata.get(KEY_MEDIA_TYPE)),
                asString(metadata.get(KEY_NAME)),
                asString(metadata.get(KEY_SHA256)),
                size instanceof Number value ? value.longValue() : 0L,
                number.intValue());
    }

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }
}
