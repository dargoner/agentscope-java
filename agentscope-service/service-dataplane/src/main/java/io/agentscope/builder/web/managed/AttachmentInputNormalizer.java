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

import io.agentscope.core.util.JsonUtils;
import io.agentscope.harness.agent.attachment.AttachmentReference;
import io.agentscope.harness.agent.attachment.AttachmentRegistration;
import io.agentscope.harness.agent.attachment.AttachmentStore;
import io.agentscope.harness.agent.attachment.AttachmentStoreScope;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Turns {@code type=file} input blocks into stored attachment references at the admission boundary.
 *
 * <p>The canonical request never carries file bytes: each {@code file} block is registered with the
 * session/user-scoped {@link AttachmentStore} and replaced in place by a text placeholder, while the
 * message gains an {@link AttachmentReference#METADATA_KEY} metadata list the model boundary compiler
 * later expands. Inline media blocks submitted directly are left untouched.
 */
@Component
public final class AttachmentInputNormalizer {

    private final SessionFileService files;
    private final AttachmentStore store;

    public AttachmentInputNormalizer(SessionFileService files, AttachmentStore store) {
        this.files = files;
        this.store = store;
    }

    /** Returns a copy of the normalized request with every file block registered as a reference. */
    @SuppressWarnings("unchecked")
    public Map<String, Object> normalize(String user, String session, Map<String, Object> request) {
        if (!request.containsKey("input")) return request;
        AttachmentStoreScope scope = new AttachmentStoreScope(user, session);
        var copy =
                new LinkedHashMap<String, Object>(
                        JsonUtils.getJsonCodec().convertValue(request, Map.class));
        var messages = new ArrayList<Map<String, Object>>();
        for (var rawMessage : (List<?>) request.get("input")) {
            var message = new LinkedHashMap<>((Map<String, Object>) rawMessage);
            var content = new ArrayList<Map<String, Object>>();
            var references = new ArrayList<Map<String, Object>>();
            for (var rawBlock : (List<?>) message.get("content")) {
                var block = (Map<String, Object>) rawBlock;
                if (!"file".equals(block.get("type"))) {
                    content.add(block);
                    continue;
                }
                String fileId = String.valueOf(block.get("file_id"));
                Map<String, Object> file = files.get(session, fileId);
                String mediaType = (String) file.get("media_type");
                String name = (String) file.get("name");
                String sha256 = (String) file.get("sha256");
                long size = ((Number) file.get("size")).longValue();
                byte[] bytes = files.content(session, fileId);
                String referenceId =
                        store.register(
                                scope,
                                new AttachmentRegistration(
                                        "file:" + fileId,
                                        mediaType,
                                        name,
                                        sha256,
                                        size,
                                        new ByteArrayInputStream(bytes)));
                int ordinal = content.size();
                content.add(Map.of("type", "text", "text", "[attachment id=" + referenceId + "]"));
                references.add(
                        new AttachmentReference(referenceId, mediaType, name, sha256, size, ordinal)
                                .toMetadata());
            }
            message.put("content", content);
            if (!references.isEmpty()) message.put("metadata", mergeMetadata(message, references));
            messages.add(message);
        }
        copy.put("input", messages);
        return copy;
    }

    // Preserves any unrelated message metadata and appends to an existing attachment reference list
    // instead of replacing it, so an internal caller that already carries references is not lost.
    @SuppressWarnings("unchecked")
    private static Map<String, Object> mergeMetadata(
            Map<String, Object> message, List<Map<String, Object>> references) {
        var metadata = new LinkedHashMap<String, Object>();
        if (message.get("metadata") instanceof Map<?, ?> existing)
            metadata.putAll((Map<String, Object>) existing);
        var merged = new ArrayList<Object>();
        if (metadata.get(AttachmentReference.METADATA_KEY) instanceof List<?> prior)
            merged.addAll(prior);
        merged.addAll(references);
        metadata.put(AttachmentReference.METADATA_KEY, merged);
        return metadata;
    }
}
