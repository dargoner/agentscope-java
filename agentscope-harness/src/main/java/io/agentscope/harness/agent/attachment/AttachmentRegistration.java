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

import java.io.InputStream;
import java.util.Objects;

/**
 * A request to store one attachment blob under an idempotency key.
 *
 * <p>{@code idempotencyKey} is scoped to the caller's {@link AttachmentStoreScope}: registering the
 * same key with identical metadata must return the existing reference id, while a different
 * metadata set for the same key is a {@link AttachmentStoreCategory#CONFLICT}. The {@code content}
 * stream is consumed once by {@link AttachmentStore#register}; callers must not reuse it afterward.
 * The {@code sha256} and {@code size} are supplied so a store can verify the blob without buffering
 * it whole.
 */
public record AttachmentRegistration(
        String idempotencyKey,
        String mediaType,
        String name,
        String sha256,
        long size,
        InputStream content) {

    public AttachmentRegistration {
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        Objects.requireNonNull(content, "content");
        if (idempotencyKey.isBlank())
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        if (size < 0) throw new IllegalArgumentException("size must not be negative");
    }
}
