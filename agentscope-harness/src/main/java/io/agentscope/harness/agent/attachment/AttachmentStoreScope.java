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

import java.util.Objects;

/**
 * Ownership scope for an {@link AttachmentStore} operation.
 *
 * <p>A store must isolate attachments by user and session: a reference registered under one scope
 * must never resolve or open under another. The harness never relaxes this on the store's behalf, so
 * an implementation that cannot prove the caller's scope must fail with {@code FORBIDDEN} rather
 * than return content.
 */
public record AttachmentStoreScope(String userId, String sessionId) {

    public AttachmentStoreScope {
        Objects.requireNonNull(userId, "userId");
        Objects.requireNonNull(sessionId, "sessionId");
        if (userId.isBlank()) throw new IllegalArgumentException("userId must not be blank");
        if (sessionId.isBlank()) throw new IllegalArgumentException("sessionId must not be blank");
    }
}
