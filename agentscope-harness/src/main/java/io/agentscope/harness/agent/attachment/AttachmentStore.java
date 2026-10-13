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

/**
 * Synchronous, scope-enforced storage for attachment blobs referenced from conversation history.
 *
 * <p>The canonical history never carries file bytes: a message holds an {@link AttachmentReference}
 * (id, media type, hash, size, block ordinal) and the bytes live in a store the embedding product
 * owns. The harness only defines this SPI so a product adapter and an {@link AttachmentResolver} can
 * agree on how a reference is registered, described and read back. The default wiring leaves it
 * unset; the harness never assumes a store exists.
 *
 * <p>This SPI is blocking by design: a product runs it on a bounded-elastic scheduler rather than on
 * a Reactor event-loop thread. Implementations must be safe to call from such a worker.
 *
 * <p>Contract:
 *
 * <ul>
 *   <li>all three operations are isolated by {@link AttachmentStoreScope}: a reference registered
 *       under one user/session must never resolve or open under another;
 *   <li>{@link #register} is idempotent on {@code (scope, idempotencyKey)} with matching metadata
 *       and returns the same reference id; a different metadata set for the same key fails with
 *       {@link AttachmentStoreCategory#CONFLICT};
 *   <li>failures surface as an {@link AttachmentStoreException} with a coarse {@link
 *       AttachmentStoreCategory}; raw exception messages, storage paths, signed URLs and access
 *       tokens must not appear in the detail or any log;
 *   <li>{@link #open} returns a stream bounded by the store's size limit and never buffers a blob
 *       that exceeds it.
 * </ul>
 */
public interface AttachmentStore {

    /**
     * Registers a blob under the caller's scope and returns its stable opaque reference id.
     *
     * @return the reference id to embed in an {@link AttachmentReference}
     * @throws AttachmentStoreException with {@link AttachmentStoreCategory#CONFLICT} when the
     *     idempotency key is already bound to different metadata, or {@code FAILED} on other errors
     */
    String register(AttachmentStoreScope scope, AttachmentRegistration registration);

    /**
     * Returns scope-checked metadata for a reference id.
     *
     * @throws AttachmentStoreException with {@code NOT_FOUND} or {@code FORBIDDEN}
     */
    AttachmentMetadata resolve(AttachmentStoreScope scope, String referenceId);

    /**
     * Opens a size-bounded stream for a reference id.
     *
     * @throws AttachmentStoreException with {@code NOT_FOUND}, {@code FORBIDDEN} or {@code TOO_LARGE}
     */
    InputStream open(AttachmentStoreScope scope, String referenceId);
}
