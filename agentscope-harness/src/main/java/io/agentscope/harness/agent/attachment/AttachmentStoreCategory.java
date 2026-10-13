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

/**
 * Coarse, non-sensitive reason an {@link AttachmentStore} operation failed.
 *
 * <p>These categories are safe to surface to callers and logs; implementations must never include a
 * raw exception message, URL or access token in an {@link AttachmentStoreException}.
 */
public enum AttachmentStoreCategory {
    /** No attachment exists for the requested reference in scope. */
    NOT_FOUND,
    /** The attachment exists but is not visible to the caller's scope. */
    FORBIDDEN,
    /** The attachment exceeds the store's read size limit. */
    TOO_LARGE,
    /** A registration used an idempotency key already bound to different metadata. */
    CONFLICT,
    /** Any other failure; the store could not classify it more precisely. */
    FAILED
}
