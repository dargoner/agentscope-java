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
 * Unchecked failure from an {@link AttachmentStore} operation carrying only a coarse {@link
 * AttachmentStoreCategory}.
 *
 * <p>The {@code detail} is a short, actionable, non-sensitive string. Implementations must not put
 * raw exception messages, storage paths, signed URLs or access tokens in it.
 */
public class AttachmentStoreException extends RuntimeException {

    private final AttachmentStoreCategory category;

    public AttachmentStoreException(AttachmentStoreCategory category, String detail) {
        super(category + (detail == null || detail.isBlank() ? "" : ": " + detail));
        this.category = category == null ? AttachmentStoreCategory.FAILED : category;
    }

    /** Returns the coarse, non-sensitive failure classification. */
    public AttachmentStoreCategory category() {
        return category;
    }
}
