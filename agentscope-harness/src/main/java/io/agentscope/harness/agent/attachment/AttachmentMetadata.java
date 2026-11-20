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

import java.time.Instant;

/**
 * Metadata of a stored attachment, returned by a scope-checked {@link AttachmentStore#resolve}.
 *
 * <p>This carries only descriptors, never the blob itself; bytes are read lazily through {@link
 * AttachmentStore#open}.
 */
public record AttachmentMetadata(
        String referenceId,
        String mediaType,
        String name,
        String sha256,
        long size,
        Instant createdAt) {}
