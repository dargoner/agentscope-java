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

import reactor.core.publisher.Mono;

/**
 * Resolves a historical {@link AttachmentReference} into content the model boundary can compile.
 *
 * <p>Implementations are owned by the embedding product: they perform permission and hash checks,
 * enforce timeout/size/MIME limits and return only the three core-expressible outcomes of {@link
 * ResolvedAttachment}. The harness never assumes a product store exists, so the default wiring keeps
 * this optional and unset.
 *
 * <p>Contract:
 *
 * <ul>
 *   <li>the same {@link AttachmentResolveContext} must resolve idempotently;
 *   <li>reactor cancellation must propagate (do not swallow {@code CancellationException});
 *   <li>access tokens, internal URLs and raw exception messages must not appear in returned
 *       values or logs;
 *   <li>a remote URL may only be returned when the provider adapter explicitly supports fetching
 *       it; application-side reachability checks are not proof the model can fetch it.
 * </ul>
 */
@FunctionalInterface
public interface AttachmentResolver {
    Mono<ResolvedAttachment> resolve(
            AttachmentReference reference, AttachmentResolveContext context);
}
