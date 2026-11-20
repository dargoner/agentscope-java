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
 * Identity and scope for a single attachment resolution attempt.
 *
 * <p>The scope is supplied so a resolver can enforce session/user isolation and return {@link
 * ResolvedAttachment.Unavailable} with a {@code FORBIDDEN} category for out-of-scope references.
 * Fields other than the reference itself may be {@code null} for sessionless calls; the harness does
 * not enforce scope on the resolver's behalf.
 */
public record AttachmentResolveContext(
        String userId, String sessionId, String agentId, String callId) {}
