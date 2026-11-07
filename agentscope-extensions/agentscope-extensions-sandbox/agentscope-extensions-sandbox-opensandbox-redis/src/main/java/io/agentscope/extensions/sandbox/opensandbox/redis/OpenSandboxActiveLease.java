/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.agentscope.extensions.sandbox.opensandbox.redis;

import java.time.Instant;
import java.util.Objects;

/** Independently expiring lease for one active Turn or Subagent operation. */
public record OpenSandboxActiveLease(
        String leaseId,
        String workspaceId,
        long generation,
        String ownerInstanceId,
        Instant startedAt,
        Instant heartbeatAt) {

    public OpenSandboxActiveLease {
        requireText(leaseId, "leaseId");
        requireText(workspaceId, "workspaceId");
        requireText(ownerInstanceId, "ownerInstanceId");
        if (generation < 0) {
            throw new IllegalArgumentException("generation must not be negative");
        }
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(heartbeatAt, "heartbeatAt");
    }

    private static void requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
