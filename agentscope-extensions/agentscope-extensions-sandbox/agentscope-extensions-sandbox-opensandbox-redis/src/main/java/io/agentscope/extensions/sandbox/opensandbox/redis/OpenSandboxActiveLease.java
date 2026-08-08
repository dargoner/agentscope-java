/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
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
