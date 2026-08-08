/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.agentscope.extensions.sandbox.opensandbox.redis;

import io.agentscope.harness.agent.sandbox.SandboxIsolationKey;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Objects;

/** Cluster-aware OpenSandbox client. Lifecycle behavior is added incrementally in this module. */
public final class RedisOpenSandboxClient {
    private RedisOpenSandboxClient() {}

    static String workspaceId(SandboxIsolationKey isolationKey, String agentId) {
        Objects.requireNonNull(isolationKey, "isolationKey");
        String canonical =
                switch (isolationKey.getScope()) {
                    case USER ->
                            "v1\0user\0"
                                    + requireText(agentId, "agentId")
                                    + "\0"
                                    + requireText(isolationKey.getValue(), "isolation value");
                    case SESSION ->
                            "v1\0session\0"
                                    + requireText(agentId, "agentId")
                                    + "\0"
                                    + requireText(isolationKey.getValue(), "isolation value");
                    case AGENT -> "v1\0agent\0" + requireText(agentId, "agentId");
                    case GLOBAL -> "v1\0global";
                };
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(canonical.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
