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
package io.agentscope.harness.agent.sandbox;

import io.agentscope.harness.agent.IsolationScope;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Objects;

/** Immutable, provider-neutral identity for a logical sandbox workspace. */
public final class SandboxWorkspaceKey {

    private static final char FIELD_SEPARATOR = '\0';

    private final IsolationScope scope;
    private final String agentId;
    private final String stableId;

    private SandboxWorkspaceKey(IsolationScope scope, String agentId, String stableId) {
        this.scope = scope;
        this.agentId = agentId;
        this.stableId = stableId;
    }

    /**
     * Derives a stable workspace identity from the effective Harness isolation key.
     *
     * @param isolationKey effective sandbox isolation key
     * @param agentId current agent identifier
     * @return immutable workspace key
     */
    public static SandboxWorkspaceKey from(SandboxIsolationKey isolationKey, String agentId) {
        Objects.requireNonNull(isolationKey, "isolationKey must not be null");
        String validAgentId = requireComponent(agentId, "agentId");
        String isolationValue = requireComponent(isolationKey.getValue(), "isolation value");
        IsolationScope scope =
                Objects.requireNonNull(isolationKey.getScope(), "isolation scope must not be null");
        if (scope == IsolationScope.AGENT && !isolationValue.equals(validAgentId)) {
            throw new IllegalArgumentException("AGENT isolation key must match agentId");
        }

        String canonical =
                switch (scope) {
                    case USER -> "v1\0user\0" + validAgentId + FIELD_SEPARATOR + isolationValue;
                    case SESSION ->
                            "v1\0session\0" + validAgentId + FIELD_SEPARATOR + isolationValue;
                    case AGENT -> "v1\0agent\0" + validAgentId;
                    case GLOBAL -> "v1\0global";
                };
        return new SandboxWorkspaceKey(scope, validAgentId, stableId(canonical));
    }

    /** Returns the effective isolation scope. */
    public IsolationScope getScope() {
        return scope;
    }

    /**
     * Returns the current agent identifier.
     *
     * <p>For GLOBAL scope this is diagnostic borrower context and is not part of identity.
     */
    public String getAgentId() {
        return agentId;
    }

    /** Returns the opaque stable workspace identifier. */
    public String getStableId() {
        return stableId;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof SandboxWorkspaceKey that && stableId.equals(that.stableId);
    }

    @Override
    public int hashCode() {
        return stableId.hashCode();
    }

    @Override
    public String toString() {
        return "SandboxWorkspaceKey{scope=" + scope + ", stableId='" + stableId + "'}";
    }

    private static String requireComponent(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        if (value.indexOf(FIELD_SEPARATOR) >= 0) {
            throw new IllegalArgumentException(name + " must not contain NUL");
        }
        return value;
    }

    private static String stableId(String canonical) {
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(canonical.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
