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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.harness.agent.IsolationScope;
import org.junit.jupiter.api.Test;

class SandboxWorkspaceKeyTest {

    @Test
    void stableIdsMatchOpenSandboxRedisV1Vectors() {
        assertEquals(
                "bH6znxIoEVo6UYUWGGrZjLO75Z8OCRyv8vBuEv95jUo",
                SandboxWorkspaceKey.from(userKey("user-1", "agent-a"), "agent-a").getStableId());
        assertEquals(
                "EStM24l-_KWcHQEfeEqHUfL_-KuU9C3F8sKvW4tJ-NM",
                SandboxWorkspaceKey.from(sessionKey("session-1", "agent-a"), "agent-a")
                        .getStableId());
        assertEquals(
                "PIRvK2OPgoG2B6ZrXsRmuVvImDClzWAT_DQL4DpiThE",
                SandboxWorkspaceKey.from(agentKey("agent-a"), "agent-a").getStableId());
        assertEquals(
                "KBNSCf5KkerW-ANyNovGvWdX99LGrNtfNU7YdKj48CQ",
                SandboxWorkspaceKey.from(globalKey("agent-a"), "agent-a").getStableId());
    }

    @Test
    void userFallbackUsesEffectiveSessionScope() {
        RuntimeContext context = RuntimeContext.builder().sessionId("session-1").build();
        SandboxIsolationKey isolationKey =
                SandboxIsolationKey.resolve(IsolationScope.USER, context, "agent-a").orElseThrow();

        SandboxWorkspaceKey workspaceKey = SandboxWorkspaceKey.from(isolationKey, "agent-a");

        assertEquals(IsolationScope.SESSION, workspaceKey.getScope());
        assertEquals("EStM24l-_KWcHQEfeEqHUfL_-KuU9C3F8sKvW4tJ-NM", workspaceKey.getStableId());
    }

    @Test
    void identityComponentsAndScopesProduceDifferentStableIds() {
        assertNotEquals(
                SandboxWorkspaceKey.from(userKey("user-1", "agent-a"), "agent-a"),
                SandboxWorkspaceKey.from(userKey("user-2", "agent-a"), "agent-a"));
        assertNotEquals(
                SandboxWorkspaceKey.from(userKey("user-1", "agent-a"), "agent-a"),
                SandboxWorkspaceKey.from(userKey("user-1", "agent-b"), "agent-b"));
        assertNotEquals(
                SandboxWorkspaceKey.from(sessionKey("same", "agent-a"), "agent-a"),
                SandboxWorkspaceKey.from(userKey("same", "agent-a"), "agent-a"));
    }

    @Test
    void globalIdentityIsSharedAcrossAgentsButKeepsBorrowerForDiagnostics() {
        SandboxWorkspaceKey first = SandboxWorkspaceKey.from(globalKey("agent-a"), "agent-a");
        SandboxWorkspaceKey second = SandboxWorkspaceKey.from(globalKey("agent-b"), "agent-b");

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertEquals("agent-a", first.getAgentId());
        assertEquals("agent-b", second.getAgentId());
    }

    @Test
    void stableIdAndToStringDoNotExposeRawIdentity() {
        SandboxWorkspaceKey key =
                SandboxWorkspaceKey.from(sessionKey("secret/session:张三", "agent-a"), "agent-a");

        assertEquals(43, key.getStableId().length());
        assertFalse(key.getStableId().contains("secret"));
        assertFalse(key.toString().contains("secret"));
        assertFalse(key.toString().contains("张三"));
        assertEquals(IsolationScope.SESSION, key.getScope());
    }

    @Test
    void rejectsInvalidOrAmbiguousIdentityComponents() {
        assertThrows(NullPointerException.class, () -> SandboxWorkspaceKey.from(null, "agent-a"));
        assertThrows(
                IllegalArgumentException.class,
                () -> SandboxWorkspaceKey.from(sessionKey("session-1", "agent-a"), " "));
        assertThrows(
                IllegalArgumentException.class,
                () -> SandboxWorkspaceKey.from(sessionKey("session-1", "agent-a"), "agent\0a"));
        assertThrows(
                IllegalArgumentException.class,
                () -> SandboxWorkspaceKey.from(sessionKey("session\0one", "agent-a"), "agent-a"));
        assertThrows(
                IllegalArgumentException.class,
                () -> SandboxWorkspaceKey.from(agentKey("agent-a"), "agent-b"));
    }

    private static SandboxIsolationKey userKey(String userId, String agentId) {
        RuntimeContext context =
                RuntimeContext.builder().userId(userId).sessionId("fallback").build();
        return SandboxIsolationKey.resolve(IsolationScope.USER, context, agentId).orElseThrow();
    }

    private static SandboxIsolationKey sessionKey(String sessionId, String agentId) {
        RuntimeContext context = RuntimeContext.builder().sessionId(sessionId).build();
        return SandboxIsolationKey.resolve(IsolationScope.SESSION, context, agentId).orElseThrow();
    }

    private static SandboxIsolationKey agentKey(String agentId) {
        return SandboxIsolationKey.resolve(IsolationScope.AGENT, null, agentId).orElseThrow();
    }

    private static SandboxIsolationKey globalKey(String agentId) {
        return SandboxIsolationKey.resolve(IsolationScope.GLOBAL, null, agentId).orElseThrow();
    }
}
