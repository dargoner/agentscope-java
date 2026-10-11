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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.state.InMemoryAgentStateStore;
import io.agentscope.core.state.State;
import io.agentscope.harness.agent.IsolationScope;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SessionSandboxStateStoreTest {

    private static final String AGENT_ID = "test-agent";
    private static final String JSON = "{\"sessionId\":\"s-1\"}";

    private SessionSandboxStateStore store;

    @BeforeEach
    void setUp() {
        store = new SessionSandboxStateStore(new InMemoryAgentStateStore(), AGENT_ID);
    }

    @Test
    void sessionScope_roundTrip() throws Exception {
        SandboxIsolationKey key = isolationKey(IsolationScope.SESSION, "sess-001");
        assertFalse(store.load(key).isPresent());

        store.save(key, JSON);
        assertEquals(JSON, store.load(key).orElseThrow());

        store.delete(key);
        assertFalse(store.load(key).isPresent());
    }

    @Test
    void sessionScopeSeparatesAgentsSharingTheSameSession() throws Exception {
        InMemoryAgentStateStore backend = new InMemoryAgentStateStore();
        SessionSandboxStateStore first = new SessionSandboxStateStore(backend, "agent-a");
        SessionSandboxStateStore second = new SessionSandboxStateStore(backend, "agent-b");
        SandboxIsolationKey key = sessionKey("shared-session", "agent-a");

        first.save(key, "first");

        assertEquals("first", first.load(key).orElseThrow());
        assertFalse(second.load(key).isPresent());

        second.save(key, "second");
        assertEquals("first", first.load(key).orElseThrow());
        assertEquals("second", second.load(key).orElseThrow());
    }

    @Test
    void sessionScopeUsesBoundedPathSafeOpaqueSlotForArbitrarySessionIds() throws Exception {
        RecordingStore backend = new RecordingStore();
        SessionSandboxStateStore recordingStore = new SessionSandboxStateStore(backend, AGENT_ID);
        String rawSessionId = "prefix:with/path\\and-张三-" + "x".repeat(1000);

        recordingStore.save(sessionKey(rawSessionId, AGENT_ID), JSON);

        assertTrue(backend.lastSessionId.matches("sandbox/session/[A-Za-z0-9_-]{43}"));
        assertTrue(backend.lastSessionId.length() <= 255);
        assertFalse(backend.lastSessionId.contains("prefix"));
        assertEquals(JSON, recordingStore.load(sessionKey(rawSessionId, AGENT_ID)).orElseThrow());
    }

    @Test
    void nonSessionScopesKeepOfficialSlotFormats() throws Exception {
        RecordingStore backend = new RecordingStore();
        SessionSandboxStateStore recordingStore = new SessionSandboxStateStore(backend, AGENT_ID);

        recordingStore.save(isolationKey(IsolationScope.USER, "user-123"), JSON);
        assertEquals("sandbox/user/test-agent/user-123", backend.lastSessionId);

        recordingStore.save(isolationKey(IsolationScope.AGENT, AGENT_ID), JSON);
        assertEquals("sandbox/agent/test-agent", backend.lastSessionId);

        recordingStore.save(
                isolationKey(IsolationScope.GLOBAL, SandboxIsolationKey.GLOBAL_VALUE), JSON);
        assertEquals("sandbox/global", backend.lastSessionId);
    }

    @Test
    void userScope_roundTrip() throws Exception {
        SandboxIsolationKey key = isolationKey(IsolationScope.USER, "user-123");
        store.save(key, JSON);
        assertEquals(JSON, store.load(key).orElseThrow());
        store.delete(key);
        assertFalse(store.load(key).isPresent());
    }

    @Test
    void agentAndGlobalScope_doNotInterfere() throws Exception {
        SandboxIsolationKey agentKey = isolationKey(IsolationScope.AGENT, AGENT_ID);
        SandboxIsolationKey globalKey =
                isolationKey(IsolationScope.GLOBAL, SandboxIsolationKey.GLOBAL_VALUE);

        store.save(agentKey, "{\"scope\":\"agent\"}");
        store.save(globalKey, "{\"scope\":\"global\"}");

        assertEquals("{\"scope\":\"agent\"}", store.load(agentKey).orElseThrow());
        assertEquals("{\"scope\":\"global\"}", store.load(globalKey).orElseThrow());
    }

    @Test
    void deleteUsesTombstone_evenWhenSessionDeleteUnsupported() throws Exception {
        SessionSandboxStateStore redisLikeStore =
                new SessionSandboxStateStore(new NoDeleteSession(), AGENT_ID);
        SandboxIsolationKey key = isolationKey(IsolationScope.SESSION, "sess-del");

        redisLikeStore.save(key, JSON);
        assertTrue(redisLikeStore.load(key).isPresent());

        redisLikeStore.delete(key);
        assertFalse(redisLikeStore.load(key).isPresent());
    }

    private static SandboxIsolationKey isolationKey(IsolationScope scope, String value) {
        return SandboxIsolationKey.resolve(scope, runtimeContext(scope, value), AGENT_ID)
                .orElseThrow();
    }

    private static SandboxIsolationKey sessionKey(String sessionId, String agentId) {
        return SandboxIsolationKey.resolve(
                        IsolationScope.SESSION,
                        RuntimeContext.builder().sessionId(sessionId).build(),
                        agentId)
                .orElseThrow();
    }

    private static RuntimeContext runtimeContext(IsolationScope scope, String value) {
        RuntimeContext.Builder b = RuntimeContext.builder();
        if (scope == IsolationScope.SESSION) {
            b.sessionId(value);
        } else if (scope == IsolationScope.USER) {
            b.userId(value);
        }
        return b.build();
    }

    /** Simulates sessions whose per-key delete is not implemented (default no-op). */
    private static final class NoDeleteSession extends InMemoryAgentStateStore {
        @Override
        public void delete(String userId, String sessionId, String key) {
            // no-op
        }
    }

    private static final class RecordingStore extends InMemoryAgentStateStore {
        private String lastSessionId;

        @Override
        public void save(String userId, String sessionId, String key, State value) {
            lastSessionId = sessionId;
            super.save(userId, sessionId, key, value);
        }

        @Override
        public <T extends State> Optional<T> get(
                String userId, String sessionId, String key, Class<T> type) {
            lastSessionId = sessionId;
            return super.get(userId, sessionId, key, type);
        }
    }
}
