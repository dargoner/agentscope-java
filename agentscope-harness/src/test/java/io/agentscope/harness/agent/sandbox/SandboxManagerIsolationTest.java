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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.harness.agent.IsolationScope;
import io.agentscope.harness.agent.sandbox.snapshot.SandboxSnapshotSpec;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SandboxManagerIsolationTest {

    private static final String AGENT_ID = "test-agent";
    private static final String STATE_JSON = "{\"sessionId\":\"s42\"}";

    @Mock SandboxClient<SandboxClientOptions> client;
    @Mock SessionSandboxStateStore stateStore;
    @Mock Sandbox freshSandbox;
    @Mock Sandbox resumedSandbox;
    @Mock Sandbox externalSandbox;
    @Mock SandboxState externalState;
    @Mock SandboxState resumedState;
    @Mock SandboxSnapshotSpec snapshotSpec;

    SandboxManager manager;

    @BeforeEach
    void setUp() {
        when(stateStore.agentId()).thenReturn(AGENT_ID);
        manager = new SandboxManager(client, stateStore, AGENT_ID);
    }

    // ---- Priority 1: user-managed external session ----

    @Test
    void priority1_externalSession_usedDirectly() throws Exception {
        SandboxContext ctx = SandboxContext.builder().externalSandbox(externalSandbox).build();

        SandboxAcquireResult result = manager.acquire(ctx, null);

        assertSame(externalSandbox, result.getSandbox());
        assertEquals(false, result.isSelfManaged());
        verify(stateStore, never()).load(any());
        verify(client, never()).resume(any(), any());
    }

    // ---- Priority 2: explicit session state ----

    @Test
    void priority2_externalSessionState_resumedDirectly() throws Exception {
        when(externalState.getSessionId()).thenReturn("explicit-id");
        when(client.resume(externalState)).thenReturn(resumedSandbox);

        SandboxContext ctx = SandboxContext.builder().externalSandboxState(externalState).build();

        SandboxAcquireResult result = manager.acquire(ctx, null);

        assertSame(resumedSandbox, result.getSandbox());
        assertEquals(true, result.isSelfManaged());
        verify(stateStore, never()).load(any());
        verify(client, never()).resume(any(), any());
    }

    // ---- Priority 3: state store hit (session scope) ----

    @Test
    void priority3_stateStoreHit_resumesSession() throws Exception {
        when(stateStore.load(any())).thenReturn(Optional.of(STATE_JSON));
        when(client.deserializeState(STATE_JSON, null)).thenReturn(resumedState);
        when(client.resume(any(), any())).thenReturn(resumedSandbox);

        RuntimeContext rtx = RuntimeContext.builder().sessionId("sess-1").build();
        SandboxContext sCtx =
                SandboxContext.builder().isolationScope(IsolationScope.SESSION).build();

        SandboxAcquireResult result = manager.acquire(sCtx, rtx);

        assertSame(resumedSandbox, result.getSandbox());
        assertEquals(true, result.isSelfManaged());
        verify(client, never()).create(any(), any(), any(), any());
    }

    // ---- Priority 3: state store miss → Priority 4 fresh create ----

    @Test
    void priority3_stateStoreMiss_createsFreshSession() throws Exception {
        when(stateStore.load(any())).thenReturn(Optional.empty());
        when(client.create(any(), any(), any(), any())).thenReturn(freshSandbox);

        RuntimeContext rtx = RuntimeContext.builder().sessionId("sess-2").build();
        SandboxContext sCtx =
                SandboxContext.builder()
                        .isolationScope(IsolationScope.SESSION)
                        .snapshotSpec(snapshotSpec)
                        .build();

        SandboxAcquireResult result = manager.acquire(sCtx, rtx);

        assertSame(freshSandbox, result.getSandbox());
        verify(client).create(any(), any(), any(), any());
    }

    // ---- Priority 4 (no session key → scope key empty → fresh create) ----

    @Test
    void noScopeKey_createsFreshSession() throws Exception {
        when(client.create(any(), any(), any())).thenReturn(freshSandbox);

        RuntimeContext rtx = RuntimeContext.builder().build(); // no userId or sessionId
        SandboxContext sCtx = SandboxContext.builder().build(); // scope = null → USER default

        SandboxAcquireResult result = manager.acquire(sCtx, rtx);

        assertSame(freshSandbox, result.getSandbox());
        verify(stateStore, never()).load(any());
        verify(client, never()).create(any(), any(), any(), any());
    }

    // ---- USER scope ----

    @Test
    void userScope_withUserId_loadsFromStore() throws Exception {
        when(stateStore.load(any())).thenReturn(Optional.of(STATE_JSON));
        when(client.deserializeState(STATE_JSON, null)).thenReturn(resumedState);
        when(client.resume(any(), any())).thenReturn(resumedSandbox);

        RuntimeContext rtx = RuntimeContext.builder().userId("user-42").build();
        SandboxContext sCtx = SandboxContext.builder().isolationScope(IsolationScope.USER).build();

        SandboxAcquireResult result = manager.acquire(sCtx, rtx);

        assertSame(resumedSandbox, result.getSandbox());
    }

    @Test
    void priority3_stateStoreHit_passesSnapshotSpecToDeserialize() throws Exception {
        when(stateStore.load(any())).thenReturn(Optional.of(STATE_JSON));
        when(client.deserializeState(STATE_JSON, snapshotSpec)).thenReturn(resumedState);
        when(client.resume(any(), any())).thenReturn(resumedSandbox);

        RuntimeContext rtx = RuntimeContext.builder().sessionId("sess-1").build();
        SandboxContext sCtx =
                SandboxContext.builder()
                        .isolationScope(IsolationScope.SESSION)
                        .snapshotSpec(snapshotSpec)
                        .build();

        SandboxAcquireResult result = manager.acquire(sCtx, rtx);

        assertSame(resumedSandbox, result.getSandbox());
        verify(client).deserializeState(STATE_JSON, snapshotSpec);
    }

    @Test
    void userScope_withUserId_acquiresExecutionGuardWithUserKey() throws Exception {
        AtomicReference<SandboxIsolationKey> capturedKey = new AtomicReference<>();
        SandboxExecutionGuard guard =
                key -> {
                    capturedKey.set(key);
                    return SandboxLease.noop();
                };
        manager = new SandboxManager(client, stateStore, AGENT_ID, guard);
        when(stateStore.load(any())).thenReturn(Optional.empty());
        when(client.create(any(), any(), any(), any())).thenReturn(freshSandbox);

        RuntimeContext rtx = RuntimeContext.builder().userId("user-42").build();
        SandboxContext sCtx = SandboxContext.builder().isolationScope(IsolationScope.USER).build();

        SandboxAcquireResult result = manager.acquire(sCtx, rtx);

        assertSame(freshSandbox, result.getSandbox());
        assertEquals(IsolationScope.USER, capturedKey.get().getScope());
        assertEquals("user-42", capturedKey.get().getValue());
    }

    @Test
    void userScope_missingUserId_createsFreshSession() throws Exception {
        when(client.create(any(), any(), any())).thenReturn(freshSandbox);

        RuntimeContext rtx = RuntimeContext.builder().build(); // no userId
        SandboxContext sCtx = SandboxContext.builder().isolationScope(IsolationScope.USER).build();

        SandboxAcquireResult result = manager.acquire(sCtx, rtx);

        assertSame(freshSandbox, result.getSandbox());
        verify(stateStore, never()).load(any());
    }

    // ---- AGENT scope ----

    @Test
    void agentScope_alwaysHasScopeKey() throws Exception {
        when(stateStore.load(any())).thenReturn(Optional.empty());
        when(client.create(any(), any(), any(), any())).thenReturn(freshSandbox);

        SandboxContext sCtx = SandboxContext.builder().isolationScope(IsolationScope.AGENT).build();

        SandboxAcquireResult result = manager.acquire(sCtx, null);

        assertNotNull(result.getSandbox());
        verify(stateStore).load(any()); // scope key resolved; store was queried
    }

    // ---- GLOBAL scope ----

    @Test
    void globalScope_alwaysHasScopeKey() throws Exception {
        when(stateStore.load(any())).thenReturn(Optional.empty());
        when(client.create(any(), any(), any(), any())).thenReturn(freshSandbox);

        SandboxContext sCtx =
                SandboxContext.builder().isolationScope(IsolationScope.GLOBAL).build();

        SandboxAcquireResult result = manager.acquire(sCtx, null);

        assertNotNull(result.getSandbox());
        verify(stateStore).load(any());
    }

    @Test
    void workspaceAwareClientReceivesResolvedUserKeyOnCreate() throws Exception {
        RecordingContextClient recording = new RecordingContextClient();
        recording.createResult = freshSandbox;
        manager = new SandboxManager(recording, stateStore, AGENT_ID);
        when(stateStore.load(any())).thenReturn(Optional.empty());

        RuntimeContext context =
                RuntimeContext.builder().userId("user-1").sessionId("session-1").build();
        manager.acquire(
                SandboxContext.builder().isolationScope(IsolationScope.USER).build(), context);

        assertEquals(IsolationScope.USER, recording.createKey.getScope());
        assertEquals(AGENT_ID, recording.createKey.getAgentId());
        assertEquals(
                SandboxWorkspaceKey.from(userKey("user-1"), AGENT_ID).getStableId(),
                recording.createKey.getStableId());
        assertEquals(0, recording.originalCreateCalls);
    }

    @Test
    void workspaceAwareClientReceivesResolvedKeyOnPersistedResume() throws Exception {
        RecordingContextClient recording = new RecordingContextClient();
        recording.deserializeResult = resumedState;
        recording.resumeResult = resumedSandbox;
        manager = new SandboxManager(recording, stateStore, AGENT_ID);
        when(stateStore.load(any())).thenReturn(Optional.of(STATE_JSON));

        RuntimeContext context = RuntimeContext.builder().sessionId("session-2").build();
        manager.acquire(
                SandboxContext.builder().isolationScope(IsolationScope.SESSION).build(), context);

        assertEquals(IsolationScope.SESSION, recording.resumeKey.getScope());
        assertEquals(AGENT_ID, recording.resumeKey.getAgentId());
        assertEquals(
                SandboxWorkspaceKey.from(sessionKey("session-2"), AGENT_ID).getStableId(),
                recording.resumeKey.getStableId());
        assertEquals(0, recording.originalResumeCalls);
    }

    @Test
    void missingIdentityUsesOriginalCreateMethod() throws Exception {
        RecordingContextClient recording = new RecordingContextClient();
        recording.createResult = freshSandbox;
        manager = new SandboxManager(recording, stateStore, AGENT_ID);

        manager.acquire(SandboxContext.builder().build(), RuntimeContext.builder().build());

        assertEquals(null, recording.createKey);
        assertEquals(1, recording.originalCreateCalls);
    }

    @Test
    void mismatchedManagerAndStateStoreAgentIdsAreRejected() {
        when(stateStore.agentId()).thenReturn("other-agent");

        assertThrows(
                IllegalArgumentException.class,
                () -> new SandboxManager(client, stateStore, AGENT_ID));
    }

    // ---- persistState ----

    @Test
    void persistState_savesJsonForResolvedScopeKey() throws Exception {
        SandboxState state = mock(SandboxState.class);
        when(state.getSessionId()).thenReturn("sid");
        Sandbox sandbox = mock(Sandbox.class);
        when(sandbox.getState()).thenReturn(state);
        when(client.serializeState(state)).thenReturn(STATE_JSON);
        SandboxAcquireResult result = SandboxAcquireResult.selfManaged(sandbox);

        RuntimeContext rtx = RuntimeContext.builder().sessionId("sess-p").build();
        SandboxContext sCtx =
                SandboxContext.builder().isolationScope(IsolationScope.SESSION).build();

        manager.persistState(result, sCtx, rtx);

        verify(stateStore).save(any(), any());
    }

    @Test
    void persistState_missingScopeKey_skipped() throws Exception {
        Sandbox sandbox = mock(Sandbox.class);
        when(sandbox.getState()).thenReturn(mock(SandboxState.class));
        SandboxAcquireResult result = SandboxAcquireResult.selfManaged(sandbox);

        RuntimeContext rtx = RuntimeContext.builder().build(); // no session key
        SandboxContext sCtx = SandboxContext.builder().build(); // SESSION scope by default

        manager.persistState(result, sCtx, rtx);

        verify(stateStore, never()).save(any(), any());
    }

    // ---- clearState ----

    @Test
    void clearState_deletesFromStore() throws Exception {
        RuntimeContext rtx = RuntimeContext.builder().sessionId("sess-c").build();
        SandboxContext sCtx =
                SandboxContext.builder().isolationScope(IsolationScope.SESSION).build();

        manager.clearState(sCtx, rtx);

        verify(stateStore).delete(any());
    }

    private static final class RecordingContextClient
            implements SandboxClient<SandboxClientOptions> {

        private Sandbox createResult;
        private Sandbox resumeResult;
        private SandboxState deserializeResult;
        private SandboxWorkspaceKey createKey;
        private SandboxWorkspaceKey resumeKey;
        private int originalCreateCalls;
        private int originalResumeCalls;

        @Override
        public Sandbox create(
                WorkspaceSpec workspaceSpec,
                SandboxSnapshotSpec snapshotSpec,
                SandboxClientOptions options) {
            originalCreateCalls++;
            return createResult;
        }

        @Override
        public Sandbox create(
                WorkspaceSpec workspaceSpec,
                SandboxSnapshotSpec snapshotSpec,
                SandboxClientOptions options,
                SandboxWorkspaceKey workspaceKey) {
            createKey = workspaceKey;
            return createResult;
        }

        @Override
        public Sandbox resume(SandboxState state) {
            originalResumeCalls++;
            return resumeResult;
        }

        @Override
        public Sandbox resume(SandboxState state, SandboxWorkspaceKey workspaceKey) {
            resumeKey = workspaceKey;
            return resumeResult;
        }

        @Override
        public void delete(Sandbox sandbox) {}

        @Override
        public String serializeState(SandboxState state) {
            return STATE_JSON;
        }

        @Override
        public SandboxState deserializeState(String json) {
            return deserializeResult;
        }
    }

    private static SandboxIsolationKey userKey(String userId) {
        RuntimeContext context = RuntimeContext.builder().userId(userId).build();
        return SandboxIsolationKey.resolve(IsolationScope.USER, context, AGENT_ID).orElseThrow();
    }

    private static SandboxIsolationKey sessionKey(String sessionId) {
        RuntimeContext context = RuntimeContext.builder().sessionId(sessionId).build();
        return SandboxIsolationKey.resolve(IsolationScope.SESSION, context, AGENT_ID).orElseThrow();
    }
}
