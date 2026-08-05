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
package io.agentscope.harness.agent.filesystem.sandbox;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.harness.agent.filesystem.model.FileDownloadResponse;
import io.agentscope.harness.agent.filesystem.model.FileUploadResponse;
import io.agentscope.harness.agent.sandbox.ExecResult;
import io.agentscope.harness.agent.sandbox.Sandbox;
import io.agentscope.harness.agent.sandbox.SandboxException;
import io.agentscope.harness.agent.sandbox.SandboxFileTransfer;
import io.agentscope.harness.agent.sandbox.SandboxState;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class SandboxBackedFilesystemTest {

    private static RuntimeContext rc(String sessionId) {
        return RuntimeContext.builder().sessionId(sessionId).build();
    }

    private static RuntimeContext rc(String userId, String sessionId) {
        return RuntimeContext.builder().userId(userId).sessionId(sessionId).build();
    }

    // mirrors the binding key logic in SandboxBackedFilesystem / SandboxLifecycleMiddleware
    private static String bindKey(RuntimeContext ctx) {
        String uid = ctx.getUserId();
        String sid = ctx.getSessionId();
        return (uid == null || uid.isBlank() ? "__anon__" : uid) + "/" + sid;
    }

    @Test
    void downloadFiles_decodesWrappedBase64Output() {
        byte[] expected = new byte[] {1, 2, 3, 4, 5, 6};
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        FakeSandbox sandbox = new FakeSandbox(new ExecResult(0, "AQID\nBAUG", "", false));
        RuntimeContext ctx = rc("session-a");
        filesystem.bindSandbox(bindKey(ctx), sandbox);

        List<FileDownloadResponse> responses =
                filesystem.downloadFiles(ctx, List.of("/tmp/data.bin"));

        assertEquals("base64 '/tmp/data.bin'", sandbox.lastCommand);
        assertEquals(1, responses.size());
        assertTrue(responses.get(0).isSuccess());
        assertEquals("/tmp/data.bin", responses.get(0).path());
        assertArrayEquals(expected, responses.get(0).content());
    }

    @Test
    void downloadFiles_decodesEmptyPayloadWhenStdoutIsNull() {
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        FakeSandbox sandbox = new FakeSandbox(new ExecResult(0, null, "", false));
        RuntimeContext ctx = rc("session-a");
        filesystem.bindSandbox(bindKey(ctx), sandbox);

        List<FileDownloadResponse> responses =
                filesystem.downloadFiles(ctx, List.of("/tmp/empty.bin"));

        assertEquals("base64 '/tmp/empty.bin'", sandbox.lastCommand);
        assertEquals(1, responses.size());
        assertTrue(responses.get(0).isSuccess());
        assertEquals("/tmp/empty.bin", responses.get(0).path());
        assertArrayEquals(new byte[0], responses.get(0).content());
    }

    @Test
    void getWorkspaceRoot_usesRuntimeContextBinding() {
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        RuntimeContext ctxA = rc("session-a");
        RuntimeContext ctxB = rc("session-b");
        FakeSandbox sandboxA = new FakeSandbox(new ExecResult(0, "", "", false));
        FakeSandbox sandboxB = new FakeSandbox(new ExecResult(0, "", "", false));
        sandboxA.workspaceRoot = "/sandbox/a";
        sandboxB.workspaceRoot = "/sandbox/b";
        filesystem.bindSandbox(bindKey(ctxA), sandboxA);
        filesystem.bindSandbox(bindKey(ctxB), sandboxB);

        assertEquals("/sandbox/a", filesystem.getWorkspaceRoot(ctxA));
        assertEquals("/sandbox/b", filesystem.getWorkspaceRoot(ctxB));
    }

    @Test
    void getWorkspaceRoot_fallbackWhenSessionIsUnbound() {
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();

        assertEquals("/workspace", filesystem.getWorkspaceRoot(rc("unknown-session")));
    }

    @Test
    void downloadFiles_returnsFailureWhenCommandFails() {
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        FakeSandbox sandbox = new FakeSandbox(new ExecResult(1, "", "boom", false));
        RuntimeContext ctx = rc("session-a");
        filesystem.bindSandbox(bindKey(ctx), sandbox);

        List<FileDownloadResponse> responses =
                filesystem.downloadFiles(ctx, List.of("/tmp/fail.bin"));

        assertEquals("base64 '/tmp/fail.bin'", sandbox.lastCommand);
        assertEquals(1, responses.size());
        assertTrue(!responses.get(0).isSuccess());
        assertEquals("/tmp/fail.bin", responses.get(0).path());
        assertEquals("[stderr] boom", responses.get(0).error());
    }

    @Test
    void uploadFiles_prefersNativeTransferWhenSupported() {
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        FakeTransferSandbox sandbox = new FakeTransferSandbox("/workspace");
        RuntimeContext ctx = rc("native-upload");
        filesystem.bindSandbox(bindKey(ctx), sandbox);

        List<FileUploadResponse> responses =
                filesystem.uploadFiles(
                        ctx, List.of(Map.entry("/workspace/a.txt", new byte[] {7, 8})));

        assertTrue(responses.get(0).isSuccess());
        assertArrayEquals(new byte[] {7, 8}, sandbox.uploaded.get("/workspace/a.txt"));
        assertNull(sandbox.lastCommand);
    }

    @Test
    void uploadFiles_fallsBackToExecForUnsupportedPaths() {
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        FakeTransferSandbox sandbox = new FakeTransferSandbox("/workspace");
        RuntimeContext ctx = rc("fallback-upload");
        filesystem.bindSandbox(bindKey(ctx), sandbox);

        List<FileUploadResponse> responses =
                filesystem.uploadFiles(ctx, List.of(Map.entry("/etc/other.txt", new byte[] {1})));

        assertTrue(responses.get(0).isSuccess());
        assertTrue(sandbox.uploaded.isEmpty());
        assertTrue(sandbox.lastCommand.contains("base64 -d > '/etc/other.txt'"));
    }

    @Test
    void downloadFiles_prefersNativeTransferWhenSupported() {
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        FakeTransferSandbox sandbox = new FakeTransferSandbox("/workspace");
        sandbox.uploaded.put("/workspace/b.bin", new byte[] {9, 9});
        RuntimeContext ctx = rc("native-download");
        filesystem.bindSandbox(bindKey(ctx), sandbox);

        List<FileDownloadResponse> responses =
                filesystem.downloadFiles(ctx, List.of("/workspace/b.bin"));

        assertTrue(responses.get(0).isSuccess());
        assertArrayEquals(new byte[] {9, 9}, responses.get(0).content());
        assertNull(sandbox.lastCommand);
    }

    @Test
    void uploadFiles_reportsNativeTransferFailure() {
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        FakeTransferSandbox sandbox = new FakeTransferSandbox("/workspace");
        sandbox.failTransfers = true;
        RuntimeContext ctx = rc("failed-upload");
        filesystem.bindSandbox(bindKey(ctx), sandbox);

        List<FileUploadResponse> responses =
                filesystem.uploadFiles(ctx, List.of(Map.entry("/workspace/c.txt", new byte[] {1})));

        assertTrue(!responses.get(0).isSuccess());
        assertEquals("transfer down", responses.get(0).error());
    }

    @Test
    void requireSandbox_throwsWhenNoBindingForSession() {
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        RuntimeContext ctx = rc("unknown-session");

        assertThrows(
                SandboxException.SandboxConfigurationException.class,
                () -> filesystem.execute(ctx, "echo hi", null));
    }

    @Test
    void bindAndUnbind_isolatesSessionsFromEachOther() {
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        FakeSandbox sandboxA = new FakeSandbox(new ExecResult(0, "AAAA", "", false));
        FakeSandbox sandboxB = new FakeSandbox(new ExecResult(0, "BBBB", "", false));

        RuntimeContext ctxA = rc("session-a");
        RuntimeContext ctxB = rc("session-b");
        filesystem.bindSandbox(bindKey(ctxA), sandboxA);
        filesystem.bindSandbox(bindKey(ctxB), sandboxB);

        filesystem.downloadFiles(ctxA, List.of("/f"));
        assertNotNull(sandboxA.lastCommand);
        assertNull(sandboxB.lastCommand);

        filesystem.downloadFiles(ctxB, List.of("/g"));
        assertNotNull(sandboxB.lastCommand);

        filesystem.unbindSandbox(bindKey(ctxA));
        assertThrows(
                SandboxException.SandboxConfigurationException.class,
                () -> filesystem.execute(ctxA, "echo", null));
        filesystem.downloadFiles(ctxB, List.of("/h"));
    }

    @Test
    void concurrentSessionsDontCrossContaminate() throws InterruptedException {
        int sessions = 8;
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        FakeSandbox[] sandboxes = new FakeSandbox[sessions];
        RuntimeContext[] contexts = new RuntimeContext[sessions];
        for (int i = 0; i < sessions; i++) {
            sandboxes[i] = new FakeSandbox(new ExecResult(0, "AA==", "", false));
            contexts[i] = rc("session-" + i);
            filesystem.bindSandbox(bindKey(contexts[i]), sandboxes[i]);
        }

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(sessions);
        ExecutorService pool = Executors.newFixedThreadPool(sessions);
        String[] errors = new String[sessions];

        for (int i = 0; i < sessions; i++) {
            final int idx = i;
            pool.submit(
                    () -> {
                        try {
                            start.await();
                            filesystem.execute(contexts[idx], "cmd-" + idx, null);
                            String expected = "cmd-" + idx;
                            if (!expected.equals(sandboxes[idx].lastCommand)) {
                                errors[idx] =
                                        "session-"
                                                + idx
                                                + " routed to wrong sandbox: "
                                                + sandboxes[idx].lastCommand;
                            }
                        } catch (Exception e) {
                            errors[idx] = e.getMessage();
                        } finally {
                            done.countDown();
                        }
                    });
        }

        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdown();

        for (int i = 0; i < sessions; i++) {
            assertNull(errors[i], "Error in session-" + i + ": " + errors[i]);
        }
    }

    @Test
    void sameSessionIdDifferentUsersAreIsolated() {
        SandboxBackedFilesystem filesystem = new SandboxBackedFilesystem();
        FakeSandbox sandboxAlice = new FakeSandbox(new ExecResult(0, "AA==", "", false));
        FakeSandbox sandboxBob = new FakeSandbox(new ExecResult(0, "AA==", "", false));

        RuntimeContext ctxAlice = rc("alice", "default");
        RuntimeContext ctxBob = rc("bob", "default");
        filesystem.bindSandbox(bindKey(ctxAlice), sandboxAlice);
        filesystem.bindSandbox(bindKey(ctxBob), sandboxBob);

        filesystem.execute(ctxAlice, "alice-cmd", null);
        filesystem.execute(ctxBob, "bob-cmd", null);

        assertEquals("alice-cmd", sandboxAlice.lastCommand);
        assertEquals("bob-cmd", sandboxBob.lastCommand);
    }

    private static final class FakeTransferSandbox extends BaseFakeSandbox
            implements SandboxFileTransfer {

        private final String rootPrefix;
        private final Map<String, byte[]> uploaded = new HashMap<>();
        private boolean failTransfers;

        private FakeTransferSandbox(String root) {
            super(new ExecResult(0, "", "", false));
            this.rootPrefix = root + "/";
        }

        @Override
        public boolean supportsFileTransfer(String absolutePath) {
            return absolutePath.startsWith(rootPrefix);
        }

        @Override
        public void uploadFile(String absolutePath, byte[] content) throws Exception {
            if (failTransfers) {
                throw new IllegalStateException("transfer down");
            }
            uploaded.put(absolutePath, content);
        }

        @Override
        public byte[] downloadFile(String absolutePath) throws Exception {
            if (failTransfers) {
                throw new IllegalStateException("transfer down");
            }
            return uploaded.get(absolutePath);
        }
    }

    private static final class FakeSandbox extends BaseFakeSandbox {

        private FakeSandbox(ExecResult execResult) {
            super(execResult);
        }
    }

    private static class BaseFakeSandbox implements Sandbox {

        private final ExecResult execResult;
        protected String lastCommand;
        protected String workspaceRoot = "/workspace";

        protected BaseFakeSandbox(ExecResult execResult) {
            this.execResult = execResult;
        }

        @Override
        public String getWorkspaceRoot() {
            return workspaceRoot;
        }

        @Override
        public void start() {}

        @Override
        public void stop() {}

        @Override
        public void shutdown() {}

        @Override
        public void close() {}

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public SandboxState getState() {
            return null;
        }

        @Override
        public ExecResult exec(
                RuntimeContext runtimeContext, String command, Integer timeoutSeconds) {
            this.lastCommand = command;
            return execResult;
        }

        @Override
        public InputStream persistWorkspace() {
            return InputStream.nullInputStream();
        }

        @Override
        public void hydrateWorkspace(InputStream archive) {}
    }
}
