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
package io.agentscope.extensions.sandbox.opensandbox.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.agentscope.extensions.sandbox.opensandbox.OpenSandbox;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxState;
import io.agentscope.harness.agent.sandbox.ExecResult;
import java.time.Instant;
import java.util.concurrent.ScheduledFuture;
import org.junit.jupiter.api.Test;

class RedisManagedOpenSandboxTest {

    @Test
    void startDelegatesExecutionAndStopFinallyReleasesOnlyThisLease() throws Exception {
        RedisOpenSandboxClient owner = mock(RedisOpenSandboxClient.class);
        OpenSandbox delegate = mock(OpenSandbox.class);
        ScheduledFuture<?> heartbeat = mock(ScheduledFuture.class);
        OpenSandboxActiveLease lease =
                new OpenSandboxActiveLease(
                        "lease-1",
                        "workspace-1",
                        2,
                        "instance-a",
                        Instant.parse("2026-08-08T08:00:00Z"),
                        Instant.parse("2026-08-08T08:00:00Z"));
        doReturn(heartbeat).when(owner).startHeartbeat(eq(lease), any(), any());
        when(delegate.exec(null, "pwd", 10)).thenReturn(new ExecResult(0, "/workspace", "", false));
        RedisManagedOpenSandbox sandbox = new RedisManagedOpenSandbox(owner, delegate, lease);

        sandbox.start();
        ExecResult result = sandbox.exec(null, "pwd", 10);
        sandbox.stop();
        sandbox.stop();
        sandbox.shutdown();

        assertEquals("/workspace", result.stdout());
        assertFalse(sandbox.isRunning());
        verify(owner, times(1)).startHeartbeat(eq(lease), any(), any());
        verify(heartbeat, times(1)).cancel(false);
        verify(delegate, times(1)).disconnect();
        verify(owner, times(1)).release(sandbox);
        verify(delegate, never()).shutdown();
    }

    @Test
    void nativeFileTransferAndStateAreDelegated() throws Exception {
        RedisOpenSandboxClient owner = mock(RedisOpenSandboxClient.class);
        OpenSandbox delegate = mock(OpenSandbox.class);
        OpenSandboxState state = new OpenSandboxState();
        when(delegate.getState()).thenReturn(state);
        when(delegate.supportsFileTransfer("/workspace/a.txt")).thenReturn(true);
        when(delegate.downloadFile("/workspace/a.txt")).thenReturn(new byte[] {1, 2});
        OpenSandboxActiveLease lease =
                new OpenSandboxActiveLease(
                        "lease-1",
                        "workspace-1",
                        2,
                        "instance-a",
                        Instant.parse("2026-08-08T08:00:00Z"),
                        Instant.parse("2026-08-08T08:00:00Z"));
        RedisManagedOpenSandbox sandbox = new RedisManagedOpenSandbox(owner, delegate, lease);

        sandbox.uploadFile("/workspace/a.txt", new byte[] {1, 2});

        assertTrue(sandbox.supportsFileTransfer("/workspace/a.txt"));
        assertEquals(2, sandbox.downloadFile("/workspace/a.txt").length);
        assertEquals(state, sandbox.getState());
        assertEquals("lease-1", sandbox.leaseId());
        assertEquals("workspace-1", sandbox.workspaceId());
        verify(delegate).uploadFile("/workspace/a.txt", new byte[] {1, 2});
    }

    @Test
    void stopStillDisconnectsAndReleasesWhenHeartbeatCancellationFails() throws Exception {
        RedisOpenSandboxClient owner = mock(RedisOpenSandboxClient.class);
        OpenSandbox delegate = mock(OpenSandbox.class);
        ScheduledFuture<?> heartbeat = mock(ScheduledFuture.class);
        OpenSandboxActiveLease lease = lease();
        doReturn(heartbeat).when(owner).startHeartbeat(eq(lease), any(), any());
        doThrow(new IllegalStateException("cancel failed")).when(heartbeat).cancel(false);
        RedisManagedOpenSandbox sandbox = new RedisManagedOpenSandbox(owner, delegate, lease);
        sandbox.start();

        IllegalStateException error = assertThrows(IllegalStateException.class, sandbox::stop);

        assertEquals("cancel failed", error.getMessage());
        verify(delegate).disconnect();
        verify(owner).release(sandbox);
    }

    @Test
    void stopRetriesReleaseWithoutRepeatingLocalCleanup() throws Exception {
        RedisOpenSandboxClient owner = mock(RedisOpenSandboxClient.class);
        OpenSandbox delegate = mock(OpenSandbox.class);
        ScheduledFuture<?> heartbeat = mock(ScheduledFuture.class);
        OpenSandboxActiveLease lease = lease();
        doReturn(heartbeat).when(owner).startHeartbeat(eq(lease), any(), any());
        RedisManagedOpenSandbox sandbox = new RedisManagedOpenSandbox(owner, delegate, lease);
        doThrow(new IllegalStateException("redis unavailable"))
                .doNothing()
                .when(owner)
                .release(sandbox);
        sandbox.start();

        IllegalStateException first = assertThrows(IllegalStateException.class, sandbox::stop);
        sandbox.stop();

        assertEquals("redis unavailable", first.getMessage());
        verify(owner, times(2)).release(sandbox);
        verify(heartbeat, times(1)).cancel(false);
        verify(delegate, times(1)).disconnect();
    }

    private static OpenSandboxActiveLease lease() {
        return new OpenSandboxActiveLease(
                "lease-1",
                "workspace-1",
                2,
                "instance-a",
                Instant.parse("2026-08-08T08:00:00Z"),
                Instant.parse("2026-08-08T08:00:00Z"));
    }
}
