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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.harness.agent.IsolationScope;
import io.agentscope.harness.agent.sandbox.SandboxIsolationKey;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class OpenSandboxRedisLifecycleOptionsTest {

    @Test
    void defaultsMatchClusterLifecycleContract() {
        OpenSandboxRedisLifecycleOptions options = new OpenSandboxRedisLifecycleOptions();

        assertEquals(Duration.ofHours(1), options.getIdleTtl());
        assertEquals(Duration.ofMinutes(5), options.getSweepInterval());
        assertEquals(Duration.ofMinutes(5), options.getEvictionGrace());
        assertEquals(Duration.ofSeconds(30), options.getHeartbeatInterval());
        assertEquals(Duration.ofSeconds(180), options.getActiveLeaseTtl());
        assertEquals(Duration.ofMinutes(10), options.getActiveSandboxTtl());
        assertEquals(Duration.ofMinutes(6), options.getActiveRenewLead());
        assertEquals(Duration.ofMinutes(5), options.getPauseRetention());
        assertEquals(Duration.ofMinutes(5), options.getSnapshotReadyTimeout());
        assertEquals(Duration.ofSeconds(10), options.getLockWait());
        assertEquals(Duration.ofMinutes(10), options.getOrphanGrace());
        assertEquals(true, options.isSweeperEnabled());
    }

    @Test
    void rejectsInvalidLifecycleRelationships() {
        OpenSandboxRedisLifecycleOptions options = new OpenSandboxRedisLifecycleOptions();

        assertThrows(
                IllegalArgumentException.class,
                () -> options.setActiveLeaseTtl(Duration.ofSeconds(60)));
        assertThrows(
                IllegalArgumentException.class,
                () -> options.setSweepInterval(Duration.ofHours(1)));
        assertThrows(
                IllegalArgumentException.class,
                () -> options.setActiveRenewLead(Duration.ofMinutes(10)));
        assertThrows(
                IllegalArgumentException.class,
                () -> options.setSnapshotReadyTimeout(Duration.ofMinutes(10)));
        assertThrows(
                IllegalArgumentException.class,
                () -> options.setEvictionGrace(Duration.ofSeconds(10)));
        assertThrows(IllegalArgumentException.class, () -> options.setLockWait(Duration.ZERO));
    }

    @Test
    void copyIsIndependentFromSource() {
        OpenSandboxRedisLifecycleOptions source = new OpenSandboxRedisLifecycleOptions();
        OpenSandboxRedisLifecycleOptions copy = OpenSandboxRedisLifecycleOptions.copyOf(source);

        copy.setIdleTtl(Duration.ofHours(2));
        copy.setSweeperEnabled(false);

        assertEquals(Duration.ofHours(1), source.getIdleTtl());
        assertEquals(true, source.isSweeperEnabled());
    }

    @Test
    void workspaceIdIsStableScopedAndDoesNotExposeIdentity() {
        RuntimeContext first =
                RuntimeContext.builder()
                        .userId("customer-sensitive-id")
                        .sessionId("session-1")
                        .build();
        RuntimeContext second =
                RuntimeContext.builder()
                        .userId("customer-sensitive-id")
                        .sessionId("session-2")
                        .build();
        SandboxIsolationKey firstKey =
                SandboxIsolationKey.resolve(IsolationScope.USER, first, "agent-a").orElseThrow();
        SandboxIsolationKey secondKey =
                SandboxIsolationKey.resolve(IsolationScope.USER, second, "agent-a").orElseThrow();

        String firstId = RedisOpenSandboxClient.workspaceId(firstKey, "agent-a");
        String secondId = RedisOpenSandboxClient.workspaceId(secondKey, "agent-a");
        String otherAgentId = RedisOpenSandboxClient.workspaceId(firstKey, "agent-b");

        assertEquals(firstId, secondId);
        assertNotEquals(firstId, otherAgentId);
        assertFalse(firstId.contains("customer-sensitive-id"));
        assertFalse(firstId.contains("="));
        assertEquals(43, firstId.length());
    }

    @Test
    void workspaceRecordRoundTripPreservesLifecycleState() throws Exception {
        OpenSandboxWorkspaceRecord record = new OpenSandboxWorkspaceRecord();
        record.setSchemaVersion(1);
        record.setWorkspaceId("workspace-hash");
        record.setIsolationScope("USER");
        record.setAgentId("agent-a");
        record.setSandboxId("sandbox-1");
        record.setLifecycleState(OpenSandboxWorkspaceRecord.LifecycleState.RUNNING);
        record.setGeneration(3);
        record.setRuntimeImage("image:v1");
        record.setRuntimeProfileHash("profile-hash");
        record.setNativeSnapshotId("snapshot-current");
        record.setPreviousNativeSnapshotId("snapshot-previous");
        record.setLastAccessAt(Instant.parse("2026-08-08T08:00:00Z"));
        record.setDirty(true);

        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        OpenSandboxWorkspaceRecord decoded =
                mapper.readValue(
                        mapper.writeValueAsBytes(record), OpenSandboxWorkspaceRecord.class);

        assertEquals("workspace-hash", decoded.getWorkspaceId());
        assertEquals(
                OpenSandboxWorkspaceRecord.LifecycleState.RUNNING, decoded.getLifecycleState());
        assertEquals(3, decoded.getGeneration());
        assertEquals("snapshot-current", decoded.getNativeSnapshotId());
        assertEquals(Instant.parse("2026-08-08T08:00:00Z"), decoded.getLastAccessAt());
        assertEquals(true, decoded.isDirty());
    }

    @Test
    void activeLeaseRequiresCompleteIdentity() {
        Instant now = Instant.parse("2026-08-08T08:00:00Z");
        OpenSandboxActiveLease lease =
                new OpenSandboxActiveLease("lease-1", "workspace-1", 2, "instance-1", now, now);

        assertEquals("lease-1", lease.leaseId());
        assertThrows(
                NullPointerException.class,
                () -> new OpenSandboxActiveLease(null, "workspace-1", 2, "instance-1", now, now));
    }
}
