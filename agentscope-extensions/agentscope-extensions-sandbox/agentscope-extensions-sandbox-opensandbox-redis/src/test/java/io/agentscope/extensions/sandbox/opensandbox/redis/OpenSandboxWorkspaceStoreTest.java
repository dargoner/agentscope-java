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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RMapCache;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

class OpenSandboxWorkspaceStoreTest {
    private static final String WORKSPACE_ID = "workspace-hash";

    private RedissonClient redisson;
    private RBucket<String> bucket;
    private RMapCache<String, String> leases;
    private RScoredSortedSet<String> idle;
    private RLock lock;
    private OpenSandboxWorkspaceStore store;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisson = mock(RedissonClient.class);
        bucket = mock(RBucket.class);
        leases = mock(RMapCache.class);
        idle = mock(RScoredSortedSet.class);
        lock = mock(RLock.class);
        doReturn(bucket)
                .when(redisson)
                .getBucket(
                        "agentscope:opensandbox:{" + WORKSPACE_ID + "}:record",
                        StringCodec.INSTANCE);
        doReturn(leases)
                .when(redisson)
                .getMapCache(
                        "agentscope:opensandbox:{" + WORKSPACE_ID + "}:active-turns",
                        StringCodec.INSTANCE);
        doReturn(idle)
                .when(redisson)
                .getScoredSortedSet("agentscope:opensandbox:idle:v1", StringCodec.INSTANCE);
        when(redisson.getLock("agentscope:opensandbox:{" + WORKSPACE_ID + "}:lifecycle-lock"))
                .thenReturn(lock);
        store = new OpenSandboxWorkspaceStore(redisson);
    }

    @Test
    void keysUseWorkspaceClusterHashTag() {
        assertEquals(
                "agentscope:opensandbox:{" + WORKSPACE_ID + "}:record",
                store.recordKey(WORKSPACE_ID));
        assertEquals(
                "agentscope:opensandbox:{" + WORKSPACE_ID + "}:active-turns",
                store.activeTurnsKey(WORKSPACE_ID));
        assertEquals(
                "agentscope:opensandbox:{" + WORKSPACE_ID + "}:lifecycle-lock",
                store.lifecycleLockKey(WORKSPACE_ID));
        assertSame(lock, store.lifecycleLock(WORKSPACE_ID));
    }

    @Test
    void recordLoadAndCompareAndSetUseJsonStrings() throws Exception {
        OpenSandboxWorkspaceRecord record = record(4);
        String json = new ObjectMapper().findAndRegisterModules().writeValueAsString(record);
        when(bucket.get()).thenReturn(json);
        when(bucket.compareAndSet(isNull(), any(String.class))).thenReturn(true);

        Optional<OpenSandboxWorkspaceRecord> loaded = store.load(WORKSPACE_ID);
        boolean created = store.compareAndSet(null, record);

        assertEquals("sandbox-4", loaded.orElseThrow().getSandboxId());
        assertTrue(created);
        ArgumentCaptor<String> update = ArgumentCaptor.forClass(String.class);
        verify(bucket).compareAndSet(isNull(), update.capture());
        OpenSandboxWorkspaceRecord stored =
                new ObjectMapper()
                        .findAndRegisterModules()
                        .readValue(update.getValue(), OpenSandboxWorkspaceRecord.class);
        assertEquals(4, stored.getGeneration());
        assertEquals("workspace-hash", stored.getWorkspaceId());
    }

    @Test
    void leaseUsesEntryTtlAndActiveReadFiltersOldGeneration() throws Exception {
        Instant now = Instant.parse("2026-08-08T08:00:00Z");
        OpenSandboxActiveLease current =
                new OpenSandboxActiveLease("lease-current", WORKSPACE_ID, 4, "node-a", now, now);
        OpenSandboxActiveLease stale =
                new OpenSandboxActiveLease("lease-stale", WORKSPACE_ID, 3, "node-b", now, now);
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        when(leases.readAllMap())
                .thenReturn(
                        Map.of(
                                current.leaseId(), mapper.writeValueAsString(current),
                                stale.leaseId(), mapper.writeValueAsString(stale)));

        store.putLease(current, Duration.ofSeconds(180));
        List<OpenSandboxActiveLease> active = store.activeLeases(WORKSPACE_ID, 4);
        store.removeLease(WORKSPACE_ID, current.leaseId());

        verify(leases)
                .put(
                        eq("lease-current"),
                        any(String.class),
                        eq(180_000L),
                        eq(TimeUnit.MILLISECONDS));
        verify(leases).fastRemove("lease-current");
        assertEquals(List.of(current), active);
    }

    @Test
    void idleIndexIsNonDestructiveHint() {
        Instant dueAt = Instant.parse("2026-08-08T09:00:00Z");
        when(idle.valueRange(
                        anyDouble(), anyBoolean(), anyDouble(), anyBoolean(), anyInt(), anyInt()))
                .thenReturn(List.of(WORKSPACE_ID));

        store.scheduleIdle(WORKSPACE_ID, dueAt);
        List<String> due = store.dueIdle(dueAt, 25);
        store.cancelIdle(WORKSPACE_ID);

        verify(idle).add((double) dueAt.toEpochMilli(), WORKSPACE_ID);
        verify(idle)
                .valueRange(
                        Double.NEGATIVE_INFINITY, true, (double) dueAt.toEpochMilli(), true, 0, 25);
        verify(idle).remove(WORKSPACE_ID);
        verify(idle, never()).pollFirst(anyInt());
        assertEquals(List.of(WORKSPACE_ID), due);
    }

    @Test
    void closeDoesNotShutdownInjectedRedissonClient() {
        store.close();

        verify(redisson, never()).shutdown();
    }

    private static OpenSandboxWorkspaceRecord record(long generation) {
        OpenSandboxWorkspaceRecord record = new OpenSandboxWorkspaceRecord();
        record.setWorkspaceId(WORKSPACE_ID);
        record.setSandboxId("sandbox-" + generation);
        record.setGeneration(generation);
        record.setLifecycleState(OpenSandboxWorkspaceRecord.LifecycleState.RUNNING);
        return record;
    }
}
