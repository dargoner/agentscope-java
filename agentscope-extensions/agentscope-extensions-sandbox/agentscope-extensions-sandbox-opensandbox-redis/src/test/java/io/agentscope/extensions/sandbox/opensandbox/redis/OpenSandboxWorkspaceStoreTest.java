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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RMapCache;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

class OpenSandboxWorkspaceStoreTest {
    private static final String WORKSPACE_ID = "workspace-hash";

    private RedissonClient redisson;
    private RBucket<String> bucket;
    private RBucket<String> tombstone;
    private RMapCache<String, String> leases;
    private RScoredSortedSet<String> idle;
    private RScoredSortedSet<String> repairs;
    private RScoredSortedSet<String> orphanSnapshots;
    private RLock lock;
    private RLock snapshotReferencesLock;
    private RMap<String, String> snapshotReferences;
    private OpenSandboxWorkspaceStore store;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisson = mock(RedissonClient.class);
        bucket = mock(RBucket.class);
        tombstone = mock(RBucket.class);
        leases = mock(RMapCache.class);
        idle = mock(RScoredSortedSet.class);
        repairs = mock(RScoredSortedSet.class);
        orphanSnapshots = mock(RScoredSortedSet.class);
        lock = mock(RLock.class);
        snapshotReferencesLock = mock(RLock.class);
        snapshotReferences = mock(RMap.class);
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
        doReturn(tombstone)
                .when(redisson)
                .getBucket(
                        "agentscope:opensandbox:{" + WORKSPACE_ID + "}:deleted-through",
                        StringCodec.INSTANCE);
        doReturn(idle)
                .when(redisson)
                .getScoredSortedSet("agentscope:opensandbox:idle:v1", StringCodec.INSTANCE);
        doReturn(repairs)
                .when(redisson)
                .getScoredSortedSet(
                        "agentscope:opensandbox:{workspace-repairs}:v1", StringCodec.INSTANCE);
        doReturn(orphanSnapshots)
                .when(redisson)
                .getScoredSortedSet(
                        "agentscope:opensandbox:orphan-snapshots:v1", StringCodec.INSTANCE);
        when(redisson.getLock("agentscope:opensandbox:{" + WORKSPACE_ID + "}:lifecycle-lock"))
                .thenReturn(lock);
        when(redisson.getLock("agentscope:opensandbox:{snapshot-references}:lock:v1"))
                .thenReturn(snapshotReferencesLock);
        doReturn(snapshotReferences)
                .when(redisson)
                .getMap("agentscope:opensandbox:{snapshot-references}:v1", StringCodec.INSTANCE);
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
        assertEquals(
                "agentscope:opensandbox:{" + WORKSPACE_ID + "}:deleted-through",
                store.deletedThroughKey(WORKSPACE_ID));
        assertSame(lock, store.lifecycleLock(WORKSPACE_ID));
    }

    @Test
    void deletedThroughGenerationOnlyMovesForward() {
        when(tombstone.get()).thenReturn(null, "4", "7");
        when(tombstone.compareAndSet(null, "4")).thenReturn(true);
        when(tombstone.compareAndSet("4", "7")).thenReturn(true);

        assertEquals(4, store.advanceDeletedThroughGeneration(WORKSPACE_ID, 4));
        assertEquals(7, store.advanceDeletedThroughGeneration(WORKSPACE_ID, 7));
        assertEquals(7, store.advanceDeletedThroughGeneration(WORKSPACE_ID, 6));
        assertEquals(7, store.deletedThroughGeneration(WORKSPACE_ID));

        verify(tombstone, never()).compareAndSet("7", "6");
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
        assertFalse(update.getValue().contains("serializedSandboxState"));
        OpenSandboxWorkspaceRecord stored =
                new ObjectMapper()
                        .findAndRegisterModules()
                        .readValue(update.getValue(), OpenSandboxWorkspaceRecord.class);
        assertEquals(4, stored.getGeneration());
        assertEquals("workspace-hash", stored.getWorkspaceId());
    }

    @Test
    void loadMigratesLegacyRecordWithoutRetainingWorkspaceSecrets() throws Exception {
        String legacyJson = legacyRecordJson(4, "FILE_SECRET_ALPHA", "ENV_SECRET_ALPHA");
        when(bucket.get()).thenReturn(legacyJson);
        when(bucket.compareAndSet(eq(legacyJson), any(String.class))).thenReturn(true);

        OpenSandboxWorkspaceRecord loaded = store.load(WORKSPACE_ID).orElseThrow();

        ArgumentCaptor<String> migrated = ArgumentCaptor.forClass(String.class);
        verify(bucket).compareAndSet(eq(legacyJson), migrated.capture());
        String migratedJson = migrated.getValue();
        assertEquals(2, loaded.getSchemaVersion());
        assertFalse(migratedJson.contains("serializedSandboxState"));
        assertFalse(migratedJson.contains("FILE_SECRET_ALPHA"));
        assertFalse(migratedJson.contains("ENV_SECRET_ALPHA"));
        assertFalse(migratedJson.contains("\"content\""));
        assertFalse(migratedJson.contains("API_TOKEN"));

        OpenSandboxWorkspaceRecord update = loaded.copy();
        update.setGeneration(5);
        when(bucket.compareAndSet(eq(migratedJson), any(String.class))).thenReturn(true);

        assertTrue(store.compareAndSet(loaded, update));
        verify(bucket).compareAndSet(eq(migratedJson), any(String.class));
    }

    @Test
    void loadRetriesLegacyMigrationAfterCasCompetition() throws Exception {
        String first = legacyRecordJson(4, "FILE_SECRET_FIRST", "ENV_SECRET_FIRST");
        String winner = legacyRecordJson(5, "FILE_SECRET_WINNER", "ENV_SECRET_WINNER");
        when(bucket.get()).thenReturn(first, winner);
        when(bucket.compareAndSet(eq(first), any(String.class))).thenReturn(false);
        when(bucket.compareAndSet(eq(winner), any(String.class))).thenReturn(true);

        OpenSandboxWorkspaceRecord loaded = store.load(WORKSPACE_ID).orElseThrow();

        assertEquals(5, loaded.getGeneration());
        assertEquals(2, loaded.getSchemaVersion());
        ArgumentCaptor<String> migrated = ArgumentCaptor.forClass(String.class);
        verify(bucket).compareAndSet(eq(winner), migrated.capture());
        assertFalse(migrated.getValue().contains("serializedSandboxState"));
        assertFalse(migrated.getValue().contains("FILE_SECRET_WINNER"));
        assertFalse(migrated.getValue().contains("ENV_SECRET_WINNER"));
        verify(bucket, org.mockito.Mockito.times(2)).get();
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

    @Test
    void globalSnapshotReferenceIndexProtectsSnapshotsAcrossWorkspaces() {
        OpenSandboxWorkspaceRecord actual = record(4);
        actual.setNativeSnapshotId("snapshot-current");
        actual.setPreviousNativeSnapshotId("snapshot-shared");
        when(snapshotReferences.readAllMap())
                .thenReturn(Map.of(WORKSPACE_ID, "snapshot-current\0snapshot-shared"));
        try {
            when(bucket.get())
                    .thenReturn(
                            new ObjectMapper().findAndRegisterModules().writeValueAsString(actual));
        } catch (Exception error) {
            throw new AssertionError(error);
        }

        assertTrue(store.isSnapshotReferenced("snapshot-shared"));
        assertFalse(store.isSnapshotReferenced("snapshot-unreferenced"));
        verify(bucket, never()).get();
    }

    @Test
    void staleSnapshotReferenceRemainsConservativeUntilWorkspaceRepairReconcilesIt() {
        when(snapshotReferences.readAllMap()).thenReturn(Map.of(WORKSPACE_ID, "snapshot-stale"));
        when(bucket.get()).thenReturn(null);

        assertTrue(store.isSnapshotReferenced("snapshot-stale"));
        verify(bucket, never()).get();

        store.reconcileSnapshotReferences(WORKSPACE_ID);

        verify(bucket).get();
        verify(snapshotReferences).remove(WORKSPACE_ID);
    }

    @Test
    @SuppressWarnings("unchecked")
    void referenceCheckDuringWorkspaceCasSeesConservativePrewriteWithoutReconciliation()
            throws Exception {
        RedissonClient localRedisson = mock(RedissonClient.class);
        RBucket<String> localBucket = mock(RBucket.class);
        RScoredSortedSet<String> localRepairs = mock(RScoredSortedSet.class);
        RMap<String, String> localReferences = mock(RMap.class);
        RLock localReferencesLock = mock(RLock.class);
        ReentrantLock backingLock = new ReentrantLock();
        org.mockito.Mockito.doAnswer(
                        ignored -> {
                            backingLock.lock();
                            return null;
                        })
                .when(localReferencesLock)
                .lock();
        org.mockito.Mockito.doAnswer(
                        ignored -> {
                            backingLock.unlock();
                            return null;
                        })
                .when(localReferencesLock)
                .unlock();
        doReturn(localBucket)
                .when(localRedisson)
                .getBucket(
                        "agentscope:opensandbox:{" + WORKSPACE_ID + "}:record",
                        StringCodec.INSTANCE);
        doReturn(localRepairs)
                .when(localRedisson)
                .getScoredSortedSet(
                        OpenSandboxWorkspaceStore.WORKSPACE_REPAIR_INDEX_KEY, StringCodec.INSTANCE);
        doReturn(localReferences)
                .when(localRedisson)
                .getMap(OpenSandboxWorkspaceStore.SNAPSHOT_REFERENCES_KEY, StringCodec.INSTANCE);
        when(localRedisson.getLock("agentscope:opensandbox:{snapshot-references}:lock:v1"))
                .thenReturn(localReferencesLock);
        Map<String, String> references = new ConcurrentHashMap<>();
        when(localReferences.get(anyString()))
                .thenAnswer(invocation -> references.get(invocation.getArgument(0)));
        when(localReferences.put(anyString(), anyString()))
                .thenAnswer(
                        invocation ->
                                references.put(
                                        invocation.getArgument(0), invocation.getArgument(1)));
        when(localReferences.remove(anyString()))
                .thenAnswer(invocation -> references.remove(invocation.getArgument(0)));
        when(localReferences.readAllMap()).thenAnswer(ignored -> new HashMap<>(references));
        OpenSandboxWorkspaceRecord expected = record(4);
        expected.setNativeSnapshotId("snapshot-old");
        OpenSandboxWorkspaceRecord update = expected.copy();
        update.setNativeSnapshotId("snapshot-new");
        String expectedJson =
                new ObjectMapper().findAndRegisterModules().writeValueAsString(expected);
        when(localBucket.get()).thenReturn(expectedJson);
        CountDownLatch casEntered = new CountDownLatch(1);
        CountDownLatch releaseCas = new CountDownLatch(1);
        when(localBucket.compareAndSet(anyString(), anyString()))
                .thenAnswer(
                        ignored -> {
                            casEntered.countDown();
                            assertTrue(releaseCas.await(5, TimeUnit.SECONDS));
                            return true;
                        });
        OpenSandboxWorkspaceStore localStore = new OpenSandboxWorkspaceStore(localRedisson);
        CompletableFuture<Boolean> updateFuture =
                CompletableFuture.supplyAsync(() -> localStore.compareAndSet(expected, update));
        assertTrue(casEntered.await(5, TimeUnit.SECONDS));
        CompletableFuture<Boolean> referenceCheck =
                CompletableFuture.supplyAsync(
                        () -> localStore.isSnapshotReferenced("snapshot-new"));
        boolean referenced;
        try {
            referenced = referenceCheck.get(2, TimeUnit.SECONDS);
        } finally {
            releaseCas.countDown();
            assertTrue(updateFuture.get(5, TimeUnit.SECONDS));
        }

        assertTrue(referenced);
        verify(localBucket, never()).get();
    }

    @Test
    void postCommitReferenceFailureIsReconciledByRestartedStore() throws Exception {
        OpenSandboxWorkspaceRecord expected = record(4);
        expected.setNativeSnapshotId("snapshot-current");
        OpenSandboxWorkspaceRecord update = expected.copy();
        String expectedJson =
                new ObjectMapper().findAndRegisterModules().writeValueAsString(expected);
        String updateJson = new ObjectMapper().findAndRegisterModules().writeValueAsString(update);
        when(bucket.compareAndSet(expectedJson, updateJson)).thenReturn(true);
        when(bucket.get()).thenReturn(updateJson);
        when(snapshotReferences.get(WORKSPACE_ID)).thenReturn("snapshot-current\0snapshot-stale");
        AtomicInteger writes = new AtomicInteger();
        when(snapshotReferences.put(eq(WORKSPACE_ID), anyString()))
                .thenAnswer(
                        invocation -> {
                            if (writes.incrementAndGet() == 2) {
                                throw new IllegalStateException("reference write failed");
                            }
                            return null;
                        });

        assertThrows(IllegalStateException.class, () -> store.compareAndSet(expected, update));
        verify(repairs).addIfAbsent(0.0, WORKSPACE_ID);

        OpenSandboxWorkspaceStore restarted = new OpenSandboxWorkspaceStore(redisson);
        restarted.reconcileSnapshotReferences(WORKSPACE_ID);

        ArgumentCaptor<String> references = ArgumentCaptor.forClass(String.class);
        verify(snapshotReferences, org.mockito.Mockito.times(3))
                .put(eq(WORKSPACE_ID), references.capture());
        assertEquals("snapshot-current", references.getAllValues().get(2));
    }

    @Test
    void successfulRecordCasIndexesCurrentAndPreviousSnapshots() {
        OpenSandboxWorkspaceRecord record = record(4);
        record.setNativeSnapshotId("snapshot-current");
        record.setPreviousNativeSnapshotId("snapshot-previous");
        when(bucket.compareAndSet(isNull(), any(String.class))).thenReturn(true);

        assertTrue(store.compareAndSet(null, record));

        ArgumentCaptor<String> references = ArgumentCaptor.forClass(String.class);
        verify(snapshotReferences, times(2)).put(eq(WORKSPACE_ID), references.capture());
        String finalReferences = references.getAllValues().get(1);
        assertTrue(finalReferences.contains("snapshot-current"));
        assertTrue(finalReferences.contains("snapshot-previous"));
        verify(repairs).addIfAbsent(0.0, WORKSPACE_ID);
    }

    @Test
    void failedRecordCasRebuildsSnapshotReferencesFromActualRecord() throws Exception {
        OpenSandboxWorkspaceRecord actual = record(4);
        actual.setNativeSnapshotId("snapshot-actual");
        OpenSandboxWorkspaceRecord update = actual.copy();
        update.setNativeSnapshotId("snapshot-uncommitted");
        String actualJson = new ObjectMapper().findAndRegisterModules().writeValueAsString(actual);
        when(bucket.compareAndSet(any(String.class), any(String.class))).thenReturn(false);
        when(bucket.get()).thenReturn(actualJson);

        assertFalse(store.compareAndSet(actual, update));

        ArgumentCaptor<String> references = ArgumentCaptor.forClass(String.class);
        verify(snapshotReferences, org.mockito.Mockito.times(2))
                .put(eq(WORKSPACE_ID), references.capture());
        assertTrue(references.getAllValues().get(0).contains("snapshot-uncommitted"));
        assertEquals("snapshot-actual", references.getAllValues().get(1));
        verify(repairs).addIfAbsent(0.0, WORKSPACE_ID);
    }

    @Test
    void exceptionalRecordCasStillRebuildsSnapshotReferences() throws Exception {
        OpenSandboxWorkspaceRecord actual = record(4);
        actual.setNativeSnapshotId("snapshot-actual");
        OpenSandboxWorkspaceRecord update = actual.copy();
        update.setNativeSnapshotId("snapshot-uncommitted");
        String actualJson = new ObjectMapper().findAndRegisterModules().writeValueAsString(actual);
        IllegalStateException failure = new IllegalStateException("redis write failed");
        when(bucket.compareAndSet(any(String.class), any(String.class))).thenThrow(failure);
        when(bucket.get()).thenReturn(actualJson);

        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, () -> store.compareAndSet(actual, update));

        ArgumentCaptor<String> references = ArgumentCaptor.forClass(String.class);
        verify(snapshotReferences, org.mockito.Mockito.times(2))
                .put(eq(WORKSPACE_ID), references.capture());
        assertEquals("snapshot-actual", references.getAllValues().get(1));
        verify(repairs).addIfAbsent(0.0, WORKSPACE_ID);
    }

    @Test
    void claimedIndexesAreRescoredAndMalformedOrphansAreIsolated() {
        Instant due = Instant.parse("2026-08-08T09:00:00Z");
        Instant retry = Instant.parse("2026-08-08T09:05:00Z");
        when(idle.valueRange(
                        anyDouble(), anyBoolean(), anyDouble(), anyBoolean(), anyInt(), anyInt()))
                .thenReturn(List.of(WORKSPACE_ID));
        when(orphanSnapshots.valueRange(
                        anyDouble(), anyBoolean(), anyDouble(), anyBoolean(), anyInt(), anyInt()))
                .thenReturn(List.of("malformed", WORKSPACE_ID + "\0snapshot-1"));

        assertEquals(List.of(WORKSPACE_ID), store.claimDueIdle(due, retry, 100));
        assertEquals(
                List.of(new OpenSandboxWorkspaceStore.OrphanReference(WORKSPACE_ID, "snapshot-1")),
                store.claimDueOrphanSnapshots(due, retry, 100));

        verify(idle).add((double) retry.toEpochMilli(), WORKSPACE_ID);
        verify(orphanSnapshots).remove("malformed");
        verify(orphanSnapshots).add((double) retry.toEpochMilli(), WORKSPACE_ID + "\0snapshot-1");
    }

    @Test
    @SuppressWarnings("unchecked")
    void successfulRepairCasPreservesClaimScoreSoNextBatchCanAdvance() {
        RedissonClient localRedisson = mock(RedissonClient.class);
        RScoredSortedSet<String> localRepairs = mock(RScoredSortedSet.class);
        RMap<String, String> localReferences = mock(RMap.class);
        RLock localReferencesLock = mock(RLock.class);
        Map<String, Double> repairScores = new HashMap<>();
        Map<String, RBucket<String>> buckets = new HashMap<>();
        doReturn(localRepairs)
                .when(localRedisson)
                .getScoredSortedSet(
                        OpenSandboxWorkspaceStore.WORKSPACE_REPAIR_INDEX_KEY, StringCodec.INSTANCE);
        doReturn(localReferences)
                .when(localRedisson)
                .getMap(OpenSandboxWorkspaceStore.SNAPSHOT_REFERENCES_KEY, StringCodec.INSTANCE);
        when(localRedisson.getLock(OpenSandboxWorkspaceStore.SNAPSHOT_REFERENCES_LOCK_KEY))
                .thenReturn(localReferencesLock);
        when(localRedisson.getBucket(anyString(), eq(StringCodec.INSTANCE)))
                .thenAnswer(
                        invocation -> {
                            String key = invocation.getArgument(0);
                            RBucket<String> localBucket = buckets.get(key);
                            if (localBucket != null) {
                                return localBucket;
                            }
                            localBucket = mock(RBucket.class);
                            when(localBucket.compareAndSet(isNull(), anyString())).thenReturn(true);
                            buckets.put(key, localBucket);
                            return localBucket;
                        });
        when(localRepairs.add(anyDouble(), anyString()))
                .thenAnswer(
                        invocation -> {
                            repairScores.put(invocation.getArgument(1), invocation.getArgument(0));
                            return true;
                        });
        when(localRepairs.addIfAbsent(anyDouble(), anyString()))
                .thenAnswer(
                        invocation ->
                                repairScores.putIfAbsent(
                                                invocation.getArgument(1),
                                                invocation.getArgument(0))
                                        == null);
        when(localRepairs.valueRange(
                        anyDouble(), anyBoolean(), anyDouble(), anyBoolean(), anyInt(), anyInt()))
                .thenAnswer(
                        invocation -> {
                            double maximum = invocation.getArgument(2);
                            int offset = invocation.getArgument(4);
                            int count = invocation.getArgument(5);
                            return repairScores.entrySet().stream()
                                    .filter(entry -> entry.getValue() <= maximum)
                                    .sorted(
                                            Comparator.<Map.Entry<String, Double>>comparingDouble(
                                                            Map.Entry::getValue)
                                                    .thenComparing(Map.Entry::getKey))
                                    .skip(offset)
                                    .limit(count)
                                    .map(Map.Entry::getKey)
                                    .toList();
                        });
        OpenSandboxWorkspaceStore localStore = new OpenSandboxWorkspaceStore(localRedisson);
        Instant now = Instant.parse("2026-08-08T09:00:00Z");
        Instant retryAt = now.plusSeconds(300);
        for (int index = 0; index <= OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE; index++) {
            localStore.scheduleRepair(String.format("workspace-%03d", index), now);
        }

        List<String> firstBatch =
                localStore.claimDueRepairs(
                        now, retryAt, OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE);
        for (String workspaceId : firstBatch) {
            OpenSandboxWorkspaceRecord update = record(3);
            update.setWorkspaceId(workspaceId);
            assertTrue(localStore.compareAndSet(null, update));
        }

        assertEquals(
                List.of("workspace-100"),
                localStore.claimDueRepairs(
                        now, retryAt, OpenSandboxLifecycleSweeper.SWEEP_BATCH_SIZE));
    }

    private static OpenSandboxWorkspaceRecord record(long generation) {
        OpenSandboxWorkspaceRecord record = new OpenSandboxWorkspaceRecord();
        record.setWorkspaceId(WORKSPACE_ID);
        record.setSandboxId("sandbox-" + generation);
        record.setGeneration(generation);
        record.setLifecycleState(OpenSandboxWorkspaceRecord.LifecycleState.RUNNING);
        return record;
    }

    private static String legacyRecordJson(
            long generation, String fileSecret, String environmentSecret) throws Exception {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        OpenSandboxWorkspaceRecord legacy = record(generation);
        legacy.setSchemaVersion(1);
        ObjectNode tree = mapper.valueToTree(legacy);
        tree.put(
                "serializedSandboxState",
                "{\"manifest\":{\"files\":[{\"content\":\""
                        + fileSecret
                        + "\"}],\"environment\":{\"API_TOKEN\":\""
                        + environmentSecret
                        + "\"}}}");
        return mapper.writeValueAsString(tree);
    }
}
