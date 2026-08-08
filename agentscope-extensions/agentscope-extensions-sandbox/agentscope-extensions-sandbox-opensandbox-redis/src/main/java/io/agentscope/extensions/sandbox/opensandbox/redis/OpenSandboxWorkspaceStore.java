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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RMapCache;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/** Redisson persistence and coordination primitives for OpenSandbox workspace lifecycle. */
public final class OpenSandboxWorkspaceStore implements AutoCloseable {
    private static final int RECORD_MIGRATION_ATTEMPTS = 3;
    private static final String LEGACY_STATE_FIELD = "serializedSandboxState";

    static final String IDLE_INDEX_KEY = "agentscope:opensandbox:idle:v1";
    static final String ORPHAN_SANDBOX_INDEX_KEY = "agentscope:opensandbox:orphan-sandboxes:v1";
    static final String ORPHAN_SNAPSHOT_INDEX_KEY = "agentscope:opensandbox:orphan-snapshots:v1";
    static final String SNAPSHOT_REFERENCES_KEY = "agentscope:opensandbox:{snapshot-references}:v1";
    static final String SNAPSHOT_REFERENCES_LOCK_KEY =
            "agentscope:opensandbox:{snapshot-references}:lock:v1";
    static final String WORKSPACE_REPAIR_INDEX_KEY =
            "agentscope:opensandbox:{workspace-repairs}:v1";

    private final RedissonClient redisson;
    private final ObjectMapper objectMapper;

    public OpenSandboxWorkspaceStore(RedissonClient redisson) {
        this(redisson, new ObjectMapper().findAndRegisterModules());
    }

    OpenSandboxWorkspaceStore(RedissonClient redisson, ObjectMapper objectMapper) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    }

    public String recordKey(String workspaceId) {
        return workspacePrefix(workspaceId) + ":record";
    }

    public String activeTurnsKey(String workspaceId) {
        return workspacePrefix(workspaceId) + ":active-turns";
    }

    public String lifecycleLockKey(String workspaceId) {
        return workspacePrefix(workspaceId) + ":lifecycle-lock";
    }

    public String deletedThroughKey(String workspaceId) {
        return workspacePrefix(workspaceId) + ":deleted-through";
    }

    public RLock lifecycleLock(String workspaceId) {
        return redisson.getLock(lifecycleLockKey(workspaceId));
    }

    public Optional<OpenSandboxWorkspaceRecord> load(String workspaceId) {
        RBucket<String> bucket = recordBucket(workspaceId);
        for (int attempt = 0; attempt < RECORD_MIGRATION_ATTEMPTS; attempt++) {
            String json = bucket.get();
            if (json == null) {
                return Optional.empty();
            }
            JsonNode tree = readTree(json);
            OpenSandboxWorkspaceRecord record;
            if (requiresMigration(tree)) {
                record = migrateRecord(tree);
                if (!bucket.compareAndSet(json, write(record))) {
                    continue;
                }
            } else {
                record = readTree(tree, OpenSandboxWorkspaceRecord.class);
            }
            mergeSnapshotReferences(record.getWorkspaceId(), snapshotIds(record));
            return Optional.of(record);
        }
        throw new IllegalStateException(
                "OpenSandbox Redis record changed repeatedly during schema migration");
    }

    public boolean compareAndSet(
            OpenSandboxWorkspaceRecord expected, OpenSandboxWorkspaceRecord update) {
        if (expected == null && update == null) {
            throw new IllegalArgumentException("expected and update must not both be null");
        }
        String workspaceId =
                expected != null ? requireWorkspaceId(expected) : requireWorkspaceId(update);
        if (expected != null && update != null && !workspaceId.equals(requireWorkspaceId(update))) {
            throw new IllegalArgumentException("expected and update workspaceId must match");
        }
        LinkedHashSet<String> conservative = new LinkedHashSet<>(snapshotIds(expected));
        conservative.addAll(snapshotIds(update));
        if (update != null) {
            workspaceRepairIndex().addIfAbsent(0.0, workspaceId);
        }
        mergeSnapshotReferences(workspaceId, conservative);
        boolean changed;
        try {
            changed =
                    recordBucket(workspaceId)
                            .compareAndSet(
                                    expected == null ? null : write(expected),
                                    update == null ? null : write(update));
        } catch (RuntimeException failure) {
            reconcileSnapshotReferencesAfterFailedCas(workspaceId, failure);
            throw failure;
        }
        if (!changed) {
            reconcileSnapshotReferencesAfterFailedCas(workspaceId, null);
            return false;
        }
        LinkedHashSet<String> finalReferences = new LinkedHashSet<>(snapshotIds(update));
        replaceSnapshotReferences(workspaceId, finalReferences);
        return true;
    }

    public long deletedThroughGeneration(String workspaceId) {
        String value = deletedThroughBucket(workspaceId).get();
        return value == null ? 0 : Long.parseLong(value);
    }

    public long advanceDeletedThroughGeneration(String workspaceId, long generation) {
        if (generation < 0) {
            throw new IllegalArgumentException("generation must not be negative");
        }
        RBucket<String> bucket = deletedThroughBucket(workspaceId);
        while (true) {
            String current = bucket.get();
            long currentGeneration = current == null ? 0 : Long.parseLong(current);
            if (currentGeneration >= generation) {
                return currentGeneration;
            }
            if (bucket.compareAndSet(current, Long.toString(generation))) {
                return generation;
            }
        }
    }

    public void putLease(OpenSandboxActiveLease lease, Duration ttl) {
        Objects.requireNonNull(lease, "lease");
        long ttlMillis = positiveMillis(ttl, "ttl");
        leases(lease.workspaceId())
                .put(lease.leaseId(), write(lease), ttlMillis, TimeUnit.MILLISECONDS);
    }

    public void removeLease(String workspaceId, String leaseId) {
        leases(workspaceId).fastRemove(requireText(leaseId, "leaseId"));
    }

    public void clearLeases(String workspaceId) {
        leases(workspaceId).delete();
    }

    public List<OpenSandboxActiveLease> activeLeases(String workspaceId, long generation) {
        if (generation < 0) {
            throw new IllegalArgumentException("generation must not be negative");
        }
        List<OpenSandboxActiveLease> result = new ArrayList<>();
        for (String json : leases(workspaceId).readAllMap().values()) {
            OpenSandboxActiveLease lease = read(json, OpenSandboxActiveLease.class);
            if (lease.generation() == generation) {
                result.add(lease);
            }
        }
        return List.copyOf(result);
    }

    public void scheduleIdle(String workspaceId, Instant dueAt) {
        idleIndex()
                .add(
                        (double) Objects.requireNonNull(dueAt, "dueAt").toEpochMilli(),
                        requireText(workspaceId, "workspaceId"));
    }

    public void cancelIdle(String workspaceId) {
        idleIndex().remove(requireText(workspaceId, "workspaceId"));
    }

    public List<String> dueIdle(Instant now, int limit) {
        Objects.requireNonNull(now, "now");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        return List.copyOf(
                idleIndex()
                        .valueRange(
                                Double.NEGATIVE_INFINITY,
                                true,
                                (double) now.toEpochMilli(),
                                true,
                                0,
                                limit));
    }

    public List<String> claimDueIdle(Instant now, Instant retryAt, int limit) {
        List<String> claimed = dueIdle(now, limit);
        claimed.forEach(workspaceId -> scheduleIdle(workspaceId, retryAt));
        return claimed;
    }

    public boolean hasIdle(String workspaceId) {
        return idleIndex().contains(requireText(workspaceId, "workspaceId"));
    }

    public void scheduleRepair(String workspaceId, Instant dueAt) {
        workspaceRepairIndex()
                .add(
                        (double) Objects.requireNonNull(dueAt, "dueAt").toEpochMilli(),
                        requireText(workspaceId, "workspaceId"));
    }

    public List<String> claimDueRepairs(Instant now, Instant retryAt, int limit) {
        List<String> claimed = dueValues(workspaceRepairIndex(), now, limit);
        claimed.forEach(workspaceId -> scheduleRepair(workspaceId, retryAt));
        return claimed;
    }

    public void cancelRepair(String workspaceId) {
        workspaceRepairIndex().remove(requireText(workspaceId, "workspaceId"));
    }

    public void markOrphanSandbox(String workspaceId, String sandboxId, Instant discoveredAt) {
        orphanSandboxIndex()
                .add(
                        (double)
                                Objects.requireNonNull(discoveredAt, "discoveredAt").toEpochMilli(),
                        orphanValue(workspaceId, sandboxId));
    }

    public void markOrphanSnapshot(String workspaceId, String snapshotId, Instant discoveredAt) {
        orphanSnapshotIndex()
                .add(
                        (double)
                                Objects.requireNonNull(discoveredAt, "discoveredAt").toEpochMilli(),
                        orphanValue(workspaceId, snapshotId));
    }

    public List<OrphanReference> dueOrphanSandboxes(Instant before, int limit) {
        return dueOrphans(orphanSandboxIndex(), before, limit);
    }

    public List<OrphanReference> dueOrphanSnapshots(Instant before, int limit) {
        return dueOrphans(orphanSnapshotIndex(), before, limit);
    }

    public List<OrphanReference> claimDueOrphanSandboxes(
            Instant before, Instant retryAt, int limit) {
        return claimDueOrphans(orphanSandboxIndex(), before, retryAt, limit);
    }

    public List<OrphanReference> claimDueOrphanSnapshots(
            Instant before, Instant retryAt, int limit) {
        return claimDueOrphans(orphanSnapshotIndex(), before, retryAt, limit);
    }

    public void removeOrphanSandbox(OrphanReference orphan) {
        orphanSandboxIndex().remove(orphanValue(orphan.workspaceId(), orphan.remoteId()));
    }

    public void removeOrphanSnapshot(OrphanReference orphan) {
        orphanSnapshotIndex().remove(orphanValue(orphan.workspaceId(), orphan.remoteId()));
    }

    public boolean isSnapshotReferenced(String snapshotId) {
        String required = requireText(snapshotId, "snapshotId");
        RLock lock = snapshotReferencesLock();
        lock.lock();
        try {
            return snapshotReferences().readAllMap().values().stream()
                    .anyMatch(value -> decodeSnapshotReferences(value).contains(required));
        } finally {
            lock.unlock();
        }
    }

    public void reconcileSnapshotReferences(String workspaceId) {
        String required = requireText(workspaceId, "workspaceId");
        RLock lock = snapshotReferencesLock();
        lock.lock();
        try {
            RBucket<String> bucket = recordBucket(required);
            for (int attempt = 0; attempt < RECORD_MIGRATION_ATTEMPTS; attempt++) {
                String json = bucket.get();
                if (json == null) {
                    replaceSnapshotReferencesLocked(required, List.of());
                    return;
                }
                JsonNode tree = readTree(json);
                OpenSandboxWorkspaceRecord actual;
                if (requiresMigration(tree)) {
                    actual = migrateRecord(tree);
                    if (!bucket.compareAndSet(json, write(actual))) {
                        continue;
                    }
                } else {
                    actual = readTree(tree, OpenSandboxWorkspaceRecord.class);
                }
                replaceSnapshotReferencesLocked(required, snapshotIds(actual));
                return;
            }
            throw new IllegalStateException(
                    "OpenSandbox Redis record changed repeatedly during schema migration");
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        // The injected RedissonClient remains owned by the application.
    }

    private RBucket<String> recordBucket(String workspaceId) {
        return redisson.getBucket(recordKey(workspaceId), StringCodec.INSTANCE);
    }

    private RBucket<String> deletedThroughBucket(String workspaceId) {
        return redisson.getBucket(deletedThroughKey(workspaceId), StringCodec.INSTANCE);
    }

    private RMapCache<String, String> leases(String workspaceId) {
        return redisson.getMapCache(activeTurnsKey(workspaceId), StringCodec.INSTANCE);
    }

    private RScoredSortedSet<String> idleIndex() {
        return redisson.getScoredSortedSet(IDLE_INDEX_KEY, StringCodec.INSTANCE);
    }

    private RScoredSortedSet<String> orphanSandboxIndex() {
        return redisson.getScoredSortedSet(ORPHAN_SANDBOX_INDEX_KEY, StringCodec.INSTANCE);
    }

    private RScoredSortedSet<String> orphanSnapshotIndex() {
        return redisson.getScoredSortedSet(ORPHAN_SNAPSHOT_INDEX_KEY, StringCodec.INSTANCE);
    }

    private RScoredSortedSet<String> workspaceRepairIndex() {
        return redisson.getScoredSortedSet(WORKSPACE_REPAIR_INDEX_KEY, StringCodec.INSTANCE);
    }

    private RMap<String, String> snapshotReferences() {
        return redisson.getMap(SNAPSHOT_REFERENCES_KEY, StringCodec.INSTANCE);
    }

    private RLock snapshotReferencesLock() {
        return redisson.getLock(SNAPSHOT_REFERENCES_LOCK_KEY);
    }

    private void mergeSnapshotReferences(String workspaceId, Iterable<String> references) {
        RLock lock = snapshotReferencesLock();
        lock.lock();
        try {
            LinkedHashSet<String> merged =
                    decodeSnapshotReferences(snapshotReferences().get(workspaceId));
            references.forEach(merged::add);
            replaceSnapshotReferencesLocked(workspaceId, merged);
        } finally {
            lock.unlock();
        }
    }

    private void replaceSnapshotReferences(String workspaceId, Iterable<String> references) {
        RLock lock = snapshotReferencesLock();
        lock.lock();
        try {
            replaceSnapshotReferencesLocked(workspaceId, references);
        } finally {
            lock.unlock();
        }
    }

    private void replaceSnapshotReferencesLocked(String workspaceId, Iterable<String> references) {
        LinkedHashSet<String> copy = new LinkedHashSet<>();
        references.forEach(copy::add);
        if (copy.isEmpty()) {
            snapshotReferences().remove(workspaceId);
        } else {
            snapshotReferences().put(workspaceId, String.join("\0", copy));
        }
    }

    private void reconcileSnapshotReferencesAfterFailedCas(
            String workspaceId, RuntimeException primary) {
        RLock lock = snapshotReferencesLock();
        lock.lock();
        try {
            String actual = recordBucket(workspaceId).get();
            if (actual == null) {
                replaceSnapshotReferencesLocked(workspaceId, List.of());
                return;
            }
            replaceSnapshotReferencesLocked(
                    workspaceId,
                    snapshotIds(readTree(readTree(actual), OpenSandboxWorkspaceRecord.class)));
        } catch (RuntimeException reconciliationFailure) {
            if (primary == null) {
                throw reconciliationFailure;
            }
            primary.addSuppressed(reconciliationFailure);
        } finally {
            lock.unlock();
        }
    }

    private static List<String> snapshotIds(OpenSandboxWorkspaceRecord record) {
        if (record == null) {
            return List.of();
        }
        List<String> result = new ArrayList<>(2);
        if (record.getNativeSnapshotId() != null && !record.getNativeSnapshotId().isBlank()) {
            result.add(record.getNativeSnapshotId());
        }
        if (record.getPreviousNativeSnapshotId() != null
                && !record.getPreviousNativeSnapshotId().isBlank()
                && !result.contains(record.getPreviousNativeSnapshotId())) {
            result.add(record.getPreviousNativeSnapshotId());
        }
        return result;
    }

    private static LinkedHashSet<String> decodeSnapshotReferences(String encoded) {
        LinkedHashSet<String> result = new LinkedHashSet<>();
        if (encoded == null || encoded.isEmpty()) {
            return result;
        }
        for (String reference : encoded.split("\0", -1)) {
            if (!reference.isBlank()) {
                result.add(reference);
            }
        }
        return result;
    }

    private static List<OrphanReference> dueOrphans(
            RScoredSortedSet<String> index, Instant before, int limit) {
        return dueValues(index, before, limit).stream()
                .map(OpenSandboxWorkspaceStore::parseOrphan)
                .toList();
    }

    private static List<OrphanReference> claimDueOrphans(
            RScoredSortedSet<String> index, Instant before, Instant retryAt, int limit) {
        List<OrphanReference> claimed = new ArrayList<>();
        for (String value : dueValues(index, before, limit)) {
            try {
                OrphanReference orphan = parseOrphan(value);
                index.add((double) retryAt.toEpochMilli(), value);
                claimed.add(orphan);
            } catch (IllegalStateException malformed) {
                index.remove(value);
            }
        }
        return List.copyOf(claimed);
    }

    private static List<String> dueValues(
            RScoredSortedSet<String> index, Instant before, int limit) {
        Objects.requireNonNull(before, "before");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        return List.copyOf(
                index.valueRange(
                        Double.NEGATIVE_INFINITY,
                        true,
                        (double) before.toEpochMilli(),
                        true,
                        0,
                        limit));
    }

    private static String orphanValue(String workspaceId, String remoteId) {
        return requireText(workspaceId, "workspaceId") + "\0" + requireText(remoteId, "remoteId");
    }

    private static OrphanReference parseOrphan(String value) {
        int separator = value.indexOf('\0');
        if (separator <= 0 || separator == value.length() - 1) {
            throw new IllegalStateException("Invalid OpenSandbox orphan index value");
        }
        return new OrphanReference(value.substring(0, separator), value.substring(separator + 1));
    }

    private String write(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception error) {
            throw new IllegalStateException("Failed to serialize OpenSandbox Redis state", error);
        }
    }

    private JsonNode readTree(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception error) {
            throw new IllegalStateException("Failed to parse OpenSandbox Redis state", error);
        }
    }

    private <T> T readTree(JsonNode tree, Class<T> type) {
        try {
            return objectMapper.treeToValue(tree, type);
        } catch (Exception error) {
            throw new IllegalStateException("Failed to deserialize OpenSandbox Redis state", error);
        }
    }

    private static boolean requiresMigration(JsonNode tree) {
        return tree.has(LEGACY_STATE_FIELD)
                || tree.path("schemaVersion").asInt(1)
                        < OpenSandboxWorkspaceRecord.CURRENT_SCHEMA_VERSION;
    }

    private OpenSandboxWorkspaceRecord migrateRecord(JsonNode tree) {
        if (!(tree instanceof ObjectNode object)) {
            throw new IllegalStateException("OpenSandbox Redis record must be a JSON object");
        }
        ObjectNode sanitized = object.deepCopy();
        sanitized.remove(LEGACY_STATE_FIELD);
        sanitized.put("schemaVersion", OpenSandboxWorkspaceRecord.CURRENT_SCHEMA_VERSION);
        return readTree(sanitized, OpenSandboxWorkspaceRecord.class);
    }

    private <T> T read(String json, Class<T> type) {
        try {
            return objectMapper.readValue(json, type);
        } catch (Exception error) {
            throw new IllegalStateException("Failed to deserialize OpenSandbox Redis state", error);
        }
    }

    private static String workspacePrefix(String workspaceId) {
        return "agentscope:opensandbox:{" + requireText(workspaceId, "workspaceId") + "}";
    }

    private static String requireWorkspaceId(OpenSandboxWorkspaceRecord record) {
        return requireText(
                Objects.requireNonNull(record, "record").getWorkspaceId(), "workspaceId");
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static long positiveMillis(Duration duration, String name) {
        Objects.requireNonNull(duration, name);
        if (duration.isNegative() || duration.isZero()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return duration.toMillis();
    }

    /** Remote object retained until its orphan grace has elapsed. */
    public record OrphanReference(String workspaceId, String remoteId) {}
}
