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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RMapCache;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/** Redisson persistence and coordination primitives for OpenSandbox workspace lifecycle. */
public final class OpenSandboxWorkspaceStore implements AutoCloseable {
    static final String IDLE_INDEX_KEY = "agentscope:opensandbox:idle:v1";
    static final String ORPHAN_SANDBOX_INDEX_KEY = "agentscope:opensandbox:orphan-sandboxes:v1";
    static final String ORPHAN_SNAPSHOT_INDEX_KEY = "agentscope:opensandbox:orphan-snapshots:v1";

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

    public RLock lifecycleLock(String workspaceId) {
        return redisson.getLock(lifecycleLockKey(workspaceId));
    }

    public Optional<OpenSandboxWorkspaceRecord> load(String workspaceId) {
        String json = recordBucket(workspaceId).get();
        return json == null
                ? Optional.empty()
                : Optional.of(read(json, OpenSandboxWorkspaceRecord.class));
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
        return recordBucket(workspaceId)
                .compareAndSet(
                        expected == null ? null : write(expected),
                        update == null ? null : write(update));
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

    @Override
    public void close() {
        // The injected RedissonClient remains owned by the application.
    }

    private RBucket<String> recordBucket(String workspaceId) {
        return redisson.getBucket(recordKey(workspaceId), StringCodec.INSTANCE);
    }

    private RMapCache<String, String> leases(String workspaceId) {
        return redisson.getMapCache(activeTurnsKey(workspaceId), StringCodec.INSTANCE);
    }

    private RScoredSortedSet<String> idleIndex() {
        return redisson.getScoredSortedSet(IDLE_INDEX_KEY, StringCodec.INSTANCE);
    }

    private String write(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception error) {
            throw new IllegalStateException("Failed to serialize OpenSandbox Redis state", error);
        }
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
}
