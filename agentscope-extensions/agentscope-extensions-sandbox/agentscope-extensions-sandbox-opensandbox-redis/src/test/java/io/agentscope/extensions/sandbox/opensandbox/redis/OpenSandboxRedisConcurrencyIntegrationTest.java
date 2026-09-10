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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandbox;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClient;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClient.NativeSnapshot;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClientOptions;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxState;
import io.agentscope.harness.agent.IsolationScope;
import io.agentscope.harness.agent.sandbox.ExecResult;
import io.agentscope.harness.agent.sandbox.Sandbox;
import io.agentscope.harness.agent.sandbox.SandboxIsolationKey;
import io.agentscope.harness.agent.sandbox.SandboxState;
import io.agentscope.harness.agent.sandbox.SandboxWorkspaceKey;
import io.agentscope.harness.agent.sandbox.WorkspaceSpec;
import io.agentscope.harness.agent.sandbox.snapshot.SandboxSnapshotSpec;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

class OpenSandboxRedisConcurrencyIntegrationTest {
    private static final Duration WAIT = Duration.ofSeconds(10);

    @Test
    void coordinatesSharedWorkspaceAcrossIndependentRedisClients() throws Exception {
        String redisUrl = System.getenv("REDIS_URL");
        Assumptions.assumeTrue(hasText(redisUrl));

        String suffix = UUID.randomUUID().toString();
        String agentId = "redis-concurrency-" + suffix;
        SandboxWorkspaceKey workspaceKey =
                SandboxWorkspaceKey.from(isolationKey("user-" + suffix, agentId), agentId);
        String workspaceId = workspaceKey.getStableId();
        WorkspaceSpec workspace = workspace("/workspace/redis-concurrency-" + suffix);
        MutableClock clock = new MutableClock(Instant.parse("2026-08-09T00:00:00Z"));
        OpenSandboxRedisLifecycleOptions lifecycle = fastLifecycle();
        FakeRemoteProvider remote = new FakeRemoteProvider(clock);
        RedissonClient redisA = null;
        RedissonClient redisB = null;
        OpenSandboxWorkspaceStore storeA = null;
        OpenSandboxWorkspaceStore storeB = null;
        ScheduledExecutorService clientSchedulerA = null;
        ScheduledExecutorService clientSchedulerB = null;
        ScheduledExecutorService sweepSchedulerA = null;
        ScheduledExecutorService sweepSchedulerB = null;
        ExecutorService workers = null;
        RedisOpenSandboxClient clientA = null;
        RedisOpenSandboxClient clientB = null;
        OpenSandboxLifecycleSweeper sweeperA = null;
        OpenSandboxLifecycleSweeper sweeperB = null;
        RedisManagedOpenSandbox first = null;
        RedisManagedOpenSandbox second = null;
        try {
            redisA = redisson(redisUrl);
            redisB = redisson(redisUrl);
            storeA = new OpenSandboxWorkspaceStore(redisA);
            storeB = new OpenSandboxWorkspaceStore(redisB);
            clientSchedulerA = Executors.newSingleThreadScheduledExecutor();
            clientSchedulerB = Executors.newSingleThreadScheduledExecutor();
            sweepSchedulerA = Executors.newSingleThreadScheduledExecutor();
            sweepSchedulerB = Executors.newSingleThreadScheduledExecutor();
            workers = Executors.newFixedThreadPool(4);
            clientA =
                    new RedisOpenSandboxClient(
                            remote,
                            storeA,
                            new OpenSandboxClientOptions(),
                            lifecycle,
                            clock,
                            clientSchedulerA,
                            "instance-a-" + suffix);
            clientB =
                    new RedisOpenSandboxClient(
                            remote,
                            storeB,
                            new OpenSandboxClientOptions(),
                            lifecycle,
                            clock,
                            clientSchedulerB,
                            "instance-b-" + suffix);
            RedisOpenSandboxClient clientARef = clientA;
            RedisOpenSandboxClient clientBRef = clientB;
            CountDownLatch borrowStart = new CountDownLatch(1);
            Future<RedisManagedOpenSandbox> firstBorrow =
                    workers.submit(
                            () -> {
                                borrowStart.await();
                                return (RedisManagedOpenSandbox)
                                        clientARef.create(workspace, null, null, workspaceKey);
                            });
            Future<RedisManagedOpenSandbox> secondBorrow =
                    workers.submit(
                            () -> {
                                borrowStart.await();
                                return (RedisManagedOpenSandbox)
                                        clientBRef.create(workspace, null, null, workspaceKey);
                            });
            borrowStart.countDown();
            first = firstBorrow.get(WAIT.toSeconds(), TimeUnit.SECONDS);
            second = secondBorrow.get(WAIT.toSeconds(), TimeUnit.SECONDS);
            RedisManagedOpenSandbox firstHandle = first;
            RedisManagedOpenSandbox secondHandle = second;

            assertEquals(1, remote.createCalls.get());
            assertEquals(first.workspaceId(), second.workspaceId());
            assertNotEquals(first.leaseId(), second.leaseId());
            assertEquals(
                    ((OpenSandboxState) first.getState()).getSandboxId(),
                    ((OpenSandboxState) second.getState()).getSandboxId());
            OpenSandboxWorkspaceRecord borrowed = storeA.load(workspaceId).orElseThrow();
            assertEquals(2, storeA.activeLeases(workspaceId, borrowed.getGeneration()).size());

            Future<ExecResult> firstExec =
                    workers.submit(() -> firstHandle.exec(null, "first-turn", 10));
            Future<ExecResult> secondExec =
                    workers.submit(() -> secondHandle.exec(null, "second-turn", 10));
            assertTrue(remote.execEntered.await(WAIT.toSeconds(), TimeUnit.SECONDS));
            assertEquals(2, remote.maxConcurrentExec.get());
            remote.execRelease.countDown();
            ExecResult firstResult = firstExec.get(WAIT.toSeconds(), TimeUnit.SECONDS);
            ExecResult secondResult = secondExec.get(WAIT.toSeconds(), TimeUnit.SECONDS);
            assertEquals(0, firstResult.exitCode());
            assertEquals(0, secondResult.exitCode());
            assertEquals("first-turn", firstResult.stdout());
            assertEquals("second-turn", secondResult.stdout());

            first.stop();
            assertFalse(storeA.hasIdle(workspaceId));
            assertEquals(1, storeA.activeLeases(workspaceId, borrowed.getGeneration()).size());
            second.stop();
            assertTrue(storeA.hasIdle(workspaceId));
            assertTrue(storeA.activeLeases(workspaceId, borrowed.getGeneration()).isEmpty());

            sweeperA =
                    new OpenSandboxLifecycleSweeper(
                            remote, storeA, lifecycle, clock, sweepSchedulerA);
            sweeperB =
                    new OpenSandboxLifecycleSweeper(
                            remote, storeB, lifecycle, clock, sweepSchedulerB);
            clock.advance(lifecycle.getIdleTtl().plusMillis(1));
            sweeperA.sweepOnce();
            assertEquals(
                    OpenSandboxWorkspaceRecord.LifecycleState.EVICTION_PENDING,
                    storeA.load(workspaceId).orElseThrow().getLifecycleState());
            assertEquals(0, remote.snapshotCalls.get());

            clock.advance(lifecycle.getEvictionGrace().plusMillis(1));
            Future<?> firstSweep = workers.submit(sweeperA::sweepOnce);
            assertTrue(remote.snapshotEntered.await(WAIT.toSeconds(), TimeUnit.SECONDS));
            assertFalse(storeB.lifecycleLock(workspaceId).tryLock(0, TimeUnit.SECONDS));
            storeB.scheduleIdle(workspaceId, clock.instant());
            Future<?> secondSweep = workers.submit(sweeperB::sweepOnce);
            secondSweep.get(WAIT.toSeconds(), TimeUnit.SECONDS);
            remote.snapshotRelease.countDown();
            firstSweep.get(WAIT.toSeconds(), TimeUnit.SECONDS);

            assertEquals(1, remote.snapshotCalls.get());
            assertEquals(1, remote.pauseCalls.get());
            OpenSandboxWorkspaceRecord paused = storeA.load(workspaceId).orElseThrow();
            assertEquals(
                    OpenSandboxWorkspaceRecord.LifecycleState.PAUSED, paused.getLifecycleState());
            assertEquals("snapshot-1", paused.getNativeSnapshotId());
        } finally {
            remote.execRelease.countDown();
            remote.snapshotRelease.countDown();
            stopQuietly(first);
            stopQuietly(second);
            if (clientA != null) clientA.close();
            if (clientB != null) clientB.close();
            if (sweeperA != null) sweeperA.close();
            if (sweeperB != null) sweeperB.close();
            if (workers != null) shutdown(workers);
            if (clientSchedulerA != null) shutdown(clientSchedulerA);
            if (clientSchedulerB != null) shutdown(clientSchedulerB);
            if (sweepSchedulerA != null) shutdown(sweepSchedulerA);
            if (sweepSchedulerB != null) shutdown(sweepSchedulerB);
            if (redisA != null && storeA != null) cleanupWorkspace(redisA, storeA, workspaceId);
            if (redisB != null && storeB != null) cleanupWorkspace(redisB, storeB, workspaceId);
            if (redisA != null) redisA.shutdown();
            if (redisB != null) redisB.shutdown();
        }
    }

    private static OpenSandboxRedisLifecycleOptions fastLifecycle() {
        OpenSandboxRedisLifecycleOptions lifecycle = new OpenSandboxRedisLifecycleOptions();
        lifecycle.setSweepInterval(Duration.ofMillis(100));
        lifecycle.setIdleTtl(Duration.ofSeconds(1));
        lifecycle.setHeartbeatInterval(Duration.ofMillis(100));
        lifecycle.setActiveLeaseTtl(Duration.ofSeconds(5));
        lifecycle.setEvictionGrace(Duration.ofMillis(200));
        lifecycle.setSnapshotReadyTimeout(Duration.ofSeconds(2));
        lifecycle.setSweeperEnabled(false);
        return lifecycle;
    }

    static RedissonClient redisson(String redisUrl) {
        String address = redisUrl.contains("://") ? redisUrl : "redis://" + redisUrl;
        Config config = new Config();
        config.useSingleServer().setAddress(address);
        return Redisson.create(config);
    }

    static void cleanupWorkspace(
            RedissonClient redisson, OpenSandboxWorkspaceStore store, String workspaceId) {
        store.cancelIdle(workspaceId);
        store.cancelRepair(workspaceId);
        store.clearLeases(workspaceId);
        redisson.getMap(OpenSandboxWorkspaceStore.SNAPSHOT_REFERENCES_KEY, StringCodec.INSTANCE)
                .fastRemove(workspaceId);
        removeOrphans(redisson, OpenSandboxWorkspaceStore.ORPHAN_SANDBOX_INDEX_KEY, workspaceId);
        removeOrphans(redisson, OpenSandboxWorkspaceStore.ORPHAN_SNAPSHOT_INDEX_KEY, workspaceId);
        redisson.getKeys()
                .delete(
                        store.recordKey(workspaceId),
                        store.activeTurnsKey(workspaceId),
                        store.deletedThroughKey(workspaceId),
                        store.lifecycleLockKey(workspaceId));
    }

    private static void removeOrphans(
            RedissonClient redisson, String indexName, String workspaceId) {
        RScoredSortedSet<String> index =
                redisson.getScoredSortedSet(indexName, StringCodec.INSTANCE);
        String prefix = workspaceId + '\0';
        index.readAll().stream().filter(value -> value.startsWith(prefix)).forEach(index::remove);
    }

    private static void stopQuietly(RedisManagedOpenSandbox sandbox) {
        if (sandbox == null) return;
        try {
            sandbox.stop();
        } catch (Exception ignored) {
            // Best effort after the test has already captured the primary failure.
        }
    }

    static void shutdown(ExecutorService executor) {
        executor.shutdownNow();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    static WorkspaceSpec workspace(String root) {
        WorkspaceSpec spec = new WorkspaceSpec();
        spec.setRoot(root);
        return spec;
    }

    static SandboxIsolationKey isolationKey(String userId, String agentId) {
        RuntimeContext context =
                RuntimeContext.builder().userId(userId).sessionId("session-" + userId).build();
        return SandboxIsolationKey.resolve(IsolationScope.USER, context, agentId).orElseThrow();
    }

    static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    static final class MutableClock extends Clock {
        private final AtomicReference<Instant> now;

        MutableClock(Instant initial) {
            now = new AtomicReference<>(initial);
        }

        void advance(Duration duration) {
            now.updateAndGet(current -> current.plus(duration));
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now.get();
        }
    }

    private static final class FakeRemoteProvider extends OpenSandboxClient {
        private final Clock clock;
        private final Map<String, OpenSandboxState> sandboxes = new ConcurrentHashMap<>();
        private final Map<String, NativeSnapshot> snapshots = new ConcurrentHashMap<>();
        private final AtomicInteger ids = new AtomicInteger();
        private final AtomicInteger activeExec = new AtomicInteger();
        private final AtomicInteger createCalls = new AtomicInteger();
        private final AtomicInteger snapshotCalls = new AtomicInteger();
        private final AtomicInteger pauseCalls = new AtomicInteger();
        private final AtomicInteger maxConcurrentExec = new AtomicInteger();
        private final CountDownLatch execEntered = new CountDownLatch(2);
        private final CountDownLatch execRelease = new CountDownLatch(1);
        private final CountDownLatch snapshotEntered = new CountDownLatch(1);
        private final CountDownLatch snapshotRelease = new CountDownLatch(1);

        FakeRemoteProvider(Clock clock) {
            this.clock = clock;
        }

        @Override
        public Sandbox create(
                WorkspaceSpec workspaceSpec,
                SandboxSnapshotSpec snapshotSpec,
                OpenSandboxClientOptions options) {
            createCalls.incrementAndGet();
            String sandboxId = "sandbox-" + ids.incrementAndGet();
            OpenSandboxState state = state(sandboxId, workspaceSpec, options);
            sandboxes.put(sandboxId, state);
            return handle(state);
        }

        @Override
        public Sandbox resume(SandboxState state) {
            OpenSandboxState requested = (OpenSandboxState) state;
            return handle(sandboxes.get(requested.getSandboxId()));
        }

        @Override
        public OpenSandboxState describe(String sandboxId) {
            return sandboxes.get(sandboxId);
        }

        @Override
        public List<OpenSandboxState> listByMetadata(Map<String, String> metadata) {
            return sandboxes.values().stream()
                    .filter(
                            state ->
                                    state.getMetadata().entrySet().containsAll(metadata.entrySet()))
                    .toList();
        }

        @Override
        public Instant renew(String sandboxId, Duration duration) {
            Instant expiresAt = clock.instant().plus(duration);
            sandboxes.get(sandboxId).setRemoteExpiresAt(expiresAt);
            return expiresAt;
        }

        @Override
        public String createNativeSnapshot(String sandboxId, String name) {
            snapshotCalls.incrementAndGet();
            snapshotEntered.countDown();
            await(snapshotRelease);
            NativeSnapshot snapshot =
                    new NativeSnapshot("snapshot-1", name, clock.instant(), "CREATING");
            snapshots.put(snapshot.id(), snapshot);
            return snapshot.id();
        }

        @Override
        public String waitForNativeSnapshotReady(String snapshotId, Duration readyTimeout) {
            NativeSnapshot current = snapshots.get(snapshotId);
            snapshots.put(
                    snapshotId,
                    new NativeSnapshot(current.id(), current.name(), current.createdAt(), "READY"));
            return snapshotId;
        }

        @Override
        public Map<String, NativeSnapshot> listNativeSnapshotDetailsByNamePrefix(
                String namePrefix) {
            return Map.copyOf(snapshots);
        }

        @Override
        public void pause(String sandboxId) {
            pauseCalls.incrementAndGet();
            sandboxes.get(sandboxId).setRemoteStatus("PAUSED");
        }

        @Override
        public boolean isNotFound(Throwable error) {
            return false;
        }

        private OpenSandbox handle(OpenSandboxState state) {
            OpenSandbox sandbox = mock(OpenSandbox.class);
            when(sandbox.getState()).thenReturn(state);
            when(sandbox.getWorkspaceRoot()).thenReturn(state.getWorkspaceSpec().getRoot());
            try {
                doAnswer(
                                invocation -> {
                                    int concurrent = activeExec.incrementAndGet();
                                    maxConcurrentExec.accumulateAndGet(concurrent, Math::max);
                                    execEntered.countDown();
                                    await(execRelease);
                                    activeExec.decrementAndGet();
                                    return new ExecResult(0, invocation.getArgument(1), "", false);
                                })
                        .when(sandbox)
                        .exec(any(), any(), any());
            } catch (Exception impossible) {
                throw new IllegalStateException(impossible);
            }
            return sandbox;
        }

        private OpenSandboxState state(
                String sandboxId, WorkspaceSpec workspaceSpec, OpenSandboxClientOptions options) {
            OpenSandboxState state = new OpenSandboxState();
            state.setSandboxId(sandboxId);
            state.setSandboxOwned(true);
            state.setWorkspaceSpec(workspaceSpec.copy());
            state.setWorkspaceRootReady(true);
            state.setImage(options.getImage());
            state.setMetadata(options.getMetadata());
            state.setRemoteStatus("RUNNING");
            state.setRemoteCreatedAt(clock.instant());
            state.setRemoteExpiresAt(clock.instant().plus(Duration.ofMinutes(10)));
            return state;
        }

        private static void await(CountDownLatch latch) {
            try {
                if (!latch.await(WAIT.toSeconds(), TimeUnit.SECONDS)) {
                    throw new IllegalStateException("timed out waiting for test coordination");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("test coordination interrupted", interrupted);
            }
        }
    }
}
