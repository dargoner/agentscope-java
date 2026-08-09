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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.extensions.sandbox.opensandbox.OpenSandbox;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClient;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClient.NativeSnapshot;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxClientOptions;
import io.agentscope.extensions.sandbox.opensandbox.OpenSandboxState;
import io.agentscope.harness.agent.sandbox.ExecResult;
import io.agentscope.harness.agent.sandbox.Sandbox;
import io.agentscope.harness.agent.sandbox.SandboxIsolationKey;
import io.agentscope.harness.agent.sandbox.WorkspaceSpec;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

class OpenSandboxRedisRealServiceIntegrationTest {
    private static final Duration FUTURE_TIMEOUT = Duration.ofMinutes(2);

    @Test
    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    void realServiceSnapshotsPausesAndRestoresSharedWorkspace() throws Exception {
        String endpoint = System.getenv("OPEN_SANDBOX_ENDPOINT");
        String apiKey = System.getenv("OPEN_SANDBOX_API_KEY");
        String redisUrl = System.getenv("REDIS_URL");
        Assumptions.assumeTrue(OpenSandboxRedisConcurrencyIntegrationTest.hasText(endpoint));
        Assumptions.assumeTrue(OpenSandboxRedisConcurrencyIntegrationTest.hasText(apiKey));
        Assumptions.assumeTrue(OpenSandboxRedisConcurrencyIntegrationTest.hasText(redisUrl));

        String suffix = UUID.randomUUID().toString();
        String agentId = "redis-real-service-" + suffix;
        SandboxIsolationKey isolationKey =
                OpenSandboxRedisConcurrencyIntegrationTest.isolationKey("user-" + suffix, agentId);
        String workspaceId = RedisOpenSandboxClient.workspaceId(isolationKey, agentId);
        WorkspaceSpec workspace =
                OpenSandboxRedisConcurrencyIntegrationTest.workspace(
                        "/workspace/redis-real-service-" + suffix);
        String binaryPath = workspace.getRoot() + "/payload.bin";
        byte[] payload = new byte[] {0, 1, 2, 3, (byte) 128, (byte) 255, 10, 13};
        OpenSandboxClientOptions options = options(endpoint, apiKey);
        OpenSandboxRedisLifecycleOptions lifecycle = lifecycle();
        OpenSandboxRedisConcurrencyIntegrationTest.MutableClock clock =
                new OpenSandboxRedisConcurrencyIntegrationTest.MutableClock(Instant.now());
        RedissonClient redisA = null;
        RedissonClient redisB = null;
        OpenSandboxWorkspaceStore storeA = null;
        OpenSandboxWorkspaceStore storeB = null;
        OpenSandboxClient remoteA = null;
        OpenSandboxClient remoteB = null;
        ScheduledExecutorService clientSchedulerA = null;
        ScheduledExecutorService clientSchedulerB = null;
        ScheduledExecutorService sweepScheduler = null;
        ExecutorService workers = null;
        RedisOpenSandboxClient clientA = null;
        RedisOpenSandboxClient clientB = null;
        OpenSandboxLifecycleSweeper sweeper = null;
        RedisManagedOpenSandbox first = null;
        RedisManagedOpenSandbox second = null;
        RedisManagedOpenSandbox resumed = null;
        RedisManagedOpenSandbox restored = null;
        String stage = "create-first-handle";
        Throwable primaryFailure = null;
        try {
            redisA = OpenSandboxRedisConcurrencyIntegrationTest.redisson(redisUrl);
            redisB = OpenSandboxRedisConcurrencyIntegrationTest.redisson(redisUrl);
            storeA = new OpenSandboxWorkspaceStore(redisA);
            storeB = new OpenSandboxWorkspaceStore(redisB);
            remoteA = new OpenSandboxClient(options, null);
            remoteB = new OpenSandboxClient(options, null);
            clientSchedulerA = Executors.newSingleThreadScheduledExecutor();
            clientSchedulerB = Executors.newSingleThreadScheduledExecutor();
            sweepScheduler = Executors.newSingleThreadScheduledExecutor();
            workers = Executors.newFixedThreadPool(2);
            clientA =
                    new RedisOpenSandboxClient(
                            remoteA,
                            storeA,
                            options,
                            lifecycle,
                            clock,
                            clientSchedulerA,
                            "real-a-" + suffix);
            clientB =
                    new RedisOpenSandboxClient(
                            remoteB,
                            storeB,
                            options,
                            lifecycle,
                            clock,
                            clientSchedulerB,
                            "real-b-" + suffix);
            sweeper =
                    new OpenSandboxLifecycleSweeper(
                            remoteA, storeA, lifecycle, clock, sweepScheduler);
            first =
                    (RedisManagedOpenSandbox)
                            clientA.create(workspace, null, null, isolationKey, agentId);
            first.start();
            String originalSandboxId = ((OpenSandboxState) first.getState()).getSandboxId();

            stage = "create-second-handle";
            second =
                    (RedisManagedOpenSandbox)
                            clientB.create(workspace, null, null, isolationKey, agentId);
            second.start();
            assertEquals(originalSandboxId, ((OpenSandboxState) second.getState()).getSandboxId());
            assertNotEquals(first.leaseId(), second.leaseId());

            stage = "parallel-exec";
            RedisManagedOpenSandbox firstHandle = first;
            RedisManagedOpenSandbox secondHandle = second;
            CountDownLatch execStart = new CountDownLatch(1);
            String rendezvous = workspace.getRoot() + "/.agentscope-overlap-" + suffix;
            String firstReady = rendezvous + "/first-ready";
            String secondReady = rendezvous + "/second-ready";
            Future<ExecResult> firstExec =
                    workers.submit(
                            () -> {
                                execStart.await();
                                return firstHandle.exec(
                                        null,
                                        "mkdir -p "
                                                + shellQuote(rendezvous)
                                                + "; touch "
                                                + shellQuote(firstReady)
                                                + "; while [ ! -f "
                                                + shellQuote(secondReady)
                                                + " ]; do sleep 0.1; done; printf 'real-first'",
                                        30);
                            });
            Future<ExecResult> secondExec =
                    workers.submit(
                            () -> {
                                execStart.await();
                                return secondHandle.exec(
                                        null,
                                        "mkdir -p "
                                                + shellQuote(rendezvous)
                                                + "; touch "
                                                + shellQuote(secondReady)
                                                + "; while [ ! -f "
                                                + shellQuote(firstReady)
                                                + " ]; do sleep 0.1; done; printf 'real-second'",
                                        30);
                            });
            execStart.countDown();
            assertEquals(
                    "real-first",
                    firstExec.get(FUTURE_TIMEOUT.toSeconds(), TimeUnit.SECONDS).stdout());
            assertEquals(
                    "real-second",
                    secondExec.get(FUTURE_TIMEOUT.toSeconds(), TimeUnit.SECONDS).stdout());

            stage = "binary-upload-download";
            first.uploadFile(binaryPath, payload);
            assertArrayEquals(payload, second.downloadFile(binaryPath));

            stage = "release-first-handle";
            first.stop();
            assertFalse(storeA.hasIdle(workspaceId));
            stage = "release-last-handle";
            second.stop();
            assertTrue(storeA.hasIdle(workspaceId));

            stage = "redis-credential-scan";
            String storedRecord =
                    redisA.<String>getBucket(storeA.recordKey(workspaceId), StringCodec.INSTANCE)
                            .get();
            assertNotNull(storedRecord);
            assertFalse(storedRecord.contains(apiKey));
            assertTrue(
                    remoteA.describe(originalSandboxId).getMetadata().values().stream()
                            .noneMatch(value -> value.contains(apiKey)));

            stage = "manual-sweep-eviction-grace";
            clock.advance(lifecycle.getIdleTtl().plusMillis(1));
            sweeper.sweepOnce();
            assertEquals(
                    OpenSandboxWorkspaceRecord.LifecycleState.EVICTION_PENDING,
                    storeA.load(workspaceId).orElseThrow().getLifecycleState());

            stage = "manual-sweep-snapshot-ready-and-pause";
            clock.advance(lifecycle.getEvictionGrace().plusMillis(1));
            sweeper.sweepOnce();
            OpenSandboxWorkspaceRecord paused = storeA.load(workspaceId).orElseThrow();
            assertEquals(
                    OpenSandboxWorkspaceRecord.LifecycleState.PAUSED, paused.getLifecycleState());
            assertNotNull(paused.getNativeSnapshotId());
            NativeSnapshot ready =
                    remoteA.listNativeSnapshotDetailsByNamePrefix(
                                    OpenSandboxLifecycleSweeper.snapshotNamePrefix(workspaceId))
                            .get(paused.getNativeSnapshotId());
            assertNotNull(ready);
            assertEquals("READY", ready.status().toUpperCase());
            assertEquals("PAUSED", remoteA.describe(originalSandboxId).getRemoteStatus());

            stage = "resume-paused-original";
            resumed =
                    (RedisManagedOpenSandbox)
                            clientA.create(workspace, null, null, isolationKey, agentId);
            resumed.start();
            assertEquals(originalSandboxId, ((OpenSandboxState) resumed.getState()).getSandboxId());
            assertArrayEquals(payload, resumed.downloadFile(binaryPath));
            resumed.stop();

            stage = "kill-original";
            OpenSandbox killTarget =
                    (OpenSandbox) remoteA.resume(remoteA.describe(originalSandboxId));
            remoteA.delete(killTarget);

            stage = "restore-new-sandbox-from-snapshot";
            restored =
                    (RedisManagedOpenSandbox)
                            clientB.create(workspace, null, null, isolationKey, agentId);
            restored.start();
            String restoredSandboxId = ((OpenSandboxState) restored.getState()).getSandboxId();
            assertNotEquals(originalSandboxId, restoredSandboxId);
            assertArrayEquals(payload, restored.downloadFile(binaryPath));
            assertEquals("restored-ok", restored.exec(null, "printf 'restored-ok'", 30).stdout());
        } catch (Throwable failure) {
            primaryFailure = failure;
            throw new AssertionError("OpenSandbox real-service stage failed: " + stage, failure);
        } finally {
            List<Throwable> cleanupFailures = new ArrayList<>();
            stop(cleanupFailures, restored);
            stop(cleanupFailures, resumed);
            stop(cleanupFailures, second);
            stop(cleanupFailures, first);
            if (remoteA != null) cleanupRemote(cleanupFailures, remoteA, workspaceId);
            if (clientA != null) clientA.close();
            if (clientB != null) clientB.close();
            if (sweeper != null) sweeper.close();
            if (workers != null) OpenSandboxRedisConcurrencyIntegrationTest.shutdown(workers);
            if (clientSchedulerA != null)
                OpenSandboxRedisConcurrencyIntegrationTest.shutdown(clientSchedulerA);
            if (clientSchedulerB != null)
                OpenSandboxRedisConcurrencyIntegrationTest.shutdown(clientSchedulerB);
            if (sweepScheduler != null)
                OpenSandboxRedisConcurrencyIntegrationTest.shutdown(sweepScheduler);
            if (redisA != null && storeA != null)
                cleanupRedis(cleanupFailures, redisA, storeA, workspaceId);
            if (redisB != null && storeB != null)
                cleanupRedis(cleanupFailures, redisB, storeB, workspaceId);
            if (redisA != null) redisA.shutdown();
            if (redisB != null) redisB.shutdown();
            if (!cleanupFailures.isEmpty()) {
                if (primaryFailure != null) {
                    cleanupFailures.forEach(primaryFailure::addSuppressed);
                } else {
                    AssertionError cleanupError =
                            new AssertionError("OpenSandbox real-service cleanup failed");
                    cleanupFailures.forEach(cleanupError::addSuppressed);
                    throw cleanupError;
                }
            }
        }
    }

    private static OpenSandboxClientOptions options(String endpoint, String apiKey) {
        OpenSandboxClientOptions options = new OpenSandboxClientOptions();
        options.setEndpoint(endpoint);
        options.setApiKey(apiKey);
        String image = System.getenv("OPEN_SANDBOX_TEST_IMAGE");
        if (OpenSandboxRedisConcurrencyIntegrationTest.hasText(image)) {
            options.setImage(image);
        }
        options.setReadyTimeoutSeconds(300);
        options.setRequestTimeoutSeconds(120);
        options.setSandboxTimeoutSeconds(1800);
        return options;
    }

    private static OpenSandboxRedisLifecycleOptions lifecycle() {
        OpenSandboxRedisLifecycleOptions lifecycle = new OpenSandboxRedisLifecycleOptions();
        lifecycle.setSweepInterval(Duration.ofSeconds(1));
        lifecycle.setIdleTtl(Duration.ofSeconds(2));
        lifecycle.setHeartbeatInterval(Duration.ofSeconds(1));
        lifecycle.setActiveLeaseTtl(Duration.ofSeconds(30));
        lifecycle.setEvictionGrace(Duration.ofSeconds(2));
        lifecycle.setSnapshotReadyTimeout(Duration.ofMinutes(4));
        lifecycle.setSweeperEnabled(false);
        return lifecycle;
    }

    private static void cleanupRemote(
            List<Throwable> failures, OpenSandboxClient client, String workspaceId) {
        Map<String, String> identity =
                Map.of(
                        "agentscope.owner",
                        "opensandbox-redis",
                        "agentscope.workspace-id",
                        workspaceId);
        try {
            for (OpenSandboxState state : client.listByMetadata(identity)) {
                try {
                    client.delete((Sandbox) client.resume(state));
                } catch (Throwable failure) {
                    if (!client.isNotFound(failure)) failures.add(failure);
                }
            }
        } catch (Throwable failure) {
            failures.add(failure);
        }
        try {
            for (String snapshotId :
                    client.listNativeSnapshotDetailsByNamePrefix(
                                    OpenSandboxLifecycleSweeper.snapshotNamePrefix(workspaceId))
                            .keySet()) {
                try {
                    client.deleteNativeSnapshot(snapshotId);
                } catch (Throwable failure) {
                    if (!client.isNotFound(failure)) failures.add(failure);
                }
            }
        } catch (Throwable failure) {
            failures.add(failure);
        }
    }

    private static void cleanupRedis(
            List<Throwable> failures,
            RedissonClient redisson,
            OpenSandboxWorkspaceStore store,
            String workspaceId) {
        try {
            OpenSandboxRedisConcurrencyIntegrationTest.cleanupWorkspace(
                    redisson, store, workspaceId);
        } catch (Throwable failure) {
            failures.add(failure);
        }
    }

    private static void stop(List<Throwable> failures, RedisManagedOpenSandbox sandbox) {
        if (sandbox == null) return;
        try {
            sandbox.stop();
        } catch (Throwable failure) {
            failures.add(failure);
        }
    }

    private static String shellQuote(String value) {
        return "'" + value.replace("'", "'\"'\"'") + "'";
    }
}
