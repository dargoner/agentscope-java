/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.agentscope.core.agui.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.agentscope.core.agui.event.AguiEvent;
import io.agentscope.core.agui.model.RunAgentInput;
import io.agentscope.core.state.InMemoryAgentStateStore;
import io.agentscope.core.util.JsonUtils;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Repeatable micro-benchmarks for local coordinator and snapshot operations. */
@Tag("performance")
class AguiResumeCoordinatorPerformanceTest {

    @Test
    void measuresColdFirstFiveSteadyStateAndSnapshotRestore() {
        InMemoryAgentStateStore store = new InMemoryAgentStateStore();
        AguiResumeCoordinator coordinator = new AguiResumeCoordinator(store);
        long[] firstFiveNanos = new long[5];
        for (int i = 0; i < firstFiveNanos.length; i++) {
            firstFiveNanos[i] = measureRun(coordinator, "cold-" + i);
        }

        for (int i = 0; i < 100; i++) {
            measureRun(coordinator, "warmup-" + i);
        }
        long[] steadyNanos = new long[1000];
        for (int i = 0; i < steadyNanos.length; i++) {
            steadyNanos[i] = measureRun(coordinator, "steady-" + i);
        }

        AguiEvent.Interrupt interrupt =
                new AguiEvent.Interrupt(
                        "interrupt-1", "tool_call", "confirm", "tool-call-1", null, null, null);
        AguiResumeState snapshot = new AguiResumeState(null, Map.of("interrupt-1", interrupt));
        long[] restoreNanos = new long[1000];
        for (int i = 0; i < restoreNanos.length; i++) {
            long start = System.nanoTime();
            String json = JsonUtils.getJsonCodec().toJson(snapshot);
            AguiResumeState restored =
                    JsonUtils.getJsonCodec().fromJson(json, AguiResumeState.class);
            restoreNanos[i] = System.nanoTime() - start;
            assertEquals(snapshot, restored);
        }

        long[] resumeNanos = new long[1000];
        for (int i = 0; i < resumeNanos.length; i++) {
            InMemoryAgentStateStore resumeStore = new InMemoryAgentStateStore();
            AguiResumeCoordinator producer = new AguiResumeCoordinator(resumeStore);
            AguiResumeCoordinator.LeaseAcquisition produced =
                    producer.acquire(input("snapshot-source-" + i));
            assertFalse(produced.isError());
            producer.completeRun(
                    produced.lease(),
                    new AguiEvent.RunFinished(
                            "thread-snapshot-source-" + i,
                            "snapshot-source-" + i,
                            null,
                            new AguiEvent.RunFinishedInterruptOutcome(java.util.List.of(interrupt)),
                            null,
                            null),
                    false);
            AguiResumeCoordinator consumer = new AguiResumeCoordinator(resumeStore);
            long start = System.nanoTime();
            AguiResumeCoordinator.LeaseAcquisition resumed =
                    consumer.acquire(
                            RunAgentInput.builder()
                                    .threadId("thread-snapshot-source-" + i)
                                    .runId("snapshot-resume-" + i)
                                    .resume(
                                            java.util.List.of(
                                                    new io.agentscope.core.agui.model.AguiResume(
                                                            "interrupt-1",
                                                            io.agentscope.core.agui.model.AguiResume
                                                                    .STATUS_RESOLVED,
                                                            Map.of("approved", true))))
                                    .build());
            resumeNanos[i] = System.nanoTime() - start;
            assertFalse(resumed.isError());
        }

        System.out.printf(
                "AG-UI performance: first5=%s us, steady_avg=%.2f us, steady_p95=%.2f us, "
                        + "snapshot_restore_avg=%.2f us, snapshot_restore_p95=%.2f us, "
                        + "cross_instance_resume_avg=%.2f us, cross_instance_resume_p95=%.2f us%n",
                nanosToMicros(firstFiveNanos),
                nanosToMicros(average(steadyNanos)),
                nanosToMicros(percentile(steadyNanos, 0.95)),
                nanosToMicros(average(restoreNanos)),
                nanosToMicros(percentile(restoreNanos, 0.95)),
                nanosToMicros(average(resumeNanos)),
                nanosToMicros(percentile(resumeNanos, 0.95)));
    }

    private static long measureRun(AguiResumeCoordinator coordinator, String runId) {
        long start = System.nanoTime();
        AguiResumeCoordinator.LeaseAcquisition acquisition = coordinator.acquire(input(runId));
        assertFalse(acquisition.isError());
        assertFalse(
                coordinator
                        .completeRun(
                                acquisition.lease(),
                                new AguiEvent.RunFinished("thread-" + runId, runId),
                                false)
                        .isError());
        return System.nanoTime() - start;
    }

    private static RunAgentInput input(String runId) {
        return RunAgentInput.builder().threadId("thread-" + runId).runId(runId).build();
    }

    private static double average(long[] values) {
        return Arrays.stream(values).average().orElse(0.0);
    }

    private static double percentile(long[] values, double percentile) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        int index = Math.min(sorted.length - 1, (int) Math.ceil(percentile * sorted.length) - 1);
        return sorted[index];
    }

    private static String nanosToMicros(long[] values) {
        StringBuilder result = new StringBuilder("[");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                result.append(", ");
            }
            result.append(String.format("%.2f", values[i] / 1_000.0));
        }
        return result.append(']').toString();
    }

    private static double nanosToMicros(double nanos) {
        return nanos / 1_000.0;
    }
}
