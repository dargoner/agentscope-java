/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.agentscope.core.agui.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.agui.adapter.AguiAgentAdapter;
import io.agentscope.core.agui.event.AguiEvent;
import io.agentscope.core.agui.event.AguiEventType;
import io.agentscope.core.agui.model.AguiMessage;
import io.agentscope.core.agui.model.AguiResume;
import io.agentscope.core.agui.model.RunAgentInput;
import io.agentscope.core.state.AgentStateStore;
import io.agentscope.core.state.InMemoryAgentStateStore;
import io.agentscope.core.state.State;
import io.agentscope.core.util.JsonUtils;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Unit tests for AguiResumeCoordinator. */
class AguiResumeCoordinatorTest {

    @Test
    void resumeStateRoundTripsNestedInterruptsAndOptionalFields() {
        AguiEvent.Interrupt nested =
                new AguiEvent.Interrupt(
                        "interrupt-1",
                        "tool_call",
                        "confirm",
                        "tool-call-1",
                        Map.of("type", "object"),
                        null,
                        Map.of("raw", Map.of("enabled", true)));
        AguiResumeState state =
                new AguiResumeState(
                        new AguiActiveRun("run-1", "lease-1", 1234L),
                        Map.of("interrupt-1", nested));

        AguiResumeState restored =
                JsonUtils.getJsonCodec()
                        .fromJson(JsonUtils.getJsonCodec().toJson(state), AguiResumeState.class);

        assertEquals(state.activeRun(), restored.activeRun());
        assertEquals(state.pendingInterrupts(), restored.pendingInterrupts());
    }

    @Test
    void coordinatorsSharingStoreSharePendingInterrupts() {
        InMemoryAgentStateStore store = new InMemoryAgentStateStore();
        AguiResumeCoordinator first = new AguiResumeCoordinator(store);
        AguiResumeCoordinator second = new AguiResumeCoordinator(store);

        AguiResumeCoordinator.LeaseAcquisition acquisition = first.acquire(input("run-1"));
        assertFalse(acquisition.isError());
        first.completeRun(
                acquisition.lease(), interruptedFinished("run-1", interrupt("interrupt-1")), false);

        AguiResumeCoordinator.LeaseAcquisition resume =
                second.acquire(
                        RunAgentInput.builder()
                                .threadId("thread-1")
                                .runId("run-2")
                                .resume(
                                        List.of(
                                                new AguiResume(
                                                        "interrupt-1",
                                                        AguiResume.STATUS_RESOLVED,
                                                        Map.of("approved", true))))
                                .build());
        assertFalse(resume.isError());
        assertEquals(
                Map.of("interrupt-1", interrupt("interrupt-1")),
                resume.lease().pendingInterrupts());
    }

    @Test
    void expiredLeaseCanBeTakenOverButStaleOwnerCannotComplete() {
        InMemoryAgentStateStore store = new InMemoryAgentStateStore();
        MutableClock clock = new MutableClock(1_000L);
        AguiResumeCoordinator first = new AguiResumeCoordinator(store, clock);
        AguiResumeCoordinator second = new AguiResumeCoordinator(store, clock);

        AguiResumeCoordinator.LeaseAcquisition firstRun = first.acquire(input("run-1"));
        clock.advanceMillis(Duration.ofMinutes(10).toMillis() + 1L);
        AguiResumeCoordinator.LeaseAcquisition takeover = second.acquire(input("run-2"));

        assertFalse(takeover.isError());
        assertTrue(
                first.completeRun(
                                firstRun.lease(),
                                new AguiEvent.RunFinished("thread-1", "run-1"),
                                false)
                        .isError());
        assertFalse(
                second.completeRun(
                                takeover.lease(),
                                new AguiEvent.RunFinished("thread-1", "run-2"),
                                false)
                        .isError());
    }

    @Test
    void rejectsNonVersionedStore() {
        AgentStateStore store = new NonVersionedStore();
        assertTrue(
                org.junit.jupiter.api.Assertions.assertThrows(
                                IllegalArgumentException.class,
                                () -> new AguiResumeCoordinator(store))
                        .getMessage()
                        .contains("versioning"));
    }

    @Test
    void validateRejectsNewInputWhenThreadHasOpenInterrupts() {
        AguiResumeCoordinator coordinator = new AguiResumeCoordinator();
        track(coordinator, "run-1", interruptedFinished("run-1", interrupt("interrupt-1")), false);

        AguiResumeCoordinator.ResumeContractResult result =
                coordinator.validate(
                        RunAgentInput.builder()
                                .threadId("thread-1")
                                .runId("run-2")
                                .messages(List.of(AguiMessage.userMessage("msg-1", "hello")))
                                .build());

        assertTrue(result.isError());
    }

    @Test
    void validateRequiresResumeToCoverAllOpenInterrupts() {
        AguiResumeCoordinator coordinator = new AguiResumeCoordinator();
        track(
                coordinator,
                "run-1",
                interruptedFinished("run-1", interrupt("interrupt-1"), interrupt("interrupt-2")),
                false);

        AguiResumeCoordinator.ResumeContractResult result =
                coordinator.validate(
                        RunAgentInput.builder()
                                .threadId("thread-1")
                                .runId("run-2")
                                .resume(
                                        List.of(
                                                new AguiResume(
                                                        "interrupt-1",
                                                        AguiResume.STATUS_RESOLVED,
                                                        Map.of("approved", true))))
                                .build());

        assertTrue(result.isError());
    }

    @Test
    void addResumeInterruptsAddsKnownInterruptsToRuntimeContext() {
        AguiResumeCoordinator coordinator = new AguiResumeCoordinator();
        AguiEvent.Interrupt interrupt = interrupt("interrupt-1");
        track(coordinator, "run-1", interruptedFinished("run-1", interrupt), false);

        RuntimeContext context =
                coordinator.addResumeInterrupts(
                        RunAgentInput.builder()
                                .threadId("thread-1")
                                .runId("run-2")
                                .resume(
                                        List.of(
                                                new AguiResume(
                                                        "interrupt-1",
                                                        AguiResume.STATUS_RESOLVED,
                                                        Map.of("approved", true))))
                                .build(),
                        RuntimeContext.builder().put("tenant", "tenant-a").build());

        assertEquals("tenant-a", context.get("tenant"));
        assertEquals(
                Map.of("interrupt-1", interrupt),
                context.get(AguiAgentAdapter.RUNTIME_CONTEXT_RESUME_INTERRUPTS_KEY));
    }

    @Test
    void addResumeInterruptsAddsConfirmationInterruptToRuntimeContext() {
        AguiResumeCoordinator coordinator = new AguiResumeCoordinator();
        AguiEvent.Interrupt confirmation =
                new AguiEvent.Interrupt(
                        "interrupt-1",
                        "tool_call",
                        "confirm echo",
                        "tool-call-1",
                        null,
                        null,
                        Map.of("toolName", "echo"));
        track(coordinator, "run-1", interruptedFinished("run-1", confirmation), false);

        RuntimeContext context =
                coordinator.addResumeInterrupts(
                        RunAgentInput.builder()
                                .threadId("thread-1")
                                .runId("run-2")
                                .resume(
                                        List.of(
                                                new AguiResume(
                                                        "interrupt-1",
                                                        AguiResume.STATUS_RESOLVED,
                                                        Map.of("approved", true))))
                                .build(),
                        null);

        Object interrupts = context.get(AguiAgentAdapter.RUNTIME_CONTEXT_RESUME_INTERRUPTS_KEY);
        assertEquals(Map.of("interrupt-1", confirmation), interrupts);
    }

    @Test
    void addResumeInterruptsIgnoresInterruptsWithoutToolCallId() {
        AguiResumeCoordinator coordinator = new AguiResumeCoordinator();
        AguiEvent.Interrupt custom =
                new AguiEvent.Interrupt(
                        "interrupt-1", "custom_reason", "no tool", null, null, null, null);
        track(coordinator, "run-1", interruptedFinished("run-1", custom), false);

        RuntimeContext context =
                coordinator.addResumeInterrupts(
                        RunAgentInput.builder()
                                .threadId("thread-1")
                                .runId("run-2")
                                .resume(
                                        List.of(
                                                new AguiResume(
                                                        "interrupt-1",
                                                        AguiResume.STATUS_RESOLVED,
                                                        Map.of("approved", true))))
                                .build(),
                        RuntimeContext.builder().put("tenant", "tenant-a").build());

        assertEquals("tenant-a", context.get("tenant"));
        assertNull(context.get(AguiAgentAdapter.RUNTIME_CONTEXT_RESUME_INTERRUPTS_KEY));
    }

    @Test
    void trackDoesNotClearPendingInterruptsAfterRunError() {
        AguiResumeCoordinator coordinator = new AguiResumeCoordinator();
        track(coordinator, "run-1", interruptedFinished("run-1", interrupt("interrupt-1")), false);
        track(coordinator, "run-2", new AguiEvent.RunFinished("thread-1", "run-2"), true);

        AguiResumeCoordinator.ResumeContractResult result =
                coordinator.validate(
                        RunAgentInput.builder()
                                .threadId("thread-1")
                                .runId("run-3")
                                .resume(
                                        List.of(
                                                new AguiResume(
                                                        "interrupt-1",
                                                        AguiResume.STATUS_RESOLVED,
                                                        Map.of("approved", true))))
                                .build());

        assertFalse(result.isError());
    }

    @Test
    void beginRunRejectsConcurrentRunOnSameThread() {
        AguiResumeCoordinator coordinator = new AguiResumeCoordinator();

        AguiResumeCoordinator.ResumeContractResult first = coordinator.beginRun(input("run-1"));
        AguiResumeCoordinator.ResumeContractResult second = coordinator.beginRun(input("run-2"));

        assertFalse(first.isError());
        assertTrue(second.isError());
    }

    @Test
    void beginRunRejectsDuplicateActiveRunOnSameThread() {
        AguiResumeCoordinator coordinator = new AguiResumeCoordinator();

        coordinator.beginRun(input("run-1"));
        AguiResumeCoordinator.ResumeContractResult duplicate = coordinator.beginRun(input("run-1"));

        assertTrue(duplicate.isError());
    }

    @Test
    void finishRunDoesNotReleaseDifferentActiveRun() {
        AguiResumeCoordinator coordinator = new AguiResumeCoordinator();
        coordinator.beginRun(input("run-1"));

        coordinator.finishRun("thread-1", "run-2");
        AguiResumeCoordinator.ResumeContractResult result = coordinator.beginRun(input("run-3"));

        assertTrue(result.isError());
    }

    @Test
    void trackIgnoresEventsFromInactiveRun() {
        AguiResumeCoordinator coordinator = new AguiResumeCoordinator();
        coordinator.beginRun(input("run-2"));

        coordinator.trackPendingInterrupts(
                "thread-1", "run-1", interruptedFinished("run-1", interrupt("interrupt-1")), false);

        AguiResumeCoordinator.ResumeContractResult result =
                coordinator.validate(
                        RunAgentInput.builder()
                                .threadId("thread-1")
                                .runId("run-3")
                                .messages(List.of(AguiMessage.userMessage("msg-1", "hello")))
                                .build());

        assertFalse(result.isError());
    }

    @Test
    void contractErrorEventsUseAguiResumeErrorLifecycleCodeAndTimestamp() {
        AguiResumeCoordinator coordinator = new AguiResumeCoordinator();

        List<AguiEvent> events =
                coordinator.contractErrorEvents(input("run-1"), "resume contract failed", false);

        assertEquals(
                List.of(AguiEventType.RUN_STARTED, AguiEventType.RUN_ERROR),
                events.stream().map(AguiEvent::getType).toList());
        AguiEvent.RunError error = (AguiEvent.RunError) events.get(1);
        assertEquals(AguiResumeCoordinator.CONTRACT_ERROR_CODE, error.code());
        assertEquals("resume contract failed", error.message());
        assertNotNull(error.timestamp());
    }

    @Test
    void contractErrorEventsEmitRunFinishedWhenEnabled() {
        AguiResumeCoordinator coordinator = new AguiResumeCoordinator();

        List<AguiEvent> events =
                coordinator.contractErrorEvents(input("run-1"), "resume contract failed", true);

        assertEquals(
                List.of(
                        AguiEventType.RUN_STARTED,
                        AguiEventType.RUN_ERROR,
                        AguiEventType.RUN_FINISHED),
                events.stream().map(AguiEvent::getType).toList());
    }

    private static void track(
            AguiResumeCoordinator coordinator,
            String runId,
            AguiEvent.RunFinished event,
            boolean runErrorSeen) {
        coordinator.beginRun(input(runId));
        coordinator.trackPendingInterrupts("thread-1", runId, event, runErrorSeen);
        coordinator.finishRun("thread-1", runId);
    }

    private static AguiEvent.Interrupt interrupt(String interruptId) {
        return interrupt(interruptId, "tool-call-1");
    }

    private static AguiEvent.RunFinished interruptedFinished(
            String runId, AguiEvent.Interrupt... interrupts) {
        return new AguiEvent.RunFinished(
                "thread-1",
                runId,
                null,
                new AguiEvent.RunFinishedInterruptOutcome(List.of(interrupts)));
    }

    private static AguiEvent.Interrupt interrupt(String interruptId, String toolCallId) {
        return new AguiEvent.Interrupt(
                interruptId, "tool_call", "approve", toolCallId, null, null, null);
    }

    private static RunAgentInput input(String runId) {
        return RunAgentInput.builder().threadId("thread-1").runId(runId).build();
    }

    private static final class MutableClock extends Clock {
        private long millis;

        private MutableClock(long millis) {
            this.millis = millis;
        }

        private void advanceMillis(long amount) {
            millis += amount;
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }
    }

    private static final class NonVersionedStore implements AgentStateStore {
        @Override
        public void save(String u, String s, String k, State v) {}

        @Override
        public void save(String u, String s, String k, List<? extends State> v) {}

        @Override
        public <T extends State> java.util.Optional<T> get(
                String u, String s, String k, Class<T> t) {
            return java.util.Optional.empty();
        }

        @Override
        public <T extends State> List<T> getList(String u, String s, String k, Class<T> t) {
            return List.of();
        }

        @Override
        public boolean exists(String u, String s) {
            return false;
        }

        @Override
        public void delete(String u, String s) {}

        @Override
        public java.util.Set<String> listSessionIds(String u) {
            return java.util.Set.of();
        }
    }
}
