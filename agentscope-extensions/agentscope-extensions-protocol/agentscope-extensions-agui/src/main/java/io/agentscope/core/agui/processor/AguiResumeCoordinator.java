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

import static io.agentscope.core.agui.AguiInterruptConstants.TOOL_CALL_INTERRUPT_REASON;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.agui.adapter.AguiAgentAdapter;
import io.agentscope.core.agui.event.AguiEvent;
import io.agentscope.core.agui.model.AguiResume;
import io.agentscope.core.agui.model.RunAgentInput;
import io.agentscope.core.state.AgentStateStore;
import io.agentscope.core.state.InMemoryAgentStateStore;
import io.agentscope.core.state.VersionedState;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Coordinates AG-UI resume state with an optimistic-concurrency state store. */
final class AguiResumeCoordinator {

    static final String CONTRACT_ERROR_CODE = "AGUI_INTERRUPT_CONTRACT_ERROR";
    static final int MAX_CAS_ATTEMPTS = 8;
    private static final String USER_NAMESPACE = "__agentscope_agui_resume__";
    private static final String STATE_KEY = "resume_coordinator";
    private static final long MIN_LEASE_MILLIS = Duration.ofMinutes(1).toMillis();
    private static final Duration DEFAULT_LEASE = Duration.ofMinutes(10);

    private final AgentStateStore stateStore;
    private final Clock clock;
    private final long leaseDurationMillis;
    private final ConcurrentMap<String, LeaseHandle> compatibilityHandles =
            new ConcurrentHashMap<>();

    AguiResumeCoordinator() {
        this(new InMemoryAgentStateStore(), Clock.systemUTC(), DEFAULT_LEASE);
    }

    AguiResumeCoordinator(AgentStateStore stateStore) {
        this(stateStore, Clock.systemUTC(), DEFAULT_LEASE);
    }

    AguiResumeCoordinator(AgentStateStore stateStore, Clock clock) {
        this(stateStore, clock, DEFAULT_LEASE);
    }

    AguiResumeCoordinator(AgentStateStore stateStore, Clock clock, Duration runTimeout) {
        if (stateStore == null) {
            throw new NullPointerException("stateStore cannot be null");
        }
        if (!stateStore.supportsVersioning()) {
            throw new IllegalArgumentException("AG-UI resume state store must support versioning");
        }
        this.stateStore = stateStore;
        this.clock = clock != null ? clock : Clock.systemUTC();
        Duration timeout = runTimeout != null ? runTimeout : DEFAULT_LEASE;
        this.leaseDurationMillis = Math.max(MIN_LEASE_MILLIS, timeout.toMillis());
    }

    LeaseAcquisition acquire(RunAgentInput input) {
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            VersionedState<AguiResumeState> versioned = read(input.getThreadId());
            AguiResumeState current = emptyIfAbsent(versioned.value());
            long now = clock.millis();
            if (current.activeRun() != null && current.activeRun().leaseExpiresAt() > now) {
                return LeaseAcquisition.error(
                        "Thread already has an active run; wait for run "
                                + current.activeRun().runId()
                                + " to finish before starting another run on the same thread");
            }

            ResumeContractResult validation = validateAgainst(input, current.pendingInterrupts());
            if (validation.isError()) {
                return LeaseAcquisition.error(validation.message());
            }

            LeaseHandle handle =
                    new LeaseHandle(
                            input.getThreadId(),
                            input.getRunId(),
                            UUID.randomUUID().toString(),
                            now + leaseDurationMillis,
                            current.pendingInterrupts());
            AguiResumeState updated =
                    new AguiResumeState(
                            new AguiActiveRun(
                                    handle.runId(), handle.leaseId(), handle.leaseExpiresAt()),
                            current.pendingInterrupts());
            if (save(input.getThreadId(), updated, versioned.version())) {
                return LeaseAcquisition.success(handle);
            }
        }
        return LeaseAcquisition.error("AG-UI resume coordination conflict; retry the request");
    }

    LeaseTransition renewRun(LeaseHandle handle) {
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            VersionedState<AguiResumeState> versioned = read(handle.threadId());
            AguiResumeState current = emptyIfAbsent(versioned.value());
            if (!owns(current, handle) || current.activeRun().leaseExpiresAt() <= clock.millis()) {
                return LeaseTransition.error("AG-UI run lease is no longer owned");
            }
            AguiResumeState updated =
                    new AguiResumeState(
                            new AguiActiveRun(
                                    handle.runId(),
                                    handle.leaseId(),
                                    clock.millis() + leaseDurationMillis),
                            current.pendingInterrupts());
            if (save(handle.threadId(), updated, versioned.version())) {
                return LeaseTransition.success();
            }
        }
        return LeaseTransition.error("AG-UI resume coordination conflict; retry the request");
    }

    LeaseTransition completeRun(
            LeaseHandle handle, AguiEvent.RunFinished finished, boolean runErrorSeen) {
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            VersionedState<AguiResumeState> versioned = read(handle.threadId());
            AguiResumeState current = emptyIfAbsent(versioned.value());
            if (!owns(current, handle) || current.activeRun().leaseExpiresAt() <= clock.millis()) {
                return LeaseTransition.error("AG-UI run lease is no longer owned");
            }

            Map<String, AguiEvent.Interrupt> pending = current.pendingInterrupts();
            if (finished.outcome() instanceof AguiEvent.RunFinishedInterruptOutcome outcome
                    && !outcome.interrupts().isEmpty()) {
                Map<String, AguiEvent.Interrupt> next = new LinkedHashMap<>();
                for (AguiEvent.Interrupt interrupt : outcome.interrupts()) {
                    next.put(interrupt.id(), interrupt);
                }
                pending = next;
            } else if (!runErrorSeen) {
                pending = Map.of();
            }

            if (save(handle.threadId(), new AguiResumeState(null, pending), versioned.version())) {
                return LeaseTransition.success();
            }
        }
        return LeaseTransition.error("AG-UI resume coordination conflict; retry the request");
    }

    LeaseTransition releaseRun(LeaseHandle handle) {
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            VersionedState<AguiResumeState> versioned = read(handle.threadId());
            AguiResumeState current = emptyIfAbsent(versioned.value());
            if (!owns(current, handle) || current.activeRun().leaseExpiresAt() <= clock.millis()) {
                return LeaseTransition.error("AG-UI run lease is no longer owned");
            }
            if (save(
                    handle.threadId(),
                    new AguiResumeState(null, current.pendingInterrupts()),
                    versioned.version())) {
                return LeaseTransition.success();
            }
        }
        return LeaseTransition.error("AG-UI resume coordination conflict; retry the request");
    }

    ResumeContractResult validate(RunAgentInput input) {
        return validateAgainst(
                input, emptyIfAbsent(read(input.getThreadId()).value()).pendingInterrupts());
    }

    /** Compatibility entry point retained for package tests and callers during migration. */
    ResumeContractResult beginRun(RunAgentInput input) {
        LeaseAcquisition result = acquire(input);
        if (!result.isError()) {
            compatibilityHandles.put(input.getThreadId(), result.lease());
            return ResumeContractResult.proceed();
        }
        return ResumeContractResult.error(result.message());
    }

    /** Compatibility completion entry point. */
    void finishRun(String threadId, String runId) {
        LeaseHandle handle = compatibilityHandles.remove(threadId);
        if (handle != null && handle.runId().equals(runId)) {
            releaseRun(handle);
        }
    }

    RuntimeContext addResumeInterrupts(RunAgentInput input, RuntimeContext runtimeContext) {
        return addResumeInterrupts(input, runtimeContext, null);
    }

    RuntimeContext addResumeInterrupts(
            RunAgentInput input, RuntimeContext runtimeContext, LeaseHandle acquiredLease) {
        if (!input.hasResume()) {
            return runtimeContext;
        }
        Map<String, AguiEvent.Interrupt> pending =
                acquiredLease != null
                        ? acquiredLease.pendingInterrupts()
                        : emptyIfAbsent(read(input.getThreadId()).value()).pendingInterrupts();
        if (pending.isEmpty()) {
            return runtimeContext;
        }
        Map<String, AguiEvent.Interrupt> resumeInterrupts = new LinkedHashMap<>();
        for (AguiResume resume : input.getResume()) {
            AguiEvent.Interrupt interrupt = pending.get(resume.getInterruptId());
            if (interrupt != null && shouldPassResumeInterrupt(interrupt)) {
                resumeInterrupts.put(resume.getInterruptId(), interrupt);
            }
        }
        if (resumeInterrupts.isEmpty()) {
            return runtimeContext;
        }
        return RuntimeContext.builder(runtimeContext)
                .put(
                        AguiAgentAdapter.RUNTIME_CONTEXT_RESUME_INTERRUPTS_KEY,
                        Map.copyOf(resumeInterrupts))
                .build();
    }

    /** Compatibility event tracker used by the pre-lease unit tests. */
    void trackPendingInterrupts(
            String threadId, String runId, AguiEvent event, boolean runErrorSeen) {
        if (event instanceof AguiEvent.RunFinished finished) {
            LeaseHandle handle = compatibilityHandles.get(threadId);
            if (handle != null && handle.runId().equals(runId)) {
                completeRun(handle, finished, runErrorSeen);
            }
        }
    }

    /**
     * Build the AG-UI error lifecycle used when the resume contract is violated.
     *
     * @param input The invalid run input
     * @param message The validation error message
     * @param emitRunFinishedAfterError true to append {@code RUN_FINISHED} after {@code RUN_ERROR}
     *     for legacy clients
     * @return The protocol error lifecycle events
     */
    List<AguiEvent> contractErrorEvents(
            RunAgentInput input, String message, boolean emitRunFinishedAfterError) {
        List<AguiEvent> events = new ArrayList<>();
        events.add(new AguiEvent.RunStarted(input.getThreadId(), input.getRunId(), null, input));
        events.add(
                new AguiEvent.RunError(
                        input.getThreadId(),
                        input.getRunId(),
                        message,
                        CONTRACT_ERROR_CODE,
                        System.currentTimeMillis(),
                        null));
        if (emitRunFinishedAfterError) {
            events.add(new AguiEvent.RunFinished(input.getThreadId(), input.getRunId()));
        }
        return List.copyOf(events);
    }

    private ResumeContractResult validateAgainst(
            RunAgentInput input, Map<String, AguiEvent.Interrupt> pending) {
        if (pending.isEmpty()) {
            return input.hasResume()
                    ? ResumeContractResult.error(
                            "RunAgentInput.resume does not match any open interrupt")
                    : ResumeContractResult.proceed();
        }
        if (!input.hasResume()) {
            return ResumeContractResult.error(
                    "Thread has unresolved interrupts; RunAgentInput.resume must address all of"
                            + " them");
        }
        ResumeContractResult statusResult = validateResumeStatuses(input.getResume());
        if (statusResult.isError()) {
            return statusResult;
        }
        Set<String> resumeIds = new LinkedHashSet<>();
        for (AguiResume resume : input.getResume()) {
            if (!resumeIds.add(resume.getInterruptId())) {
                return ResumeContractResult.error(
                        "RunAgentInput.resume contains duplicate interruptId: "
                                + resume.getInterruptId());
            }
        }
        Set<String> pendingIds = pending.keySet();
        if (!resumeIds.equals(pendingIds)) {
            Set<String> missingIds = new LinkedHashSet<>(pendingIds);
            missingIds.removeAll(resumeIds);
            Set<String> unknownIds = new LinkedHashSet<>(resumeIds);
            unknownIds.removeAll(pendingIds);
            return ResumeContractResult.error(
                    "RunAgentInput.resume must cover all open interrupts. missing="
                            + missingIds
                            + ", unknown="
                            + unknownIds);
        }
        return ResumeContractResult.proceed();
    }

    private VersionedState<AguiResumeState> read(String threadId) {
        return stateStore.getVersioned(USER_NAMESPACE, threadId, STATE_KEY, AguiResumeState.class);
    }

    private boolean save(String threadId, AguiResumeState state, long expectedVersion) {
        return stateStore.saveIfVersion(USER_NAMESPACE, threadId, STATE_KEY, state, expectedVersion)
                != AgentStateStore.UNVERSIONED;
    }

    private static AguiResumeState emptyIfAbsent(AguiResumeState state) {
        return state != null ? state : new AguiResumeState(null, Map.of());
    }

    private static boolean owns(AguiResumeState state, LeaseHandle handle) {
        return state.activeRun() != null
                && state.activeRun().runId().equals(handle.runId())
                && state.activeRun().leaseId().equals(handle.leaseId());
    }

    private ResumeContractResult validateResumeStatuses(List<AguiResume> resumes) {
        for (AguiResume resume : resumes) {
            if (!resume.isResolved() && !resume.isCancelled()) {
                return ResumeContractResult.error(
                        "RunAgentInput.resume contains unsupported status: " + resume.getStatus());
            }
        }
        return ResumeContractResult.proceed();
    }

    private boolean shouldPassResumeInterrupt(AguiEvent.Interrupt interrupt) {
        return TOOL_CALL_INTERRUPT_REASON.equals(interrupt.reason())
                && interrupt.toolCallId() != null
                && !interrupt.toolCallId().isBlank();
    }

    record LeaseHandle(
            String threadId,
            String runId,
            String leaseId,
            long leaseExpiresAt,
            Map<String, AguiEvent.Interrupt> pendingInterrupts) {
        LeaseHandle {
            pendingInterrupts = Map.copyOf(pendingInterrupts);
        }
    }

    record LeaseAcquisition(LeaseHandle lease, String message) {
        static LeaseAcquisition success(LeaseHandle lease) {
            return new LeaseAcquisition(lease, null);
        }

        static LeaseAcquisition error(String message) {
            return new LeaseAcquisition(null, message);
        }

        boolean isError() {
            return lease == null;
        }
    }

    record LeaseTransition(boolean error, String message) {
        static LeaseTransition success() {
            return new LeaseTransition(false, null);
        }

        static LeaseTransition error(String message) {
            return new LeaseTransition(true, message);
        }

        boolean isError() {
            return error;
        }
    }

    record ResumeContractResult(boolean error, String message) {
        boolean isError() {
            return error;
        }

        static ResumeContractResult proceed() {
            return new ResumeContractResult(false, null);
        }

        static ResumeContractResult error(String message) {
            return new ResumeContractResult(true, message);
        }
    }
}
