# AG-UI Distributed Resume Coordinator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (or superpowers:subagent-driven-development) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add optional, versioned `AgentStateStore` coordination for AG-UI runs so active-run leases and pending interrupts are safe across application instances while preserving local defaults.

**Architecture:** Replace the coordinator's process-local maps with one CAS-updated `AguiResumeState` aggregate per `threadId`. `AguiRequestProcessor` owns the reactive lifecycle: acquisition is lazy, blocking store calls run on `boundedElastic`, renewal is serialized and cancellable, and `RunFinished` is forwarded only after terminal state persistence. Spring MVC/WebFlux pass one validated store from auto-configuration when explicitly enabled.

**Tech Stack:** Java 17+, Reactor Core, existing `AgentStateStore`/`VersionedState` CAS API, Spring Boot auto-configuration, JUnit 5, Mockito/TestPublisher only where transport fakes are unavoidable.

---

### Task 1: Establish Isolated Worktree And Baseline

**Files:**
- Worktree: `.worktrees/agui-distributed-resume-coordinator`

- [ ] **Step 1: Remove the already-merged sandbox worktree and branch from the main checkout.**

Run from `D:\ai-code\agentscope-java`:
`git worktree remove .worktrees/sandbox-workspace-key` and then `git branch -d feat/sandbox-workspace-key`.

- [ ] **Step 2: Create the feature worktree from current `main` and verify it is ignored.**

Run `git check-ignore -q .worktrees`, then `git worktree add .worktrees/agui-distributed-resume-coordinator -b feat/agui-distributed-resume-coordinator main`.

- [ ] **Step 3: Run the AG-UI module baseline tests in the new worktree.**

Run `mvn -pl agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui -am test`; expected baseline is green except the known Windows symlink permission failures in `agentscope-core` if `-am` reaches those tests. Run the AG-UI module alone if the unrelated symlink failures obscure the baseline.

### Task 2: Define Persisted Aggregate And Coordinator CAS Contract (TDD)

**Files:**
- Modify: `agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui/src/test/java/io/agentscope/core/agui/processor/AguiResumeCoordinatorTest.java`
- Modify: `agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui/src/main/java/io/agentscope/core/agui/processor/AguiResumeCoordinator.java`

- [ ] **Step 1: Add failing tests for shared-store acquisition, pending-interrupt sharing, different-thread concurrency, stale lease fencing, expiry takeover, CAS retry bounds, and rejection of `supportsVersioning() == false`.**

Use two coordinators backed by one `InMemoryAgentStateStore`, inject a mutable `Clock`, and assert that every public coordinator transition checks `threadId`, `runId`, and random `leaseId`.

- [ ] **Step 2: Run only `AguiResumeCoordinatorTest` and verify RED.**

Run `mvn -pl agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui -Dtest=AguiResumeCoordinatorTest test`; expected failures must identify missing constructors/lease APIs or incorrect state transitions, not compilation mistakes in the test itself.

- [ ] **Step 3: Implement the minimal `AguiResumeState` record and lease handle inside the coordinator.**

The record implements `io.agentscope.core.state.State`, stores `ActiveRun(runId, leaseId, leaseExpiresAt)` and an immutable interrupt map, and is addressed with a fixed AG-UI namespace, `sessionId == threadId`, and fixed state key. Use `getVersioned` plus at most eight `saveIfVersion` attempts; treat `UNVERSIONED` as conflict for a required versioned store.

- [ ] **Step 4: Implement `beginRun`, `renewRun`, `completeRun`, `releaseRun`, and snapshot-based resume interrupt injection.**

Expired leases are replaceable; all writes preserve unrelated pending interrupts unless the terminal event explicitly changes them. A stale handle returns a lease-loss result and cannot write state.

- [ ] **Step 5: Run the coordinator tests again and verify GREEN, then run the existing AG-UI processor tests.**

Run the focused test command from Step 2 followed by `mvn -pl agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui -Dtest='*Agui*Test' test`.

### Task 3: Integrate Lazy Acquisition, Renewal, And Terminal Ordering (TDD)

**Files:**
- Modify: `agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui/src/test/java/io/agentscope/core/agui/processor/AguiRequestProcessorTest.java` (create if absent)
- Modify: `agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui/src/main/java/io/agentscope/core/agui/processor/AguiRequestProcessor.java`

- [ ] **Step 1: Add failing processor tests for lazy acquisition, boundedElastic scheduling, renewal cancellation, lease-loss termination, terminal persistence-before-forwarding, and release on complete/error/cancel/subscription failure.**

Use a controllable adapter `Flux`, a recording versioned store, and Reactor `StepVerifier`/virtual time. Assert the store's terminal write is observed before the subscriber sees `RunFinished`.

- [ ] **Step 2: Run the focused processor tests and verify RED.**

Run `mvn -pl agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui -Dtest=AguiRequestProcessorTest test` and confirm failures are behavioral.

- [ ] **Step 3: Add `Builder.resumeStateStore(AgentStateStore)` and construct the coordinator with either the supplied store or a private versioned in-memory store.**

Reject a supplied non-versioned store during build with a clear `IllegalStateException`.

- [ ] **Step 4: Compose the event stream with `Flux.defer`, `subscribeOn(boundedElastic())`, a serialized renewal publisher, lease-loss cancellation, and `concatMap` terminal persistence.**

Track `RunError` before `RunFinished`; persist interrupted outcomes, preserve pending interrupts after errors/abnormal termination, and call matching release from `doFinally`. Never forward `RunFinished` after a failed owned terminal transition.

- [ ] **Step 5: Run the focused processor tests and all AG-UI tests; verify GREEN.**

Run the commands from Steps 2 and `mvn -pl agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui test`.

### Task 4: Thread Store Configuration Through MVC And WebFlux

**Files:**
- Modify: `agentscope-extensions/agentscope-spring-boot-starters/agentscope-agui-spring-boot-starter/src/main/java/io/agentscope/spring/boot/agui/mvc/AguiMvcController.java`
- Modify: `agentscope-extensions/agentscope-spring-boot-starters/agentscope-agui-spring-boot-starter/src/main/java/io/agentscope/spring/boot/agui/webflux/AguiWebFluxHandler.java`
- Modify: `agentscope-extensions/agentscope-spring-boot-starters/agentscope-agui-spring-boot-starter/src/test/java/io/agentscope/spring/boot/agui/common/AguiAdapterConfigAutoConfigurationTest.java`

- [ ] **Step 1: Add failing builder tests proving a supplied `AgentStateStore` reaches the processor for both transports.**
- [ ] **Step 2: Run the focused starter tests and verify RED.**
- [ ] **Step 3: Add null-safe `resumeStateStore(AgentStateStore)` builder methods and pass the field to `AguiRequestProcessor.builder()`.**
- [ ] **Step 4: Add transport tests proving coordinator persistence errors emit an error and close without a fabricated `RunFinished`, while pre-acquisition parse/agent errors retain existing behavior.**
- [ ] **Step 5: Run the focused starter test class and the AG-UI starter test suite; verify GREEN.**

### Task 5: Add Explicit Spring Distributed-Mode Configuration

**Files:**
- Modify: `agentscope-extensions/agentscope-spring-boot-starters/agentscope-agui-spring-boot-starter/src/main/java/io/agentscope/spring/boot/agui/common/AguiProperties.java`
- Modify: `agentscope-extensions/agentscope-spring-boot-starters/agentscope-agui-spring-boot-starter/src/main/java/io/agentscope/spring/boot/agui/mvc/AgentscopeAguiMvcAutoConfiguration.java`
- Modify: `agentscope-extensions/agentscope-spring-boot-starters/agentscope-agui-spring-boot-starter/src/main/java/io/agentscope/spring/boot/agui/webflux/AgentscopeAguiWebFluxAutoConfiguration.java`
- Modify: `agentscope-extensions/agentscope-spring-boot-starters/agentscope-agui-spring-boot-starter/src/test/java/io/agentscope/spring/boot/agui/common/AguiAdapterConfigAutoConfigurationTest.java`

- [ ] **Step 1: Add failing context tests for the default local path, one unique versioned store, missing store, ambiguous stores, and non-versioned store.**
- [ ] **Step 2: Run the context tests and verify RED.**
- [ ] **Step 3: Add `resume.distributed-enabled` binding (default `false`) and a shared resolver that requires exactly one `AgentStateStore` only when enabled, checking `supportsVersioning()`.**
- [ ] **Step 4: Pass the resolved store into the active MVC or WebFlux builder without changing unrelated adapter properties.**
- [ ] **Step 5: Run all starter tests and verify GREEN.**

### Task 6: Verify Serialization And Full Regression

**Files:**
- Add focused state round-trip tests in the AG-UI module where the selected store implementation is available.
- Modify documentation only if public property or builder Javadocs require it.

- [ ] **Step 1: Add a round-trip test covering `AguiResumeState` with nested interrupts, optional fields, and raw event data through the available serialized store test harness.**
- [ ] **Step 2: Run `mvn -pl agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui,agentscope-extensions/agentscope-spring-boot-starters/agentscope-agui-spring-boot-starter -am test`.**
- [ ] **Step 3: Run `mvn spotless:check` and `git diff --check`.**
- [ ] **Step 4: Record the known two Windows symlink permission failures separately if the full reactor reaches them; do not classify them as AG-UI regressions.**
- [ ] **Step 5: Commit the feature branch with focused commits, then request review before merging to `main`.**
