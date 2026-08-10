# AG-UI Distributed Resume Coordinator Design

## Summary

`AguiResumeCoordinator` currently keeps active runs and pending interrupts in process-local
concurrent maps. That is correct for a single `AguiRequestProcessor`, but two application
instances can accept the same AG-UI `threadId` concurrently and cannot resume interrupts created
by each other.

This change makes resume coordination optionally distributed by storing one versioned aggregate
state per `threadId` in the existing `AgentStateStore`. It does not add a Maven module, a
Redis-specific dependency, or another storage SPI. The existing in-memory, Redis, MySQL, and
PostgreSQL `AgentStateStore` implementations provide the required compare-and-set operation.

The default remains local and in-memory. Spring applications must explicitly enable distributed
resume coordination.

## Goals

- Enforce one active AG-UI run per `threadId` across application instances.
- Allow an interrupt produced on one instance to be resumed on another instance.
- Preserve the current resume validation and pending-interrupt lifecycle.
- Recover from an instance crash without leaving a `threadId` permanently occupied.
- Fence an expired owner so it cannot overwrite state after another instance takes over.
- Reuse versioned `AgentStateStore` implementations and keep storage access off WebFlux event
  threads.
- Preserve current behavior when distributed coordination is not configured.

## Non-goals

- Introducing a Redis-specific lock or AG-UI Redis module.
- Adding tenant, agent, user, or custom workspace identity to the coordination key.
- Distributing `ThreadSessionManager` or agent instances.
- Automatically migrating process-local pending interrupts that existed before deployment.
- Adding administrative lock inspection or manual lock-breaking APIs.
- Adding independent lease-duration and renewal-interval configuration in the first version.

## Key and Storage Mapping

The logical coordination key is exactly the same key used by the current in-memory
implementation: AG-UI `threadId`.

`AgentStateStore` requires a user ID, session ID, and state key. The coordinator maps its logical
key into that API as follows:

- `userId`: a fixed internal AG-UI resume namespace
- `sessionId`: `threadId`
- `key`: a fixed coordinator-state name

Only `threadId` varies. `agentId`, tenant IDs, runtime-context user IDs, and custom key resolvers
do not participate in identity. The fixed namespace only prevents collisions with unrelated agent
state; it does not alter the logical coordination key.

## State Model

Each `threadId` stores one aggregate `AguiResumeState` implementing `State`:

- `activeRun`: nullable active-run lease
  - `runId`
  - `leaseId`, a random owner token
  - `leaseExpiresAt`, stored as an epoch-millisecond timestamp
- `pendingInterrupts`: interrupt ID to `AguiEvent.Interrupt` map

The store-provided version is not embedded in the value. It is read with `getVersioned` and used
as the expected version for `saveIfVersion`.

Keeping active-run ownership and pending interrupts in one value allows acquisition, resume
validation, terminal-state persistence, and release to use atomic compare-and-set transitions.
The `leaseId` fences an old execution even if the same `runId` is submitted again after expiry.

## Components and Public API

### `AguiResumeCoordinator`

The coordinator remains an internal implementation detail. It owns all state transitions and CAS
retry logic. Its default constructor uses `InMemoryAgentStateStore`; a second constructor accepts
an `AgentStateStore`. A package-private `Clock` injection point makes lease tests deterministic.

The current split operations are reshaped around a lease handle:

- `beginRun` atomically validates resume data and acquires ownership. It returns an internal
  handle containing `threadId`, `runId`, and `leaseId`, plus the pending-interrupt snapshot needed
  to build the runtime context.
- `renewRun` extends the expiry only while all handle fields still match.
- `completeRun` applies the `RunFinished` pending-interrupt transition and releases ownership in
  one CAS update.
- `releaseRun` is the fallback for cancellation or abnormal termination and only clears a matching
  lease.

All mutating operations use a small bounded CAS retry loop. The initial implementation uses at
most eight attempts; exhaustion is reported as a coordination conflict rather than spinning.

### `AguiRequestProcessor`

`AguiRequestProcessor.Builder` gains one public method:

```java
resumeStateStore(AgentStateStore store)
```

No store means a private in-memory store and the current single-process behavior. A supplied store
must report `supportsVersioning() == true`; otherwise processor construction fails with a clear
configuration error.

The MVC and WebFlux handler/controller builders expose the same pass-through method. They do not
contain storage or state-transition logic.

### Spring Boot starter

The starter adds the property:

```properties
agentscope.agui.resume.distributed-enabled=false
```

When false, the auto-configured processor uses its private in-memory store. When true, MVC and
WebFlux auto-configuration require exactly one `AgentStateStore` bean and pass it to the
processor. Startup fails if the bean is missing, ambiguous, or does not support versioning. This
explicit opt-in prevents an unrelated `AgentStateStore` bean from silently changing AG-UI
behavior.

No new Redis-specific Spring configuration is required. An application can use any supported
versioned store bean.

## Run Lifecycle

### Acquire and resume validation

Acquisition runs lazily at event-stream subscription time, preserving the current Reactor
semantics.

1. Read the versioned aggregate state for `threadId`; a missing value is an empty state at version
   zero.
2. If an active lease exists and has not expired, reject the request. This includes a duplicate
   request with the same `runId`.
3. Treat an expired lease as inactive.
4. Validate the submitted resume values against the pending-interrupt snapshot using the current
   exact-match rules:
   - resume input is rejected when no interrupt is pending;
   - resume input is required when interrupts are pending;
   - every response is resolved or cancelled;
   - response interrupt IDs are unique; and
   - response IDs exactly equal the pending interrupt IDs.
5. Save a new active lease with a fresh `leaseId` using CAS.
6. Return the lease handle and pending snapshot. The snapshot is used to add tool-call resume
   interrupts to `RuntimeContext`, avoiding a second inconsistent read.

If CAS loses a race, repeat the complete read, validation, and acquisition sequence. Validation is
therefore always applied to the state version being acquired.

### Renewal

The lease duration is derived from the existing `AguiAdapterConfig.runTimeout`, with a one-minute
minimum. The renewal interval is one third of the lease duration, capped at 30 seconds. This avoids
new public timing knobs while ensuring the lease stays valid during a healthy run.

Renewal only updates `leaseExpiresAt`. It does not touch pending interrupts and it performs no
write for ordinary AG-UI events. A renewal checks `threadId`, `runId`, and `leaseId`; mismatch means
ownership has been lost and the old event stream is terminated.

Store failures and CAS conflicts receive bounded retries. If ownership cannot be confirmed before
the lease can remain safe, the processor fails closed and terminates the run rather than allowing
two owners.

### Completion and interruption

On a matching `RunFinished` event:

- an interrupted outcome replaces `pendingInterrupts` with the event's interrupts;
- a non-interrupted outcome clears `pendingInterrupts`, unless a `RunError` was observed for the
  run; and
- the matching active lease is removed in the same CAS update.

This preserves the current rule that a run error does not discard previously pending interrupts.
The terminal event is forwarded only after this state transition succeeds.

### Cancellation and abnormal termination

When a stream is cancelled, times out, or fails without a successfully persisted `RunFinished`,
`doFinally` calls `releaseRun`. It removes only the matching lease and leaves pending interrupts
unchanged.

If fallback release fails after the stream is already terminal, the processor logs the failure.
The lease expiry provides recovery, so terminal cleanup does not need an unbounded retry loop.

### Crash and takeover

A crashed instance stops renewing. After expiry, a new request may replace its lease through CAS.
All later writes from the old instance are ignored or rejected because its `leaseId` no longer
matches. Pending interrupts remain until a valid resume completes successfully or a later
interrupted run replaces them; they have no TTL.

## Reactive Execution and Blocking Stores

`AgentStateStore` is synchronous, and distributed implementations may perform blocking network or
database I/O. The processor therefore wraps coordinator calls in Reactor publishers scheduled on
`Schedulers.boundedElastic()`:

- acquisition completes before the agent event stream is subscribed;
- renewal runs independently at the low-frequency interval;
- terminal persistence is sequenced before forwarding `RunFinished`; and
- fallback release is attempted during finalization.

The coordinator itself remains synchronous and independently unit-testable. MVC already executes
work on its request executor, while using the same processor composition keeps MVC and WebFlux
semantics aligned.

## Error Semantics

- Store read/write failure during acquisition rejects the run before agent execution starts.
- An unexpired active lease produces the existing active-run rejection behavior.
- CAS retry exhaustion produces a distinct coordination-conflict error.
- Loss of lease ownership terminates the old event stream and prevents terminal state writes.
- Failure to persist terminal state prevents a successful `RunFinished` from being forwarded.
- Transport adapters must preserve that fail-closed rule: when a run-owned terminal transition,
  lease renewal, or lease-fencing check fails, MVC and WebFlux must emit the error (if the
  connection is still writable) and close without fabricating a `RunFinished`. Their generic
  request/stream error helpers must distinguish these coordinator failures from pre-acquisition
  input/agent-resolution errors, for which the existing error lifecycle may remain unchanged.
- Failure during post-terminal fallback release is logged; natural expiry recovers the key.
- Resume validation errors retain their existing messages and behavior where practical.

The design intentionally fails closed. Continuing without confirmed ownership would violate the
single-active-run guarantee and could corrupt pending interrupts.

## Compatibility and Deployment

- Existing code that does not call `resumeStateStore` is source- and behavior-compatible.
- Spring Boot keeps distributed resume coordination disabled by default.
- No database or Redis schema dedicated to AG-UI is introduced; state uses the selected
  `AgentStateStore`'s existing layout.
- There is no migration of local map contents. Deployments should drain active AG-UI runs before
  first enabling the distributed store.
- Rolling instances must use the same store and configuration once the feature is enabled.
- Disabling the feature later returns to isolated process-local state and therefore should also be
  performed after draining active runs.

## Performance

The normal distributed path performs:

- one versioned read and one CAS write at acquisition;
- one low-frequency CAS renewal per renewal interval; and
- one versioned read and CAS write at completion or release.

CAS contention is limited to requests for the same `threadId`. Different threads use independent
state slots and can proceed concurrently. Ordinary streamed tokens and tool events do not cause
storage writes.

## Test Plan

### Coordinator unit tests

- Preserve every current resume-validation case and pending-interrupt transition.
- Verify that the only variable coordination identity is `threadId`.
- Reject a second active run for the same thread, including the same `runId`.
- Permit simultaneous runs for different threads.
- Share pending interrupts between two coordinators backed by one
  `InMemoryAgentStateStore`, simulating two instances.
- Allow takeover after lease expiry using a controllable `Clock`.
- Reject renewal, completion, and release from a stale `leaseId`.
- Verify interrupted completion stores pending interrupts and successful completion clears them.
- Verify run error and abnormal termination preserve prior pending interrupts.
- Retry injected CAS conflicts and fail after the retry bound.
- Reject a non-versioned store.

### Processor tests

- Acquire only when the returned `Flux` is subscribed.
- Inject the pending snapshot into `RuntimeContext` after acquisition.
- Keep the lease renewed during a long run.
- Renewal ticks are serialized (a slow store call cannot overlap the next tick), and the renewal
  publisher is cancelled on completion, error, cancellation, or lease loss.
- Terminate the stream when lease ownership is lost.
- Persist terminal state before forwarding `RunFinished`.
- Do not forward a fabricated `RunFinished` when terminal persistence or lease fencing fails; verify
  MVC and WebFlux error paths preserve this distinction.
- Release ownership on completion, error, cancellation, and subscription-time failure.
- Keep blocking coordinator operations off the caller/event-loop thread.

### Spring starter tests

- Default configuration uses local in-memory coordination.
- Distributed mode injects a unique versioned `AgentStateStore` into MVC and WebFlux paths.
- Distributed mode fails startup for a missing, ambiguous, or non-versioned store.
- Existing unrelated AG-UI properties and adapter configuration remain unchanged.

### Store integration confidence

The AG-UI module tests the contract against `InMemoryAgentStateStore` and CAS-conflict fakes. Redis,
MySQL, and PostgreSQL do not require AG-UI-specific implementations; their existing
`AgentStateStore` CAS contract tests remain the storage-level verification. No cross-module test
dependency on a Redis client is added to the AG-UI module.

The aggregate state must also round-trip through the serialization behavior of each supported
`AgentStateStore`; at minimum, tests cover nested `AguiEvent.Interrupt` records, including optional
and raw fields, rather than relying only on the in-memory implementation.

## Acceptance Criteria

- Two application instances sharing a versioned store cannot concurrently own the same
  `threadId`.
- An interrupt persisted by one instance can be resumed correctly by another.
- A crashed owner becomes replaceable after lease expiry.
- A stale owner cannot renew, release, or update pending interrupts after takeover.
- The default non-distributed path preserves current API and behavior.
- Distributed mode works through the existing `AgentStateStore` abstraction without new Maven
  modules or Redis-specific AG-UI code.
