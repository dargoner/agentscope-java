# Sandbox Workspace Key Design

## Summary

Move logical sandbox workspace identity generation from the OpenSandbox Redis provider into
`agentscope-harness`. The Harness will expose a provider-neutral `SandboxWorkspaceKey`, resolve it
once for Harness-managed sandbox acquisition, and pass it to sandbox clients that need stable
cross-call workspace reuse.

The OpenSandbox Redis module will consume the resolved key instead of combining
`SandboxIsolationKey` and `agentId` itself. Existing OpenSandbox Redis workspace IDs remain byte-for-byte
compatible. The generic Redis extension and `RedisSandboxExecutionGuard` are outside this change.

## Motivation

The current identity-aware `SandboxClient` overload accepts two separate identity ingredients:
`SandboxIsolationKey` and `agentId`. `RedisOpenSandboxClient` then owns the USER, SESSION, AGENT, and
GLOBAL composition rules and hashes the result into `workspaceId`.

This division has three problems:

1. Workspace identity is a Harness sandbox-domain concept, not an OpenSandbox Redis concept.
2. Other sandbox providers cannot reuse the same stable identity without copying Redis provider
   logic.
3. Two independent arguments can be inconsistent, such as an AGENT isolation key resolved for one
   agent combined with a different `agentId`.

There is also an existing SESSION inconsistency: Harness state storage uses only `sessionId`, while
OpenSandbox Redis workspace identity uses `agentId + sessionId`. SESSION isolation is intended to
give different agents in the same session separate workspaces, so the Harness state slot must also
include `agentId`.

## Goals

- Define a provider-neutral, immutable logical workspace key in `agentscope-harness`.
- Preserve the current OpenSandbox Redis v1 workspace ID exactly.
- Make SESSION workspace and persisted-state identity agent-specific.
- Let future sandbox providers consume the same key without depending on Redis or OpenSandbox.
- Keep execution-guard identity separate from remote workspace lifecycle identity.
- Preserve existing sandbox providers through `SandboxClient` default methods.

## Non-goals

- Do not modify `RedisSandboxExecutionGuard` or its Redis key format.
- Do not modify `agentscope-extensions-redis`.
- Do not create a generic Redis sandbox workspace coordinator in this change.
- Do not move OpenSandbox lifecycle state, leases, generation handling, snapshots, metadata
  discovery, or sweeping into Harness.
- Do not migrate or read the legacy SESSION state-slot format.
- Do not change existing `SandboxIsolationKey` or sandbox-session logging. Logging hardening is a
  separate concern from provider workspace identity.
- Do not require other sandbox providers to implement workspace-key-aware behavior now.

## Architecture

### `SandboxIsolationKey`

`SandboxIsolationKey` keeps its current purpose and representation. It controls Harness state
isolation and `SandboxExecutionGuard` acquisition. This change does not alter guard behavior or make
the guard use an opaque workspace ID.

### `SandboxWorkspaceKey`

Add an immutable class in package `io.agentscope.harness.agent.sandbox`:

```java
public final class SandboxWorkspaceKey {
    private final IsolationScope scope;
    private final String agentId;
    private final String stableId;

    public static SandboxWorkspaceKey from(
            SandboxIsolationKey isolationKey,
            String agentId);

    public IsolationScope getScope();
    public String getAgentId();
    public String getStableId();
}
```

The object contains the effective isolation scope, the agent identifier needed by providers for
diagnostics and records, and an opaque stable ID. It does not retain or expose the raw user or
session identifier.

`equals` and `hashCode` use `stableId`, which already includes the effective scope and every
identity-bearing component. In particular, GLOBAL keys created while different agents are running
remain equal because agent ID is diagnostic context rather than part of GLOBAL identity.
`toString` includes the effective scope and stable ID only; it must not reveal raw user or session
identifiers.

For GLOBAL, `getAgentId()` identifies the current borrower for diagnostics only. A provider may
record the most recent borrower's agent ID, but it must never use that value for workspace
ownership, equality, authorization, or access control.

The factory rejects a null isolation key and blank agent ID. For AGENT scope it also verifies that
the isolation-key value equals `agentId`, preventing contradictory identities. It rejects NUL in
`agentId` and in the isolation-key value because NUL is the canonical format's field separator;
allowing it inside a component would make distinct tuples produce the same canonical byte stream.

### Stable ID format

The factory builds the same canonical UTF-8 input currently used by `RedisOpenSandboxClient`:

```text
USER    -> v1\0user\0{agentId}\0{userId}
SESSION -> v1\0session\0{agentId}\0{sessionId}
AGENT   -> v1\0agent\0{agentId}
GLOBAL  -> v1\0global
```

It computes SHA-256 and encodes the digest with URL-safe Base64 without padding. This produces the
existing 43-character OpenSandbox Redis workspace ID while keeping raw user and session values out
of Redis keys and provider metadata.

The effective scope comes from `SandboxIsolationKey`; therefore a requested USER scope that falls
back because `userId` is missing produces a SESSION workspace key.

### `SandboxClient`

Replace the recently introduced identity-aware overloads with workspace-key-aware overloads:

```java
default Sandbox create(
        WorkspaceSpec workspaceSpec,
        SandboxSnapshotSpec snapshotSpec,
        O options,
        SandboxWorkspaceKey workspaceKey) {
    return create(workspaceSpec, snapshotSpec, options);
}

default Sandbox resume(SandboxState state, SandboxWorkspaceKey workspaceKey) {
    return resume(state);
}
```

The original three-argument `create` and one-argument `resume` methods remain unchanged. Existing
providers therefore keep their current behavior. The old identity-aware overloads accepting
`SandboxIsolationKey` and `agentId` are removed; this feature is not in use and only the
OpenSandbox Redis provider implements them in the repository. At design-review time, the commit
that introduced those overloads is contained in no release tag or remote branch, so this
replacement does not remove an API from a supported published artifact.

### `SandboxManager`

For Harness-managed Priority 3 and Priority 4 acquisition, `SandboxManager` resolves one
`SandboxIsolationKey` and derives the corresponding `SandboxWorkspaceKey` for the provider call.

- The execution guard and `SessionSandboxStateStore` continue receiving `SandboxIsolationKey`.
- Identity-aware sandbox client calls receive `SandboxWorkspaceKey`.
- Priority 1 external sandboxes and Priority 2 explicit external state keep their existing paths
  and do not receive a workspace key.
- Priority 3 persisted-state resume always has a resolved workspace key because persisted state is
  only loaded when an isolation key exists.
- Priority 4 calls the workspace-key-aware overload when a key exists. If no isolation key can be
  resolved, it calls the original create method.

The last rule preserves provider behavior. OpenSandbox Redis continues to fail closed through its
original create method rather than creating an unowned distributed workspace, while providers that
do not require a stable workspace identity continue normally.

`SandboxManager` and `SessionSandboxStateStore` currently receive agent ID independently. The
state store exposes its configured agent ID to the package, and the manager constructor must verify
that it equals the manager's agent ID and reject a mismatch. This preserves one effective identity
invariant even when callers construct these public classes directly rather than through
`HarnessAgent`.

### `SessionSandboxStateStore`

Change only the SESSION state-slot format:

```text
old: sandbox:session:{sessionId}
new: sandbox:session:{SandboxWorkspaceKey.stableId}
```

The new format makes two agents in the same session use independent persisted sandbox states, in
agreement with `SandboxWorkspaceKey`. The state store derives the stable ID through
`SandboxWorkspaceKey.from(isolationKey, configuredAgentId)`, so it uses the same canonical format as
the sandbox client. The result is fixed-length, path-safe, does not expose the two raw components,
and cannot become ambiguous when a component contains punctuation used by textual slot formats.
USER, AGENT, and GLOBAL formats keep their current behavior.

No legacy read, migration write, or legacy-key deletion is added because this feature has no
current users.

### OpenSandbox Redis provider

`RedisOpenSandboxClient` overrides the new workspace-key-aware create and resume methods. It:

- obtains `workspaceId` from `workspaceKey.getStableId()`;
- obtains effective isolation scope and agent ID from the same object;
- no longer combines raw isolation values with agent ID;
- no longer owns workspace stable-ID generation.

All OpenSandbox Redis-specific behavior remains in
`agentscope-extensions-sandbox-opensandbox-redis`, including:

- Redis key prefixes and cluster hash tags;
- workspace records and compare-and-set behavior;
- lifecycle locks, active leases, generations, and delete fences;
- idle and repair indexes;
- OpenSandbox metadata discovery;
- native snapshot naming and recovery;
- orphan tracking and lifecycle sweeping.

The module continues to depend directly on Redisson. It does not depend on
`agentscope-extensions-redis` merely to obtain a transitive Redis client dependency.

## Data Flow

```text
RuntimeContext + IsolationScope + agentId
  -> SandboxIsolationKey.resolve(...)
  -> SandboxWorkspaceKey.from(isolationKey, agentId)
  -> SandboxClient.create/resume(..., workspaceKey)
  -> provider uses stableId to locate its reusable logical workspace
```

The execution-guard path remains separate:

```text
SandboxIsolationKey
  -> SandboxExecutionGuard.tryEnter(isolationKey)
```

This separation is intentional: execution serialization and provider-managed remote workspace
lifecycle are different concerns and need not use the same backend key representation.

## Error Handling

- Null isolation keys and blank agent IDs are rejected when constructing a workspace key.
- NUL in agent ID or the isolation-key value is rejected before canonicalization.
- An AGENT key whose value differs from `agentId` is rejected.
- A `SandboxManager` whose agent ID differs from its `SessionSandboxStateStore` agent ID is rejected
  at construction time.
- `SandboxWorkspaceKey.toString()` never logs raw user or session values.
- The OpenSandbox Redis original create/resume methods continue to reject calls without a resolved
  workspace key.
- SHA-256 absence is treated as an illegal Harness runtime state. Standard supported JDKs provide
  SHA-256.

## Compatibility

### Preserved

- The canonical v1 strings, digest algorithm, encoding, and resulting OpenSandbox Redis
  `workspaceId` values are unchanged.
- Existing Redis records, locks, leases, indexes, metadata, and snapshot names remain addressable.
- Existing `SandboxClient` providers continue through default methods.
- OpenSandbox Redis fail-closed behavior remains.
- `RedisSandboxExecutionGuard` behavior and keys remain unchanged.

### Intentionally changed

- The identity-aware `SandboxClient` overload now takes one `SandboxWorkspaceKey` instead of
  `SandboxIsolationKey` plus `agentId`.
- SESSION persisted state now includes agent ID in its state-slot key.
- There is no fallback to the legacy SESSION state slot.

## Testing

### Harness

Add `SandboxWorkspaceKeyTest` covering:

- deterministic USER, SESSION, AGENT, and GLOBAL stable IDs;
- fixed vectors matching the current OpenSandbox Redis workspace IDs;
- USER-to-SESSION fallback using the effective scope;
- different agents, users, sessions, and scopes producing different keys where required;
- GLOBAL producing one shared stable ID;
- raw user and session identifiers absent from `stableId` and `toString`;
- null, blank, NUL-containing, and inconsistent AGENT inputs being rejected;
- equality and hash-code behavior.

Update `SandboxManagerIsolationTest` covering:

- Priority 3 and Priority 4 passing the derived workspace key;
- Priority 1 and Priority 2 bypassing workspace-key-aware methods;
- missing isolation context using the original create method;
- mismatched manager and state-store agent IDs being rejected;
- default-method compatibility for providers that ignore workspace keys.

Update `SessionSandboxStateStoreTest` covering:

- two agents with the same session ID using different state slots;
- SESSION round trips with the new key;
- SESSION identifiers containing colons, path separators, Unicode, and long values still producing
  one bounded, path-safe, unambiguous state-slot ID;
- USER, AGENT, and GLOBAL behavior remaining unchanged;
- state-slot IDs continuing to satisfy SQL-backed store path-character restrictions.

### OpenSandbox Redis

- Replace direct tests of `RedisOpenSandboxClient.workspaceId(...)` with
  `SandboxWorkspaceKey` fixed-vector tests and provider-consumption tests.
- Verify the provider uses `getStableId()` without re-deriving identity.
- Verify records retain the correct effective scope and agent ID.
- Verify that GLOBAL record agent ID is treated as last-borrower diagnostics and is not part of
  workspace identity or ownership.
- Keep workspace record, lease, generation, idle/repair, metadata discovery, snapshot, deletion,
  orphan, and sweeping tests passing.
- Keep missing-workspace-key fail-closed tests.
- Run the real Redis/OpenSandbox lifecycle and multi-client concurrency integration tests.

### Verification scope

- `agentscope-harness` tests;
- base OpenSandbox tests;
- OpenSandbox Redis unit tests;
- OpenSandbox Redis real-service integration tests;
- Maven package for affected modules and their required reactor dependencies.

No changes or new verification obligations are introduced for `agentscope-extensions-redis` or
other sandbox providers beyond ensuring the existing default methods compile.
