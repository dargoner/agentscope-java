# Sandbox Workspace Key Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move stable sandbox workspace identity into `agentscope-harness`, pass one provider-neutral key through Harness-managed acquisition, and make OpenSandbox Redis consume it without changing existing workspace IDs.

**Architecture:** `SandboxWorkspaceKey` derives the existing v1 SHA-256 identity from `SandboxIsolationKey` and `agentId`. `SandboxManager` resolves it once for Priority 3/4 provider calls, while `SessionSandboxStateStore` reuses the same stable ID only for SESSION slots. `RedisOpenSandboxClient` consumes the key and retains all Redis/OpenSandbox lifecycle coordination.

**Tech Stack:** Java 17, Maven, JUnit 5, AssertJ/JUnit assertions, Mockito, AgentScope `AgentStateStore`, OpenSandbox, Redisson.

## Global Constraints

- The logical workspace key belongs to `agentscope-harness`; do not add a Maven module.
- Preserve OpenSandbox Redis v1 workspace IDs byte-for-byte.
- SESSION persisted state must be agent-specific, fixed-length, path-safe, and unambiguous.
- Do not change `RedisSandboxExecutionGuard`, `agentscope-extensions-redis`, or their key formats.
- Do not migrate, read, or delete the legacy SESSION state-slot format.
- Do not move Redis lifecycle state, locks, leases, generations, indexes, snapshots, discovery, orphan handling, or sweeping into Harness.
- Priority 1 and Priority 2 acquisition remain identity-unaware.
- Existing sandbox providers continue through `SandboxClient` default methods.
- Remove the unreleased `SandboxIsolationKey + agentId` overloads instead of retaining bridges.
- Reject null isolation keys, blank agent IDs, embedded NULs, and inconsistent AGENT identities.
- `SandboxWorkspaceKey.toString()` must not expose raw user or session identifiers.
- `RedisSandboxExecutionGuard` and its original `scope:value` key remain unchanged.

---

### Task 1: Add provider-neutral `SandboxWorkspaceKey`

**Files:**
- Create: `agentscope-harness/src/main/java/io/agentscope/harness/agent/sandbox/SandboxWorkspaceKey.java`
- Create: `agentscope-harness/src/test/java/io/agentscope/harness/agent/sandbox/SandboxWorkspaceKeyTest.java`

**Interfaces:**
- Consumes: `SandboxIsolationKey#getScope()`, `SandboxIsolationKey#getValue()`, `IsolationScope`.
- Produces: `SandboxWorkspaceKey.from(SandboxIsolationKey, String)`, `getScope()`, `getAgentId()`, and `getStableId()`.

- [ ] **Step 1: Write failing fixed-vector and validation tests**

Create parameterized or individual tests that assert these exact vectors:

```java
assertEquals(
        "bH6znxIoEVo6UYUWGGrZjLO75Z8OCRyv8vBuEv95jUo",
        SandboxWorkspaceKey.from(key(USER, "user-1", "agent-a"), "agent-a").getStableId());
assertEquals(
        "EStM24l-_KWcHQEfeEqHUfL_-KuU9C3F8sKvW4tJ-NM",
        SandboxWorkspaceKey.from(key(SESSION, "session-1", "agent-a"), "agent-a").getStableId());
assertEquals(
        "PIRvK2OPgoG2B6ZrXsRmuVvImDClzWAT_DQL4DpiThE",
        SandboxWorkspaceKey.from(key(AGENT, null, "agent-a"), "agent-a").getStableId());
assertEquals(
        "KBNSCf5KkerW-ANyNovGvWdX99LGrNtfNU7YdKj48CQ",
        SandboxWorkspaceKey.from(key(GLOBAL, null, "agent-a"), "agent-a")
                .getStableId());

private static SandboxIsolationKey key(
        IsolationScope scope, String contextValue, String resolvedAgentId) {
    RuntimeContext context = switch (scope) {
        case USER -> RuntimeContext.builder().userId(contextValue).sessionId("fallback").build();
        case SESSION -> RuntimeContext.builder().sessionId(contextValue).build();
        case AGENT, GLOBAL -> null;
    };
    return SandboxIsolationKey.resolve(scope, context, resolvedAgentId).orElseThrow();
}
```

Cover USER-to-SESSION fallback through `SandboxIsolationKey.resolve`, stable ID length 43, scope and agent accessors, distinct identities, GLOBAL equality across agents, equality/hash code by stable ID, safe `toString`, null/blank input, NUL in both raw components, and an AGENT key whose value differs from `agentId`.

- [ ] **Step 2: Run the new test and verify RED**

Run:

```bash
mvn -pl agentscope-harness -Dtest=SandboxWorkspaceKeyTest test
```

Expected: compilation fails because `SandboxWorkspaceKey` does not exist.

- [ ] **Step 3: Implement the minimal immutable key**

Implement a final class whose factory uses these canonical values:

```java
String canonical = switch (scope) {
    case USER -> "v1\0user\0" + agentId + "\0" + isolationKey.getValue();
    case SESSION -> "v1\0session\0" + agentId + "\0" + isolationKey.getValue();
    case AGENT -> "v1\0agent\0" + agentId;
    case GLOBAL -> "v1\0global";
};
byte[] digest = MessageDigest.getInstance("SHA-256")
        .digest(canonical.getBytes(StandardCharsets.UTF_8));
String stableId = Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
```

Validate inputs before canonicalization. Convert `NoSuchAlgorithmException` into an
`IllegalStateException`. Make equality and hash code depend on `stableId`; make `toString` include
only scope and stable ID.

- [ ] **Step 4: Run the key tests and the existing isolation tests**

Run:

```bash
mvn -pl agentscope-harness \
  -Dtest=SandboxWorkspaceKeyTest,SandboxIsolationKeyTest test
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit Task 1**

```bash
git add agentscope-harness/src/main/java/io/agentscope/harness/agent/sandbox/SandboxWorkspaceKey.java \
        agentscope-harness/src/test/java/io/agentscope/harness/agent/sandbox/SandboxWorkspaceKeyTest.java
git commit -m "feat: add sandbox workspace key"
```

---

### Task 2: Pass one workspace key through Harness-managed acquisition

**Files:**
- Modify: `agentscope-harness/src/main/java/io/agentscope/harness/agent/sandbox/SandboxClient.java`
- Modify: `agentscope-harness/src/main/java/io/agentscope/harness/agent/sandbox/SandboxManager.java`
- Modify: `agentscope-harness/src/main/java/io/agentscope/harness/agent/sandbox/SessionSandboxStateStore.java`
- Modify: `agentscope-harness/src/test/java/io/agentscope/harness/agent/sandbox/SandboxManagerIsolationTest.java`

**Interfaces:**
- Consumes: `SandboxWorkspaceKey.from(SandboxIsolationKey, String)` from Task 1.
- Produces: `SandboxClient#create(..., SandboxWorkspaceKey)`, `SandboxClient#resume(SandboxState, SandboxWorkspaceKey)`, and package-visible `SessionSandboxStateStore#agentId()`.

- [ ] **Step 1: Replace recording-client tests with workspace-key behavior tests**

Update the test recording client to override:

```java
@Override
public Sandbox create(
        WorkspaceSpec spec,
        SandboxSnapshotSpec snapshotSpec,
        SandboxClientOptions options,
        SandboxWorkspaceKey workspaceKey) {
    this.createWorkspaceKey = workspaceKey;
    return create(spec, snapshotSpec, options);
}

@Override
public Sandbox resume(SandboxState state, SandboxWorkspaceKey workspaceKey) {
    this.resumeWorkspaceKey = workspaceKey;
    return resume(state);
}
```

Assert that Priority 3 resume and Priority 4 create receive the derived key, Priority 1/2 do not
invoke the key-aware methods, and missing isolation context invokes the original three-argument
`create`. Add a provider using only original methods to verify default-method compatibility. Add a
constructor test that rejects different manager and state-store agent IDs.

- [ ] **Step 2: Run manager tests and verify RED**

Run:

```bash
mvn -pl agentscope-harness -Dtest=SandboxManagerIsolationTest test
```

Expected: compilation fails because the new overloads and `agentId()` contract do not exist.

- [ ] **Step 3: Replace the `SandboxClient` overloads**

Replace the two unreleased overloads with:

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

- [ ] **Step 4: Update manager acquisition and identity validation**

Expose the configured state-store ID through package-visible `String agentId()`. In the
`SandboxManager` constructor, reject a mismatch with `IllegalArgumentException`.

For Priority 3/4, derive the key once:

```java
Optional<SandboxWorkspaceKey> workspaceKey =
        scopeKey.map(key -> SandboxWorkspaceKey.from(key, agentId));
```

Pass it to key-aware resume/create only when present. Keep the execution guard and state store on
`SandboxIsolationKey`. Keep Priority 1/2 on the original methods.

- [ ] **Step 5: Run Harness manager and middleware tests**

Run:

```bash
mvn -pl agentscope-harness \
  -Dtest=SandboxManagerIsolationTest,SandboxLifecycleMiddlewareTest test
```

Expected: all selected tests pass.

- [ ] **Step 6: Commit Task 2**

```bash
git add agentscope-harness/src/main/java/io/agentscope/harness/agent/sandbox/SandboxClient.java \
        agentscope-harness/src/main/java/io/agentscope/harness/agent/sandbox/SandboxManager.java \
        agentscope-harness/src/main/java/io/agentscope/harness/agent/sandbox/SessionSandboxStateStore.java \
        agentscope-harness/src/test/java/io/agentscope/harness/agent/sandbox/SandboxManagerIsolationTest.java
git commit -m "refactor: pass sandbox workspace key through harness"
```

---

### Task 3: Make SESSION persisted-state slots agent-specific and safe

**Files:**
- Modify: `agentscope-harness/src/main/java/io/agentscope/harness/agent/sandbox/SessionSandboxStateStore.java`
- Modify: `agentscope-harness/src/test/java/io/agentscope/harness/agent/sandbox/SessionSandboxStateStoreTest.java`

**Interfaces:**
- Consumes: `SandboxWorkspaceKey.from(...)` and `getStableId()` from Task 1.
- Produces: SESSION slot `sandbox:session:{stableId}`; USER, AGENT, and GLOBAL formats remain unchanged.

- [ ] **Step 1: Add failing SESSION slot tests**

Use a recording `AgentStateStore` to capture the session ID. Assert:

```java
assertNotEquals(slotFor("agent-a", "shared-session"),
                slotFor("agent-b", "shared-session"));
assertTrue(slotFor("agent-a", "a:b/\\长" ).matches("sandbox:session:[A-Za-z0-9_-]{43}"));
assertTrue(slotFor("agent-a", "x".repeat(1000)).length() <= 255);
```

Also round-trip values containing colons, slashes, backslashes, Unicode, and long text. Assert USER,
AGENT, and GLOBAL slot IDs remain exactly as before, and verify two agents in one session cannot
load each other's persisted state.

- [ ] **Step 2: Run state-store tests and verify RED**

Run:

```bash
mvn -pl agentscope-harness -Dtest=SessionSandboxStateStoreTest test
```

Expected: the two-agent SESSION isolation assertion fails because the current slot contains only
the raw session ID.

- [ ] **Step 3: Change only the SESSION slot branch**

Implement:

```java
case SESSION ->
        "sandbox:session:" + SandboxWorkspaceKey.from(key, agentId).getStableId();
case USER -> "sandbox:user:" + agentId + ":" + key.getValue();
case AGENT -> "sandbox:agent:" + agentId;
case GLOBAL -> "sandbox:global";
```

Do not add legacy reads, migration writes, or deletion of the old slot.

- [ ] **Step 4: Run key, manager, and state-store tests**

Run:

```bash
mvn -pl agentscope-harness \
  -Dtest=SandboxWorkspaceKeyTest,SandboxManagerIsolationTest,SessionSandboxStateStoreTest test
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit Task 3**

```bash
git add agentscope-harness/src/main/java/io/agentscope/harness/agent/sandbox/SessionSandboxStateStore.java \
        agentscope-harness/src/test/java/io/agentscope/harness/agent/sandbox/SessionSandboxStateStoreTest.java
git commit -m "fix: isolate sandbox session state by agent"
```

---

### Task 4: Make OpenSandbox Redis consume `SandboxWorkspaceKey`

**Files:**
- Modify: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox-redis/src/main/java/io/agentscope/extensions/sandbox/opensandbox/redis/RedisOpenSandboxClient.java`
- Modify: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox-redis/src/test/java/io/agentscope/extensions/sandbox/opensandbox/redis/RedisOpenSandboxClientTest.java`
- Modify: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox-redis/src/test/java/io/agentscope/extensions/sandbox/opensandbox/redis/RedisOpenSandboxClientTest.java`
- Modify: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox-redis/src/test/java/io/agentscope/extensions/sandbox/opensandbox/redis/RedisManagedOpenSandboxTest.java`
- Modify: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox-redis/src/test/java/io/agentscope/extensions/sandbox/opensandbox/redis/OpenSandboxRedisLifecycleOptionsTest.java`
- Modify: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox-redis/src/test/java/io/agentscope/extensions/sandbox/opensandbox/redis/OpenSandboxRedisRealServiceIntegrationTest.java`
- Modify: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox-redis/src/test/java/io/agentscope/extensions/sandbox/opensandbox/redis/OpenSandboxRedisConcurrencyIntegrationTest.java`

**Interfaces:**
- Consumes: `SandboxWorkspaceKey#getStableId()`, `getScope()`, and `getAgentId()`.
- Produces: Redis/OpenSandbox lifecycle behavior with no provider-side stable-ID derivation.

- [ ] **Step 1: Convert provider tests to construct workspace keys**

Replace calls shaped like:

```java
client.create(spec, null, options, isolationKey("user-1"), "agent-a");
```

with:

```java
SandboxWorkspaceKey key =
        SandboxWorkspaceKey.from(isolationKey("user-1"), "agent-a");
client.create(spec, null, options, key);
```

Replace direct `RedisOpenSandboxClient.workspaceId(...)` assertions with `key.getStableId()`.
Capture saved `OpenSandboxWorkspaceRecord` values and assert stable ID, effective scope, and agent
ID come from the supplied key. Add a GLOBAL test showing different borrowers share the stable ID
while the record's agent ID is only the latest diagnostic borrower. Retain missing-key fail-closed
tests for the original create/resume methods.

- [ ] **Step 2: Run provider tests and verify RED**

Run:

```bash
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox-redis \
  -am -Dtest=RedisOpenSandboxClientTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: compilation fails because `RedisOpenSandboxClient` still overrides the removed overloads.

- [ ] **Step 3: Replace provider identity parameters with the key**

Update create/resume/borrow/record methods to accept `SandboxWorkspaceKey`. Use:

```java
String workspaceId = workspaceKey.getStableId();
IsolationScope scope = workspaceKey.getScope();
String agentId = workspaceKey.getAgentId();
```

Remove `workspaceId(SandboxIsolationKey, String)`, the raw canonicalization code, and now-unused
hashing imports. Update the fail-closed message to require a resolved `SandboxWorkspaceKey`. Do not
change lifecycle keys, records, leases, generations, metadata, snapshots, repair/idle indexes,
deletion, or sweeping.

- [ ] **Step 4: Run all OpenSandbox Redis unit tests**

Run:

```bash
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox-redis \
  -am -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: all module and required-reactor unit tests pass.

- [ ] **Step 5: Commit Task 4**

```bash
git add agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox-redis/src/main/java/io/agentscope/extensions/sandbox/opensandbox/redis/RedisOpenSandboxClient.java \
        agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox-redis/src/test/java/io/agentscope/extensions/sandbox/opensandbox/redis
git commit -m "refactor: consume harness sandbox workspace key"
```

---

### Task 5: Verify affected modules and real services

**Files:**
- Modify only if verification exposes a defect within this specification's scope.

**Interfaces:**
- Consumes: completed Tasks 1-4.
- Produces: verified Harness and OpenSandbox artifacts with real Redis/OpenSandbox compatibility.

- [ ] **Step 1: Run the complete Harness test suite**

```bash
mvn -pl agentscope-harness test
```

Expected: all tests pass.

- [ ] **Step 2: Run base OpenSandbox and Redis module suites**

```bash
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox,agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox-redis \
  -am -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: all tests pass.

- [ ] **Step 3: Run configured real-service tests**

Use the configured development Redis and OpenSandbox endpoint, then run:

```bash
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox-redis \
  -am \
  -Dtest=OpenSandboxRedisRealServiceIntegrationTest,OpenSandboxRedisConcurrencyIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: integration tests run rather than skip and pass against the configured services.

- [ ] **Step 4: Package all affected modules**

```bash
mvn -pl agentscope-harness,agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox,agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox-redis \
  -am -DskipTests package
```

Expected: Maven exits 0.

- [ ] **Step 5: Check formatting, diff scope, and repository status**

```bash
mvn spotless:check
git diff --check
git status --short
```

Expected: formatting and whitespace checks pass; only planned files and pre-existing untracked
files remain.

- [ ] **Step 6: Commit any verification-only correction**

If verification required an in-scope correction, rerun the failing command and commit only that
correction:

```bash
git add -u -- agentscope-harness agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox-redis
git commit -m "fix: address sandbox workspace key verification"
```

If no correction was needed, do not create an empty commit.
