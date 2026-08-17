---
title: "Sandbox"
description: "Isolated execution + cross-call recovery + multi-replica deployment"
---

> For the three filesystem-mode comparison see [Filesystem](./filesystem.md). This page focuses on sandbox mode usage.

## What sandbox solves

Confines the agent's **file operations and command execution** to an isolated environment; the host stays untouched. Plus three extra wins:

1. **Execution boundary** — untrusted input, suspicious scripts, `rm -rf`-shaped commands all stay inside the sandbox.
2. **Cross-call recovery** — not just conversation state: `pip install`, `npm install`, generated temp files (the executable environment itself) are snapshotted, so the next `call()` resumes in the same sandbox without reinstalling.
3. **Multi-replica friendly** — when multiple replicas serve the same logical user, sandbox state can share a single slot so any node can resume the same workspace.

## A minimal example

Local Docker, isolated per user:

```java
HarnessAgent agent = HarnessAgent.builder()
    .name("code-agent")
    .model(model)
    .workspace(workspace)
    .filesystem(new DockerFilesystemSpec()
        .image("ubuntu:24.04"))
    .build();

agent.call(msg, RuntimeContext.builder()
    .userId("alice")
    .sessionId("conv-1")
    .build()).block();
```

Same `userId` across `call()` → automatically reuses the same sandbox (or restores from snapshot). Different `userId` → separate sandbox. When `userId` is absent, falls back to `sessionId` as the isolation key.

## IsolationScope — who shares a sandbox

All sandbox configuration lives on the `SandboxFilesystemSpec` (e.g. `DockerFilesystemSpec`). The key parameter is `isolationScope`:

| Scope | Sharing | Typical use |
|-------|---------|-------------|
| `USER` (default) | Same `userId`'s sessions share; falls back to `SESSION` when userId is absent | Multi-user SaaS — each user keeps one workspace across conversations |
| `SESSION` | Each sessionId independent | Strict per-conversation isolation |
| `AGENT` | All users / sessions of this agent share | Public-tool-type agent, shared knowledge base |
| `GLOBAL` | One shared slot per store | Use with care |

```java
// Explicit SESSION scope (overrides default USER)
.filesystem(new DockerFilesystemSpec()
    .image("ubuntu:24.04")
    .isolationScope(IsolationScope.SESSION))
```

`SESSION` is naturally concurrency-safe (each session has its own slot). `USER` / `AGENT` / `GLOBAL` in multi-replica deployments should pair with a mutex (see "Concurrency control" below).

**USER-scope fallback:** when `IsolationScope.USER` is active (either explicitly or by default) but `RuntimeContext.userId` is absent, the framework automatically falls back to `SESSION` scope using `sessionId`. This means you don't need to guard against missing userId — the sandbox degrades gracefully.

## Cross-call recovery = snapshots

The sandbox snapshots its workspace at each `call()` end and restores at the next start:

- Container still alive + workspace still there → just continue (fastest)
- Container gone → reboot from snapshot, restore workspace
- No snapshot → full init from `WorkspaceSpec` (cold start)

Where snapshots land is decided by `snapshotSpec`:

| Option | When |
|--------|------|
| `NoopSnapshotSpec` (default) | No persistence; cold start when the container is gone |
| `LocalSnapshotSpec` | Host local file (single-machine long-running) |
| `OssSnapshotSpec` | OSS / S3-compatible (multi-replica) |
| `RedisSnapshotSpec` | Redis (low latency, small workspaces) |
| `JdbcSnapshotSpec` | MySQL / JDBC BLOB (existing relational DB) |

```java
.filesystem(new DockerFilesystemSpec()
    .image("ubuntu:24.04")
    .snapshotSpec(new OssSnapshotSpec(ossClient, "my-bucket", "agentscope/")))
```

Host-side workspace files (`AGENTS.md` / `skills/` / `subagents/` / `knowledge/`) are synced into the sandbox at each start, content-hash-gated. So if you edit a script under `skills/`, the next `call()` has the new version inside the sandbox.

## Distributed deployment

When multiple replicas run the same agent and any replica must be able to pick up the same user's conversation, you need:

1. A distributed `AgentStateStore` (e.g. Redis-backed) — passed via `.stateStore(...)` on the builder
2. A non-`Noop` snapshot (OSS / Redis / remote store) — configured directly on the filesystem spec via `.snapshotSpec(...)`
3. An appropriate `IsolationScope` (default `USER` is usually correct)

Everything is configured in one place:

```java
HarnessAgent.builder()
    .name("assistant")
    .model(model)
    .workspace(workspace)
    .stateStore(redisStateStore)                    // distributed state
    .filesystem(new DockerFilesystemSpec()
        .image("ubuntu:24.04")
        .snapshotSpec(ossSnapshotSpec)              // cross-replica snapshot
        .isolationScope(IsolationScope.USER))       // default, can omit
    .build();
```

The framework stores sandbox metadata (container ID, snapshot pointers, workspace-ready flag) in the same `AgentStateStore` that holds agent runtime state. Providing a distributed store automatically enables cross-replica sandbox resume — no extra configuration needed.

If you're using a local `AgentStateStore` (the default `JsonFileAgentStateStore`) with sandbox mode, the framework logs a warning at build time reminding you that sandbox state won't survive JVM restarts and can't be shared across instances.

## Concurrency control (multi-replica)

In `USER` / `AGENT` / `GLOBAL` modes across replicas, two replicas serving the same user concurrently both write to the same slot — last writer wins. If that's not OK, you need a distributed lock.

**Recommended**: use `distributedStore(...)` — snapshot and execution guard are auto-injected:

```java
DistributedStore store = RedisDistributedStore.fromJedis(jedis);

HarnessAgent.builder()
    .distributedStore(store)    // auto-wires stateStore + snapshotSpec + executionGuard
    .filesystem(new DockerFilesystemSpec()
        .image("ubuntu:24.04")
        .isolationScope(IsolationScope.USER))
    .build();
```

To customize lock parameters, set the guard explicitly on the `SandboxFilesystemSpec`:

```java
.filesystem(new DockerFilesystemSpec()
    .image("ubuntu:24.04")
    .isolationScope(IsolationScope.USER)
    .executionGuard(RedisSandboxExecutionGuard.builder(jedis)
        .leaseTtl(Duration.ofMinutes(30)).build()))
```

Built-in implementations: `RedisSandboxExecutionGuard` (Redis `SET NX PX`), `JdbcSandboxExecutionGuard` (MySQL `GET_LOCK()`). You can also implement `SandboxExecutionGuard` to plug in Zookeeper, etcd, or other lock stores.

## Self-managed sandbox instances (advanced)

By default the framework owns the whole sandbox lifecycle. Three "I'll manage it myself" scenarios:

**1. I already have a running container; I want the agent to use it**

```java
Sandbox mySandbox = dockerClient.create(workspaceSpec, snapshotSpec, options);
mySandbox.start();

SandboxContext callCtx = SandboxContext.builder()
    .client(dockerClient)
    .externalSandbox(mySandbox)       // framework only stops() at end of call, doesn't shutdown()
    .build();

agent.call(msgs, RuntimeContext.builder()
    .sessionId("my-session")
    .put(SandboxContext.class, callCtx)
    .build()).block();

// shut it down yourself when done
mySandbox.shutdown();
```

**2. I have a specific snapshot string; restore to that moment**

```java
SandboxState savedState = dockerClient.deserializeState(savedStateJson);
SandboxContext callCtx = SandboxContext.builder()
    .client(dockerClient)
    .externalSandboxState(savedState)  // framework restores from this state but owns the lifecycle
    .build();
```

**3. Multiple agents share one sandbox**

Pass the same `externalSandbox` to each agent's `call()`, then `shutdown()` it yourself when done.

## Choosing a sandbox store

| Store | Best for |
|---------|----------|
| **Docker** | Local dev / single machine / trusted shell |
| **Kubernetes** | Self-hosted K8s; fully based on [agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox) (SandboxClaim / WarmPool), workspace persistence via PVC (see below) |
| **Daytona** | Generic managed sandbox HTTP API |
| **E2B** | Generic managed sandbox + native platform snapshots |
| **OpenSandbox** | Self-hosted or managed OpenSandbox service with lifecycle, command execution, native file transfer, and Harness snapshots |
| **AgentRun** | Aliyun-managed sandbox (Function Compute FC 3.0); per-instance NAS / OSS auto-mount; mainland-China low latency. Treated as a regular `SandboxFilesystemSpec` — full setup details (templates, RAM permissions, NAS-first config) live in the integration docs |

All stores implement the same interface; agent code, toolkit, and `AGENTS.md` don't change.

### OpenSandbox

Add the standalone extension module:

```xml
<dependency>
    <groupId>io.agentscope</groupId>
    <artifactId>agentscope-extensions-sandbox-opensandbox</artifactId>
    <version>${agentscope.version}</version>
</dependency>
```

```java
new OpenSandboxFilesystemSpec()
        .endpoint("http://localhost:8080")
        .apiKey(System.getenv("OPEN_SANDBOX_API_KEY"))
        .image("ubuntu:22.04")
        .workspaceRoot("/workspace");
```

Inject the API key from the environment or external configuration; it is never serialized into sandbox state. The OpenSandbox adapter owns sandbox lifecycle, shell commands, and file transfer only. MCP processes and connections remain host-side.

#### Clustered OpenSandbox lifecycle with Redis

For stable workspace reuse across application replicas, use the Redis coordination module. It requires an application-configured `RedissonClient`; the module does not create a Redis connection or read Redis passwords.

```xml
<dependency>
    <groupId>io.agentscope</groupId>
    <artifactId>agentscope-extensions-sandbox-opensandbox-redis</artifactId>
    <version>${agentscope.version}</version>
</dependency>
```

```java
// Defaults: http://localhost:8080, no API key, USER isolation,
// 60-minute idle TTL, and a 5-minute sweep interval.
RedisOpenSandboxFilesystemSpec filesystemSpec =
        new RedisOpenSandboxFilesystemSpec(redissonClient);
```

Configure optional control-plane credentials only when they are present:

```java
OpenSandboxClientOptions openSandboxOptions = new OpenSandboxClientOptions();
String endpoint = System.getenv("OPEN_SANDBOX_ENDPOINT");
if (endpoint != null && !endpoint.isBlank()) {
    openSandboxOptions.setEndpoint(endpoint);
}
String apiKey = System.getenv("OPEN_SANDBOX_API_KEY");
if (apiKey != null && !apiKey.isBlank()) {
    openSandboxOptions.setApiKey(apiKey);
}

OpenSandboxRedisLifecycleOptions lifecycleOptions =
        new OpenSandboxRedisLifecycleOptions();
WorkspaceSpec workspaceSpec = new WorkspaceSpec();
workspaceSpec.setRoot("/workspace");

filesystemSpec
        .clientOptions(openSandboxOptions)
        .lifecycleOptions(lifecycleOptions)
        .workspaceSpec(workspaceSpec)
        .isolationScope(IsolationScope.USER);
```

`redissonClient` above is created and configured by the application. `USER` is the default isolation scope and derives a stable workspace identity from `userId + agentId`; `SESSION`, `AGENT`, and `GLOBAL` are available when their sharing boundaries fit the deployment better. The OpenSandbox endpoint and API key are client-level credentials and must remain stable for the lifetime of a `RedisOpenSandboxClient`; switching either credential per call fails closed.

The image and entrypoint form the runtime profile of a workspace and must also remain stable while that workspace exists. A different profile fails closed: explicitly delete the workspace and then recreate it with the new profile. Delete removes its historical native snapshots and advances the tombstone so the old generation cannot be rediscovered. Parameters that apply only during remote creation, such as resource limits and initial metadata, are not reapplied when an existing sandbox is reused.

The Redis lifecycle lock protects short create, resume, pause, snapshot, delete, and metadata transitions. It does **not** cover an entire Turn, so Turns and Subagents may use independent leases concurrently. The spec preconfigures the official no-op `SandboxExecutionGuard` and a `NoopSnapshotSpec` marker so `distributedStore(...)` does not auto-wire its full-Turn guard or tar snapshot backend into this native-snapshot mode; the distributed store can still provide agent state, task, messaging, and other facilities. Do not configure any other `SandboxExecutionGuard` for this mode. Concurrent writes to the same shared files can still conflict; coordinate them at the application level when write order matters. A detached process does not renew a lease by itself: keep a Turn active while it must run, or adjust the lifecycle settings.

Defaults are an idle TTL of 60 minutes, a sweep every 5 minutes, and a 5-minute pause retention. Eviction grace and snapshot readiness waiting come from `OpenSandboxRedisLifecycleOptions` (both default to 5 minutes). After native snapshot and pause complete, the remote sandbox is renewed for `pauseRetention` (5 minutes by default) and is ultimately reclaimed according to OpenSandbox `expiresAt`.

This mode uses OpenSandbox **native snapshots**, which preserve the remote filesystem and may therefore contain source, generated artifacts, credentials, or other sensitive files. Apply the OpenSandbox service's access controls, encryption, retention, and deletion policy accordingly. Redis stores only workspace and snapshot IDs, lifecycle metadata, delete tombstones, and active leases. It stores neither file contents nor the OpenSandbox API key. Do not configure a persistence-enabled tar `SandboxSnapshotSpec` alongside native snapshots; only `null` or `NoopSnapshotSpec` is accepted.

Recovery tries the current native snapshot and then the previous one, and fails closed instead of silently starting an empty workspace when both known snapshots fail. If Redis metadata is lost, the client deterministically discovers matching OpenSandbox sandboxes and native snapshots and rebuilds metadata; a prior explicit delete is fenced by its tombstone so deleted generations cannot be resurrected. Explicit delete advances that tombstone before remote cleanup.

The application owns and closes the injected Redisson client. Any prebuilt sandbox client injected with `.client(...)`, client used directly outside `HarnessAgent.builder().filesystem(...)`, or client supplied through a per-call `SandboxContext` remains caller-owned and is never closed by the agent. When the spec constructs the default `RedisOpenSandboxClient`, the agent owns that client and closes it once from `HarnessAgent.close()`, stopping its daemon sweeper scheduler. The internally constructed Redis client creates its OpenSandbox SDK/control-plane object from `clientOptions`; there is currently no separate public close operation for that object. Closing a borrowed sandbox handle releases that handle's lease. Neither closing the Redis client nor closing a borrowed handle shuts down Redisson. This module does not provide MCP integration, and MCP connection lifecycle is outside its scope.

## Runtime image contract

You choose the sandbox image (Docker `image`, the Kubernetes agent-sandbox runtime image, etc.), but **not every image works**. Harness file tools (`read_file` / `write_file` / `edit_file` / `grep_files` / `glob_files` / `list_files`) and the snapshot machinery are all implemented by running POSIX shell commands inside the sandbox. The image must satisfy the contract below, or tools will fail in hard-to-diagnose ways.

### Baseline contract (all backends)

The image must provide:

| Category | Requirement | Used by |
|----------|-------------|---------|
| Shell | POSIX-compatible `sh` (`[ ]`, `&&`, pipes, redirection, heredocs) | every file tool, the `execute` tool |
| Core utils | `mkdir` `dirname` `rm` `mv` `test` `printf` `sort` | individual file operations |
| Text/search | `sed`, `grep` (with `-rHnF`, `--include`), `find` | `read_file` pagination, `grep_files`, `glob_files` |
| Metadata | GNU-style `stat -c` (not BSD `stat -f`) | `list_files`, `glob_files` |
| Archive/encoding | `tar`, `base64` (encode + `-d` decode) | snapshot persist/hydrate, file upload/download |
| Interpreter | `python3` | `edit_file` (exact string replacement) |
| Filesystem | writable workspace root (default `/workspace`) | everything |

Images based on `ubuntu:24.04` or `debian` qualify out of the box (`python3` may need installing); `alpine` (BusyBox `stat` / `grep` behave differently) and distroless images do **not**.

Quick conformance check (run inside the image; all must succeed):

```bash
sh -c 'echo ok' && python3 --version && tar --version \
  && printf x | base64 | base64 -d && stat -c %Y /tmp && grep -rHnF --include='*.txt' x /tmp; true
```

### Additional contract for Kubernetes (agent-sandbox)

The agent-sandbox backend does not `kubectl exec` into the container; it talks to an HTTP API exposed by the runtime container (default port 8888). The runtime service in your image must implement:

| Endpoint | Semantics |
|----------|-----------|
| `POST /execute` (body `{"command": "..."}`) | **Must interpret the command with POSIX shell semantics** (equivalent to `sh -c`) and return `{"stdout", "stderr", "exit_code"}`. Harness sends commands containing `cd ... && (...)`, pipes, and other shell constructs |
| `POST /upload` (multipart; `filename` is a relative path) | writes a file under the file-API base directory |
| `GET /download/{path}` | downloads file bytes by relative path |
| `GET /list/{path}`, `GET /exists/{path}` | directory listing / existence check |

**The file-API base directory must match the workspace root** (recommended: `/workspace` for both). Harness transfers two kinds of content through `/upload` / `/download`: workspace snapshot tarballs (temp files under `.agentscope-tmp/` inside that directory), and single-file bytes for `write_file` / file downloads when the path lies under the base directory (Linux caps a single command-line argument at about 128 KiB; native file-API transfer has no such limit). The base directory is configured via `KubernetesSandboxClientOptions.fileApiBaseDir` (default `/workspace`); setting it blank falls back to base64-over-exec transfer.

> Note: the example runtime in the upstream agent-sandbox repo (`examples/python-runtime-sandbox`) runs commands via `shlex.split` + `subprocess.run` without a shell, and roots its file API at `/app` — it does **not** conform to this contract and should only be used as a reference for endpoint shapes. Upstream KEP-539.2 is standardizing the runtime interface (REST/gRPC spec + conformance tests); this contract can converge on the official spec once it lands.

### Why it works this way

The `Sandbox` abstraction's primary data-plane entry point is `exec(command)`. This is deliberate — tool semantics like `edit_file` / `grep_files` (regex, string replacement, globbing) cannot be expressed through a small set of file-API endpoints; running shell scripts against a standard toolchain inside the image is the only portable answer. The file API (upload/download) handles pure byte transfer only: workspace snapshots and single-file upload/download go through it (backends declare the capability by implementing the optional `SandboxFileTransfer` interface); everything else goes through `execute`. Which also means: **the image contract is part of the sandbox interface** — run the conformance check above before switching images.

**Crossing the boundary from the model's point of view.** The file API (upload/download) above is an internal mechanism — invisible to the LLM, and `FilesystemTool` exposes no transfer tools. The supported way for a sandboxed agent to hand an artifact it produced to a destination outside the sandbox is the generic **`deliver_artifact`** tool. It is registered only when you configure an `ArtifactDeliveryTarget` via `HarnessAgent.builder().artifactDeliveryTarget(...)`. The SPI stays business-agnostic — `deliver(RuntimeContext, ArtifactDeliveryRequest) -> ArtifactDeliveryResult` — so destination logic (e.g. WebDAV upload) lives in your application. The tool downloads the file bytes from the sandbox workspace and delegates the transport to the target. Without a configured target, the sandbox workspace prompt states plainly that files cannot leave the container.

## Kubernetes state persistence: PVC is the first layer

The Kubernetes store is fully based on agent-sandbox: sandbox pods are managed by the agent-sandbox controller, and image, resources, and storage are all declared cluster-side in a `SandboxTemplate` / `SandboxWarmPool` — the Java side only claims instances (`SandboxClaim`) and connects. This makes it different from other stores in one important way: **workspace data persistence is primarily the PVC's job, not the Harness snapshot's**. The two layers each own one thing:

| Layer | What it preserves | Recovery scenario |
|-------|-------------------|-------------------|
| **PVC** (`SandboxTemplate.volumeClaimTemplates`) | the workspace files themselves | pod restart / eviction / hibernate-and-wake — the claim is still alive, files come back with the volume, zero transfer |
| **SandboxState + snapshotSpec** (Harness layer) | identity pointer (claimName / namespace) + workspace tarball snapshot | locating the sandbox on resume (always required); cold recovery when the claim was deleted / PVC lost / cross-cluster |

The framework needs no special handling for PVCs: on resume, startup probes `test -d /workspace`; when the PVC is there, the "workspace preserved" branch is taken and snapshot restore is skipped entirely.

**Three things you must configure correctly:**

1. **The PVC must be mounted at `workspaceRoot`** (default `/workspace`). Mount it elsewhere or use `emptyDir`, and the workspace is gone on every pod restart — each call degrades to snapshot restore or a cold start. Reference template (from the upstream agent-sandbox examples):

```yaml
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxTemplate
spec:
  podTemplate:
    spec:
      containers:
      - name: runtime
        image: your-conformant-runtime:latest   # must satisfy the runtime image contract above
        ports:
        - containerPort: 8888
        volumeMounts:
        - name: workspace
          mountPath: /workspace                 # = workspaceRoot = fileApiBaseDir
  volumeClaimTemplates:
  - metadata:
      name: workspace
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 1Gi
```

2. **Mind the claim lifecycle boundary.** Once `shutdownTime` / `shutdownPolicy: Delete` removes the Sandbox, PVC-based warm recovery is over — the next resume won't find the claim, and the framework creates a fresh sandbox (with an empty new PVC). Whether the workspace comes back depends on point 3.

3. **Pick `snapshotSpec` accordingly.** PVC + `NoopSnapshotSpec` (default): skips the tar-and-upload at the end of every call, at the cost of a cold start once the claim is gone. PVC + OSS / Redis snapshot: belt and suspenders — claim expiry, PVC loss, and cross-cluster migration all recover from the snapshot, at the cost of a full archive per call.

One more reminder: the `SandboxState` identity layer is never optional — in multi-replica deployments configure a distributed `AgentStateStore`, otherwise other replicas can't learn the claimName and the data on the PVC is unreachable no matter how intact it is.

## How the workspace maps into the sandbox

Host-side key files under `workspace/` (`AGENTS.md`, `skills/`, `subagents/`, `knowledge/`) are synced into the sandbox at each start, content-hash-gated — unchanged content is skipped.

To bind a host directory into the sandbox (e.g. a code repo), use `BindMountEntry` (Docker only; for Kubernetes, declare mounts in the cluster-side `SandboxTemplate` pod template instead; managed sandboxes like Daytona / E2B run in the cloud and can't mount your host paths).

File changes inside the sandbox don't sync back to the host — to retrieve sandbox-produced artifacts, have the agent `read_file` them.

## Implementing your own sandbox store

To integrate a non-Docker isolation environment (self-hosted remote executor, commercial sandbox API, local mock, etc.), no Harness source changes needed — implement a few contract interfaces and pass them to `filesystem(...)`. The `InMemorySandbox` family under `agentscope-harness` tests is the minimal skeleton to copy.

## Related pages

- [Filesystem](./filesystem.md) — three declarative modes compared
- [Workspace](./workspace.md) — which files under `workspace/` sync into the sandbox
- [Architecture](./architecture.md) — where sandbox acquire / release sits in the call() timeline
