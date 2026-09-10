# OpenSandbox Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an AgentScope Java Sandbox Provider backed by the official OpenSandbox Java SDK, with lifecycle, command, native file transfer, state recovery, documentation, and real-service verification.

**Architecture:** A new optional extension module follows the existing Daytona/E2B/AgentRun provider structure. A package-private SDK boundary isolates official SDK types from Harness lifecycle code, allowing deterministic unit tests while the production adapter delegates lifecycle, command, and filesystem operations to `com.alibaba.opensandbox:sandbox:1.0.18`.

**Tech Stack:** Java 17, Maven, AgentScope Harness Sandbox SPI, OpenSandbox Java SDK 1.0.18, Jackson, JUnit 6, Mockito.

---

## File Map

**Build and publication wiring**

- Modify `agentscope-dependencies-bom/pom.xml`: manage OpenSandbox SDK version.
- Modify `agentscope-extensions/agentscope-extensions-sandbox/pom.xml`: aggregate the new module.
- Create `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/pom.xml`: module dependencies.
- Modify `agentscope-distribution/agentscope-bom/pom.xml`: publish the module in AgentScope BOM.
- Modify `agentscope-distribution/agentscope-all/pom.xml`: include the module as optional distribution dependency.

**Provider implementation**

- Create `OpenSandboxClientOptions.java`: endpoint, credentials, image, resources, and timeout configuration.
- Create `OpenSandboxEndpoint.java`: validated scheme/domain parsing.
- Create `OpenSandboxState.java`: serializable backend and recreation state.
- Create `OpenSandboxHarnessSandboxJacksonModule.java`: state subtype registration.
- Create `OpenSandboxSdk.java`: package-private test boundary around official SDK operations.
- Create `OfficialOpenSandboxSdk.java`: official SDK implementation and exception classification.
- Create `OpenSandboxClient.java`: Harness client create/resume/serialization behavior.
- Create `OpenSandbox.java`: lifecycle, command, snapshot, and native transfer behavior.
- Create `OpenSandboxFilesystemSpec.java`: public fluent Harness configuration.

**Tests and documentation**

- Create `OpenSandboxClientOptionsTest.java`.
- Create `OpenSandboxStateSerializationTest.java`.
- Create `OpenSandboxFilesystemSpecTest.java`.
- Create `OpenSandboxTest.java`.
- Create `OpenSandboxIntegrationTest.java`.
- Modify `docs/v2/zh/docs/harness/sandbox.md` and `docs/v2/en/docs/harness/sandbox.md`.
- Modify `docs/v2/zh/integration/overview.md` and `docs/v2/en/integration/overview.md`.

### Task 1: Wire The Maven Module And Managed Dependency

**Files:**
- Modify: `agentscope-dependencies-bom/pom.xml`
- Modify: `agentscope-extensions/agentscope-extensions-sandbox/pom.xml`
- Create: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/pom.xml`
- Modify: `agentscope-distribution/agentscope-bom/pom.xml`
- Modify: `agentscope-distribution/agentscope-all/pom.xml`

- [ ] **Step 1: Confirm the module is initially absent**

Run:

```powershell
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox help:evaluate -Dexpression=project.artifactId -q -DforceStdout
```

Expected: Maven reports that the selected project cannot be found.

- [ ] **Step 2: Add dependency management and module POM**

Add to `agentscope-dependencies-bom/pom.xml` properties:

```xml
<opensandbox.version>1.0.18</opensandbox.version>
```

Add to its dependency management:

```xml
<dependency>
    <groupId>com.alibaba.opensandbox</groupId>
    <artifactId>sandbox</artifactId>
    <version>${opensandbox.version}</version>
</dependency>
```

Create the module POM with:

```xml
<artifactId>agentscope-extensions-sandbox-opensandbox</artifactId>
<dependencies>
    <dependency>
        <groupId>io.agentscope</groupId>
        <artifactId>agentscope-harness</artifactId>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>com.alibaba.opensandbox</groupId>
        <artifactId>sandbox</artifactId>
    </dependency>
</dependencies>
```

Add `agentscope-extensions-sandbox-opensandbox` beside the four existing providers in the extension aggregator, AgentScope BOM, and `agentscope-all` optional dependencies.

- [ ] **Step 3: Verify Maven resolves the official SDK**

Run:

```powershell
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox -am dependency:tree -Dincludes=com.alibaba.opensandbox:sandbox -DskipTests
```

Expected: dependency tree contains `com.alibaba.opensandbox:sandbox:jar:1.0.18:compile`.

- [ ] **Step 4: Commit build wiring**

```powershell
git add agentscope-dependencies-bom/pom.xml agentscope-extensions/agentscope-extensions-sandbox/pom.xml agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/pom.xml agentscope-distribution/agentscope-bom/pom.xml agentscope-distribution/agentscope-all/pom.xml
git commit -m "build: add OpenSandbox extension module"
```

### Task 2: Implement Configuration, Endpoint Parsing, And Serializable State

**Files:**
- Create: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/src/main/java/io/agentscope/extensions/sandbox/opensandbox/OpenSandboxEndpoint.java`
- Create: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/src/main/java/io/agentscope/extensions/sandbox/opensandbox/OpenSandboxClientOptions.java`
- Create: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/src/main/java/io/agentscope/extensions/sandbox/opensandbox/OpenSandboxState.java`
- Create: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/src/main/java/io/agentscope/extensions/sandbox/opensandbox/OpenSandboxHarnessSandboxJacksonModule.java`
- Create: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/src/test/java/io/agentscope/extensions/sandbox/opensandbox/OpenSandboxClientOptionsTest.java`
- Create: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/src/test/java/io/agentscope/extensions/sandbox/opensandbox/OpenSandboxStateSerializationTest.java`

- [ ] **Step 1: Write endpoint and options tests**

Use assertions covering both accepted forms and invalid values:

```java
@Test
void endpointWithoutSchemeDefaultsToHttp() {
    OpenSandboxEndpoint endpoint = OpenSandboxEndpoint.parse("172.16.1.86:8090");
    assertEquals("http", endpoint.protocol());
    assertEquals("172.16.1.86:8090", endpoint.domain());
}

@Test
void endpointWithHttpsSeparatesProtocolAndDomain() {
    OpenSandboxEndpoint endpoint = OpenSandboxEndpoint.parse("https://sandbox.example.com/");
    assertEquals("https", endpoint.protocol());
    assertEquals("sandbox.example.com", endpoint.domain());
}

@Test
void invalidTimeoutIsRejected() {
    OpenSandboxClientOptions options = new OpenSandboxClientOptions();
    assertThrows(IllegalArgumentException.class, () -> options.setReadyTimeoutSeconds(0));
}
```

- [ ] **Step 2: Run tests and verify they fail**

```powershell
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox -am -Dtest=OpenSandboxClientOptionsTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: compilation fails because the OpenSandbox configuration classes do not exist.

- [ ] **Step 3: Implement endpoint and options**

Implement the endpoint value object with this contract:

```java
record OpenSandboxEndpoint(String protocol, String domain) {
    static OpenSandboxEndpoint parse(String value) {
        String raw = Objects.requireNonNull(value, "endpoint must not be null").trim();
        URI uri = URI.create(raw.contains("://") ? raw : "http://" + raw);
        if (!("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
                || uri.getHost() == null || uri.getPath() != null && !uri.getPath().matches("/?")) {
            throw new IllegalArgumentException("Invalid OpenSandbox endpoint: " + value);
        }
        String domain = uri.getHost() + (uri.getPort() >= 0 ? ":" + uri.getPort() : "");
        return new OpenSandboxEndpoint(uri.getScheme().toLowerCase(Locale.ROOT), domain);
    }
}
```

Implement `OpenSandboxClientOptions` with type `opensandbox`, defaults `http://localhost:8080`, `ubuntu:22.04`, `List.of("tail", "-f", "/dev/null")`, 600-second sandbox TTL, 30-second ready timeout, 30-second request timeout, resource limits `cpu=1` and `memory=2Gi`, and `useServerProxy=false`. Setters reject blank endpoint/image and non-positive timeouts.

- [ ] **Step 4: Write state serialization tests**

```java
@Test
void stateRoundTripPreservesRecreationFieldsWithoutCredentials() {
    OpenSandboxClientOptions defaults = new OpenSandboxClientOptions();
    defaults.setApiKey("secret-not-for-json");
    OpenSandboxClient client = new OpenSandboxClient(defaults, null);
    OpenSandboxState state = new OpenSandboxState();
    state.setSessionId("session-1");
    state.setSandboxId("sandbox-1");
    state.setSandboxOwned(true);
    state.setImage("ubuntu:22.04");
    state.setEntrypoint(List.of("tail", "-f", "/dev/null"));
    state.setResourceLimits(Map.of("cpu", "2", "memory", "4Gi"));
    state.setSandboxTimeoutSeconds(900);
    state.setWorkspaceSpec(workspace("/workspace"));

    String json = client.serializeState(state);
    OpenSandboxState restored = (OpenSandboxState) client.deserializeState(json);

    assertEquals("sandbox-1", restored.getSandboxId());
    assertEquals(Map.of("cpu", "2", "memory", "4Gi"), restored.getResourceLimits());
    assertFalse(json.contains("secret-not-for-json"));
}
```

- [ ] **Step 5: Implement state and Jackson module**

`OpenSandboxState` extends `SandboxState` with mutable JavaBean properties for sandbox ID, ownership, image, entrypoint, resource limits, and TTL. Register it under subtype name `opensandbox`:

```java
public final class OpenSandboxHarnessSandboxJacksonModule extends SimpleModule {
    public OpenSandboxHarnessSandboxJacksonModule() {
        super("OpenSandboxHarnessSandboxJacksonModule");
        registerSubtypes(new NamedType(OpenSandboxState.class, "opensandbox"));
    }
}
```

Add the minimal `OpenSandboxClient` serialization constructor needed by the test, registering `HarnessSandboxJacksonModule` and `OpenSandboxHarnessSandboxJacksonModule`.

- [ ] **Step 6: Run tests and commit**

```powershell
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox -am -Dtest=OpenSandboxClientOptionsTest,OpenSandboxStateSerializationTest -Dsurefire.failIfNoSpecifiedTests=false test
git add agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox
git commit -m "feat: add OpenSandbox configuration and state"
```

Expected: both test classes pass.

### Task 3: Add The Official SDK Boundary And Harness Client

**Files:**
- Create: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/src/main/java/io/agentscope/extensions/sandbox/opensandbox/OpenSandboxSdk.java`
- Create: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/src/main/java/io/agentscope/extensions/sandbox/opensandbox/OfficialOpenSandboxSdk.java`
- Complete: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/src/main/java/io/agentscope/extensions/sandbox/opensandbox/OpenSandboxClient.java`
- Create: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/src/test/java/io/agentscope/extensions/sandbox/opensandbox/OpenSandboxClientTest.java`

- [ ] **Step 1: Write client create/resume tests with a fake SDK**

```java
@Test
void createCopiesMergedCreationSettingsIntoState() {
    OpenSandboxClientOptions defaults = new OpenSandboxClientOptions();
    defaults.setImage("ubuntu:24.04");
    defaults.setResourceLimits(Map.of("cpu", "2", "memory", "4Gi"));
    OpenSandboxClient client = new OpenSandboxClient(defaults, null, fakeSdk);

    OpenSandbox sandbox = (OpenSandbox) client.create(workspace("/workspace"), null, null);
    OpenSandboxState state = (OpenSandboxState) sandbox.getState();

    assertEquals("ubuntu:24.04", state.getImage());
    assertEquals(Map.of("cpu", "2", "memory", "4Gi"), state.getResourceLimits());
    assertTrue(state.isSandboxOwned());
}

@Test
void resumeRejectsAnotherProviderState() {
    assertThrows(IllegalArgumentException.class, () -> client.resume(new DaytonaSandboxState()));
}
```

- [ ] **Step 2: Define the package-private SDK boundary**

```java
interface OpenSandboxSdk {
    Handle create(OpenSandboxState state, OpenSandboxClientOptions options) throws Exception;
    Handle connect(String sandboxId, OpenSandboxClientOptions options) throws Exception;
    void kill(String sandboxId, OpenSandboxClientOptions options) throws Exception;
    boolean isNotFound(Throwable error);

    interface Handle extends AutoCloseable {
        String id();
        ExecResult exec(String command, String workingDirectory, int timeoutSeconds) throws Exception;
        InputStream read(String absolutePath) throws Exception;
        void write(String absolutePath, byte[] content) throws Exception;
    }
}
```

- [ ] **Step 3: Implement official SDK delegation**

Build `ConnectionConfig` from the parsed endpoint and options:

```java
ConnectionConfig config = ConnectionConfig.builder()
        .domain(endpoint.domain())
        .protocol(endpoint.protocol())
        .apiKey(options.getApiKey())
        .requestTimeout(Duration.ofSeconds(options.getRequestTimeoutSeconds()))
        .useServerProxy(options.isUseServerProxy())
        .build();
```

Create with the persisted creation fields:

```java
Sandbox.builder()
        .image(state.getImage())
        .entrypoint(state.getEntrypoint())
        .resource(state.getResourceLimits())
        .timeout(Duration.ofSeconds(state.getSandboxTimeoutSeconds()))
        .readyTimeout(Duration.ofSeconds(options.getReadyTimeoutSeconds()))
        .connectionConfig(config)
        .build();
```

Connect with `Sandbox.connector().sandboxId(sandboxId).connectionConfig(config).connectTimeout(Duration.ofSeconds(options.getReadyTimeoutSeconds())).connect()`, and kill with `SandboxManager.builder().connectionConfig(config).build().killSandbox(sandboxId)`.

Map `Execution` into Harness `ExecResult` by joining `execution.getLogs().getStdout()/getStderr()` message text. Build `RunCommandRequest` with working directory and timeout. Map files through `files().readStream(path)` and `files().writeFile(WriteEntry.builder().path(path).data(content).build())`. `isNotFound` walks the cause chain and returns true only for `SandboxApiException.getStatusCode() == 404`.

- [ ] **Step 4: Complete `OpenSandboxClient`**

Implement `create`, `resume`, `delete`, option merge/copy, and snapshot construction following `DaytonaSandboxClient`. Ensure only creation settings enter `OpenSandboxState`; endpoint and API key remain in merged client options.

- [ ] **Step 5: Run tests and commit**

```powershell
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox -am -Dtest=OpenSandboxClientTest,OpenSandboxStateSerializationTest -Dsurefire.failIfNoSpecifiedTests=false test
git add agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox
git commit -m "feat: add OpenSandbox SDK client adapter"
```

Expected: client and serialization tests pass.

### Task 4: Implement Lifecycle, Execution, Snapshots, And Native File Transfer

**Files:**
- Create: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/src/main/java/io/agentscope/extensions/sandbox/opensandbox/OpenSandbox.java`
- Create: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/src/test/java/io/agentscope/extensions/sandbox/opensandbox/OpenSandboxTest.java`

- [ ] **Step 1: Write lifecycle tests against a recording fake SDK**

Cover these exact cases:

```java
@Test
void startCreatesWhenStateHasNoId() throws Exception {
    sandbox.start();
    assertEquals(1, sdk.createCalls);
    assertEquals("created-id", state.getSandboxId());
}

@Test
void startRecreatesOnlyAfterExplicitNotFound() throws Exception {
    state.setSandboxId("gone");
    sdk.connectFailure = sdk.notFound();
    sandbox.start();
    assertEquals(1, sdk.connectCalls);
    assertEquals(1, sdk.createCalls);
}

@Test
void startDoesNotRecreateAfterConnectionFailure() {
    state.setSandboxId("temporarily-unreachable");
    sdk.connectFailure = new IOException("timeout");
    assertThrows(Exception.class, sandbox::start);
    assertEquals(0, sdk.createCalls);
}

@Test
void shutdownKillsOwnedSandboxByIdAfterHandleWasClosed() throws Exception {
    sandbox.start();
    sandbox.stop();
    sandbox.shutdown();
    assertEquals(List.of("created-id"), sdk.killedIds);
}
```

- [ ] **Step 2: Run lifecycle tests and verify failure**

```powershell
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox -am -Dtest=OpenSandboxTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: compilation fails because `OpenSandbox` is incomplete or absent.

- [ ] **Step 3: Implement lifecycle and command behavior**

`start()` calls `ensureSandbox()` before `super.start()`. `ensureSandbox()` creates when the ID is blank, connects otherwise, and only catches `sdk.isNotFound(error)` for recreation. On recreation set `workspaceRootReady=false` before assigning the new ID.

`doExec` delegates to the handle and preserves existing Provider semantics:

```java
ExecResult result = handle().exec(command, getWorkspaceRoot(), Math.max(1, timeoutSeconds));
if (!result.ok()) {
    throw new SandboxException.ExecException(
            result.exitCode(), result.stdout(), result.stderr());
}
return result;
```

`stop()` calls `super.stop()` and closes/nulls the handle in `finally`. `shutdown()` calls `sdk.kill(state.getSandboxId(), options)` only when the sandbox is owned, then closes any remaining handle.

- [ ] **Step 4: Add snapshot and file transfer tests**

```java
@Test
void nativeFileTransferCreatesParentAndPreservesBinaryBytes() throws Exception {
    sandbox.start();
    byte[] bytes = new byte[] {0, 1, 2, (byte) 255};
    sandbox.uploadFile("/workspace/nested/data.bin", bytes);
    assertArrayEquals(bytes, sandbox.downloadFile("/workspace/nested/data.bin"));
}

@Test
void hydrateUploadsTarExtractsAndCleansTemporaryFile() throws Exception {
    sandbox.start();
    sandbox.hydrateWorkspace(new ByteArrayInputStream(new byte[] {1, 2, 3}));
    assertTrue(sdk.handle.commands.stream().anyMatch(c -> c.contains("tar -xf")));
    assertTrue(sdk.handle.commands.stream().anyMatch(c -> c.contains("rm -f")));
}
```

- [ ] **Step 5: Implement snapshots and native transfer**

Implement `SandboxFileTransfer` for all normalized absolute paths. `uploadFile` runs `mkdir -p` for the parent before `handle.write`; `downloadFile` reads all bytes from `handle.read`.

Persist using a unique `/tmp/agentscope-<session-hash>-persist.tar`, command `tar -cf <tmp> -C <root> .`, and `handle.read`. Return a `ByteArrayInputStream` so the remote temp file can be deleted before returning. Hydrate by reading archive bytes, writing a unique temp tar, running `mkdir -p <root> && tar -xf <tmp> -C <root>`, and deleting the temp file in `finally`. Use the same POSIX single-quote helper as E2B/Daytona.

- [ ] **Step 6: Run tests and commit**

```powershell
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox -am -Dtest=OpenSandboxTest -Dsurefire.failIfNoSpecifiedTests=false test
git add agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox
git commit -m "feat: implement OpenSandbox lifecycle and filesystem"
```

Expected: all `OpenSandboxTest` cases pass.

### Task 5: Add The Public Filesystem Spec And Documentation

**Files:**
- Create: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/src/main/java/io/agentscope/extensions/sandbox/opensandbox/OpenSandboxFilesystemSpec.java`
- Create: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/src/test/java/io/agentscope/extensions/sandbox/opensandbox/OpenSandboxFilesystemSpecTest.java`
- Modify: `docs/v2/zh/docs/harness/sandbox.md`
- Modify: `docs/v2/en/docs/harness/sandbox.md`
- Modify: `docs/v2/zh/integration/overview.md`
- Modify: `docs/v2/en/integration/overview.md`

- [ ] **Step 1: Write the public spec test**

```java
@Test
void defaultsToWorkspaceRootAndCreatesOpenSandboxClient() {
    OpenSandboxFilesystemSpec spec = new OpenSandboxFilesystemSpec();
    assertEquals("/workspace", spec.workspaceSpec().getRoot());
    assertInstanceOf(OpenSandboxClient.class, spec.createClient());
}

@Test
void fluentConfigurationReturnsSameSpec() {
    OpenSandboxFilesystemSpec spec = new OpenSandboxFilesystemSpec();
    assertSame(spec, spec.endpoint("http://localhost:8080"));
    assertSame(spec, spec.image("ubuntu:24.04"));
    assertSame(spec, spec.cpu("2"));
    assertSame(spec, spec.memory("4Gi"));
}
```

- [ ] **Step 2: Implement `OpenSandboxFilesystemSpec`**

Follow `DaytonaFilesystemSpec`: hold an optional injected client, one options object, a `NoopSnapshotSpec`, and default `/workspace` `WorkspaceSpec`. Expose fluent methods for endpoint, API key, image, entrypoint, CPU, memory, sandbox/ready/request timeouts, server proxy, workspace root/spec, and snapshot spec. Override `createClient`, `clientOptions`, `snapshotSpec`, and `workspaceSpec`.

- [ ] **Step 3: Document dependency and configuration**

Add OpenSandbox to both Harness provider tables and integration overviews. Include this minimal Java example in both language variants:

```java
new OpenSandboxFilesystemSpec()
        .endpoint("http://localhost:8080")
        .apiKey(System.getenv("OPEN_SANDBOX_API_KEY"))
        .image("ubuntu:22.04")
        .workspaceRoot("/workspace");
```

State that the API key should come from configuration/environment and that MCP processes remain host-side.

- [ ] **Step 4: Run tests and commit**

```powershell
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox -am -Dtest=OpenSandboxFilesystemSpecTest -Dsurefire.failIfNoSpecifiedTests=false test
git add agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox docs/v2/zh/docs/harness/sandbox.md docs/v2/en/docs/harness/sandbox.md docs/v2/zh/integration/overview.md docs/v2/en/integration/overview.md
git commit -m "docs: document OpenSandbox sandbox provider"
```

### Task 6: Verify Against The Real Service And Install The Repository

**Files:**
- Create: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/src/test/java/io/agentscope/extensions/sandbox/opensandbox/OpenSandboxIntegrationTest.java`

- [ ] **Step 1: Add opt-in integration test**

Guard the test with environment assumptions and never print credentials:

```java
@Test
void realServiceCreateExecTransferReconnectAndKill() throws Exception {
    String endpoint = System.getenv("OPEN_SANDBOX_ENDPOINT");
    String apiKey = System.getenv("OPEN_SANDBOX_API_KEY");
    Assumptions.assumeTrue(endpoint != null && apiKey != null);

    OpenSandboxClientOptions options = new OpenSandboxClientOptions();
    options.setEndpoint(endpoint);
    options.setApiKey(apiKey);
    options.setImage(System.getenv().getOrDefault("OPEN_SANDBOX_TEST_IMAGE", "ubuntu:22.04"));
    OpenSandboxClient client = new OpenSandboxClient(options, null);
    OpenSandbox sandbox = (OpenSandbox) client.create(workspace("/workspace"), null, null);
    try {
        sandbox.start();
        assertEquals("opensandbox-ok\n", sandbox.exec(null, "printf 'opensandbox-ok\\n'", 30).stdout());
        byte[] payload = new byte[] {0, 1, 2, (byte) 255};
        sandbox.uploadFile("/workspace/data.bin", payload);
        assertArrayEquals(payload, sandbox.downloadFile("/workspace/data.bin"));

        OpenSandboxState saved = (OpenSandboxState) sandbox.getState();
        OpenSandbox resumed = (OpenSandbox) client.resume(saved);
        resumed.start();
        assertArrayEquals(payload, resumed.downloadFile("/workspace/data.bin"));
        resumed.stop();
    } finally {
        sandbox.shutdown();
    }
}
```

- [ ] **Step 2: Run all module tests**

```powershell
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox -am test
```

Expected: unit tests pass; integration test is skipped without environment variables.

- [ ] **Step 3: Run the real-service integration test**

Set the provided endpoint and token only in the process environment, then run:

```powershell
$env:OPEN_SANDBOX_ENDPOINT='http://172.16.1.86:8090'
# OPEN_SANDBOX_API_KEY is injected into this process without writing it to disk.
$env:OPEN_SANDBOX_TEST_IMAGE='ubuntu:22.04'
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox -am -Dtest=OpenSandboxIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: one sandbox is created, command and binary transfer assertions pass, the same ID reconnects while live, and cleanup removes the sandbox. Confirm `GET /v1/sandboxes` shows no test-owned instance afterward.

- [ ] **Step 4: Package and install**

```powershell
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox -am package
mvn install
```

Expected: reactor summary is `BUILD SUCCESS` for both commands.

- [ ] **Step 5: Check secrets and worktree, then commit**

```powershell
rg -n "aide-token|172\.16\.1\.86" agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox docs/v2
git diff --check
git status --short
git add agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox
git commit -m "test: verify OpenSandbox provider integration"
```

Expected: secret scan returns no token; only intended implementation, test, plan, and pre-existing untracked files remain.
