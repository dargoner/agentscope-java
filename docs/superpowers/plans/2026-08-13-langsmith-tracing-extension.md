# LangSmith Tracing Extension Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional LangSmith tracing handler that enriches AgentScope's standard OpenTelemetry spans with LangSmith run kind, tags, and metadata while leaving OTLP transport and SDK ownership to the application.

**Architecture:** A new leaf Maven module depends on the public tracing lifecycle API delivered by the core plan. `LangSmithTracingHandler` only calls OpenTelemetry `Span.setAttribute`; it has no network client, exporter, SDK registration, or LangSmith Java SDK. Static configuration is validated and copied at build time, while per-call metadata is derived safely from `OtelTracingContext`.

**Tech Stack:** Java 17, AgentScope core tracing API, OpenTelemetry API, JUnit 5, OpenTelemetry SDK testing exporter, Maven, Spotless, JaCoCo.

## Global Constraints

- Execute only after the core tracing extension plan is complete and its public API is available.
- Do not depend on the LangSmith Java SDK or an OpenTelemetry exporter.
- Do not create, register, replace, shut down, or otherwise mutate `GlobalOpenTelemetry`.
- Do not automatically set `langsmith.trace.session_id` or `langsmith.trace.session_name`.
- AgentScope session IDs map only to `langsmith.metadata.session_id` by default.
- Operation mapping is exactly `AGENT -> chain`, `MODEL -> llm`, `TOOL -> tool`.
- Metadata precedence is safe defaults, then static metadata, then provider metadata.
- Tags and metadata are attached to every supported AgentScope span.
- Static blank keys/tags and null values fail fast; invalid dynamic entries are ignored.
- Java 17 and the repository Spotless AOSP Google Java Format configuration are authoritative.
- Every new Java file needs the repository Apache 2.0 header; all public APIs require useful Javadoc that passes doclint.
- `LangSmithTracingHandler` requires 100% line coverage and at least 90% branch coverage.
- Tests must not send network traffic and must prove the global OpenTelemetry instance is unchanged.

## File Map

- `agentscope-extensions/pom.xml`: registers the new optional extension module.
- `agentscope-extensions/agentscope-extensions-langsmith/pom.xml`: leaf module metadata and provided/optional core dependency.
- `agentscope-distribution/agentscope-bom/pom.xml`: publishes the extension version through the AgentScope BOM.
- `agentscope-distribution/agentscope-all/pom.xml`: includes the extension as an optional aggregate dependency.
- `agentscope-extensions/agentscope-extensions-langsmith/src/main/java/io/agentscope/extensions/langsmith/LangSmithMetadataProvider.java`: public per-call metadata functional interface.
- `agentscope-extensions/agentscope-extensions-langsmith/src/main/java/io/agentscope/extensions/langsmith/LangSmithTracingHandler.java`: immutable builder and span enrichment logic.
- `agentscope-extensions/agentscope-extensions-langsmith/src/test/java/io/agentscope/extensions/langsmith/LangSmithTracingHandlerTest.java`: mapping, validation, precedence, null/failure, global SDK, and integration coverage.
- `docs/v2/en/docs/building-blocks/middleware.md`: English LangSmith OTLP setup and privacy guidance.
- `docs/v2/zh/docs/building-blocks/middleware.md`: Chinese LangSmith OTLP setup and privacy guidance.

---

### Task 1: Extension module and deterministic LangSmith attributes

**Files:**
- Modify: `agentscope-extensions/pom.xml`
- Create: `agentscope-extensions/agentscope-extensions-langsmith/pom.xml`
- Modify: `agentscope-distribution/agentscope-bom/pom.xml`
- Modify: `agentscope-distribution/agentscope-all/pom.xml`
- Create: `agentscope-extensions/agentscope-extensions-langsmith/src/main/java/io/agentscope/extensions/langsmith/LangSmithMetadataProvider.java`
- Create: `agentscope-extensions/agentscope-extensions-langsmith/src/main/java/io/agentscope/extensions/langsmith/LangSmithTracingHandler.java`
- Test: `agentscope-extensions/agentscope-extensions-langsmith/src/test/java/io/agentscope/extensions/langsmith/LangSmithTracingHandlerTest.java`

**Interfaces:**
- Consumes: `OtelTracingHandler`, `OtelTracingContext`, `OtelTracingOperation`, `RuntimeContext`, `Agent`, and `Span`.
- Produces: `LangSmithMetadataProvider.provide(OtelTracingContext)` and `LangSmithTracingHandler.builder()`.

- [ ] **Step 1: Add the empty leaf module to the reactor**

Add `<module>agentscope-extensions-langsmith</module>` immediately after the Studio extension in
`agentscope-extensions/pom.xml`. Create a leaf POM following the Studio module's repository metadata
but with only this main dependency:

```xml
<dependency>
    <groupId>io.agentscope</groupId>
    <artifactId>agentscope-core</artifactId>
    <optional>true</optional>
    <scope>provided</scope>
</dependency>
```

The artifact ID is `agentscope-extensions-langsmith`, name is
`AgentScope Java - Extensions - LangSmith`, and description is
`LangSmith span enrichment for AgentScope OpenTelemetry tracing`. Add `opentelemetry-api` with
`provided` scope because the handler calls `Span.setAttribute`. Add `opentelemetry-sdk` and
`opentelemetry-sdk-testing` with `test` scope for the in-memory integration tests. Do not add
LangSmith, OTel exporter, or HTTP client dependencies.

Add `io.agentscope:agentscope-extensions-langsmith:${project.version}` immediately after Studio in
`agentscope-distribution/agentscope-bom/pom.xml`. Add the same artifact immediately after Studio in
`agentscope-distribution/agentscope-all/pom.xml` with `<scope>compile</scope>` and
`<optional>true</optional>`.

- [ ] **Step 2: Write failing operation and safe-default metadata tests**

Set up `InMemorySpanExporter`, `SdkTracerProvider`, and `OpenTelemetrySdk` in the test, but construct
the handler directly and call it against operation-specific `OtelTracingContext` objects. Because
the context factories are package-private, obtain contexts by installing the handler on
`OtelTracingMiddleware` and invoking agent/model/tool publishers. Assert:

```java
assertEquals("chain", stringAttribute(agentSpan, "langsmith.span.kind"));
assertEquals("llm", stringAttribute(modelSpan, "langsmith.span.kind"));
assertEquals("tool", stringAttribute(toolSpan, "langsmith.span.kind"));
assertEquals("user-1", stringAttribute(agentSpan, "langsmith.metadata.user_id"));
assertEquals("session-1", stringAttribute(agentSpan, "langsmith.metadata.session_id"));
assertEquals("agent-1", stringAttribute(agentSpan, "langsmith.metadata.agent_id"));
assertEquals("assistant", stringAttribute(agentSpan, "langsmith.metadata.agent_name"));
assertNull(stringAttribute(agentSpan, "langsmith.trace.session_id"));
assertNull(stringAttribute(agentSpan, "langsmith.trace.session_name"));
```

Use a runtime context built with `.userId("user-1").sessionId("session-1")`. Copy the minimal
`StubAgent` and `StubModel` implementations from `OtelTracingMiddlewareTest` into this test class,
then invoke `onAgent`, `onModelCall`, and `onActing` with empty publishers and a single
`ToolUseBlock(id="call-1", name="search", input={})`.

- [ ] **Step 3: Run the module test and verify expected failure**

```powershell
mvn -pl agentscope-extensions/agentscope-extensions-langsmith -am -Dtest=LangSmithTracingHandlerTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: test compilation fails because the handler/provider do not exist.

- [ ] **Step 4: Implement the provider and initial handler**

Define:

```java
@FunctionalInterface
public interface LangSmithMetadataProvider {
    Map<String, String> provide(OtelTracingContext context);
}
```

Create `public final class LangSmithTracingHandler implements OtelTracingHandler` with a private
constructor and this builder surface:

```java
public static Builder builder();

public static final class Builder {
    public Builder tags(List<String> tags);
    public Builder metadata(Map<String, String> metadata);
    public Builder metadataProvider(LangSmithMetadataProvider metadataProvider);
    public LangSmithTracingHandler build();
}
```

For this step, implement `onStart` to set `langsmith.span.kind` from a private exhaustive switch and
safe defaults only when non-null/non-blank. Use literal attribute names in private constants. The
handler's `supports` returns true for `AGENT`, `MODEL`, and `TOOL` and false defensively for an
unexpected null operation, although valid contexts never contain one.

- [ ] **Step 5: Run mapping tests and Spotless**

```powershell
mvn -pl agentscope-extensions/agentscope-extensions-langsmith -am -Dtest=LangSmithTracingHandlerTest -Dsurefire.failIfNoSpecifiedTests=false test
mvn -pl agentscope-extensions/agentscope-extensions-langsmith spotless:check
```

Expected: module compiles and deterministic mapping tests pass.

- [ ] **Step 6: Commit the module and base mapping**

```powershell
git add agentscope-extensions/pom.xml agentscope-extensions/agentscope-extensions-langsmith agentscope-distribution/agentscope-bom/pom.xml agentscope-distribution/agentscope-all/pom.xml
git commit -m "feat: add LangSmith tracing extension"
```

---

### Task 2: Immutable tags, metadata precedence, and runtime failure safety

**Files:**
- Modify: `agentscope-extensions/agentscope-extensions-langsmith/src/main/java/io/agentscope/extensions/langsmith/LangSmithTracingHandler.java`
- Modify: `agentscope-extensions/agentscope-extensions-langsmith/src/test/java/io/agentscope/extensions/langsmith/LangSmithTracingHandlerTest.java`

**Interfaces:**
- Consumes: builder API and provider from Task 1.
- Produces: validated immutable configuration and deterministic enrichment of every supported span.

- [ ] **Step 1: Write failing builder validation and defensive-copy tests**

Add exact assertions for null collections/provider, null elements/values, and blank values:

```java
assertThrows(NullPointerException.class, () -> builder.tags(null));
assertThrows(NullPointerException.class, () -> builder.metadata(null));
assertThrows(NullPointerException.class, () -> builder.metadataProvider(null));
assertThrows(IllegalArgumentException.class, () -> builder.tags(List.of(" ")));
assertThrows(IllegalArgumentException.class, () -> builder.metadata(Map.of(" ", "value")));
```

Use mutable `ArrayList`/`LinkedHashMap`, build the handler, mutate the originals, invoke a span, and
assert only the pre-build values are present. For null elements/values, use collections that permit
null rather than `List.of`/`Map.of`.

- [ ] **Step 2: Write failing tags and precedence tests**

Build with tags `production`, `agentscope`; static metadata `service=customer-support` and
`agent_name=static-name`; provider metadata `tenant_id=tenant-1` and `agent_name=dynamic-name`.
Assert every agent/model/tool span contains:

```java
assertEquals(List.of("production", "agentscope"),
        stringArrayAttribute(span, "langsmith.span.tags"));
assertEquals("customer-support",
        stringAttribute(span, "langsmith.metadata.service"));
assertEquals("tenant-1",
        stringAttribute(span, "langsmith.metadata.tenant_id"));
assertEquals("dynamic-name",
        stringAttribute(span, "langsmith.metadata.agent_name"));
```

This proves provider metadata overrides static metadata, which overrides the safe default.

- [ ] **Step 3: Write failing null-runtime and dynamic-invalid-entry tests**

Invoke with a null `RuntimeContext` and assert kind plus agent defaults still appear without an
exception. Return a mutable provider map containing a blank key and null key/value; assert valid
entries are set and invalid entries are ignored. Return `null` from a provider and assert that is
treated as no dynamic metadata.

- [ ] **Step 4: Run tests and observe missing behavior**

```powershell
mvn -pl agentscope-extensions/agentscope-extensions-langsmith -am -Dtest=LangSmithTracingHandlerTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: tag, precedence, and validation assertions fail.

- [ ] **Step 5: Implement validation, copies, tags, and metadata merge**

At builder method call time:

- validate collections/provider with `Objects.requireNonNull`;
- copy tags into an `ArrayList`, rejecting null/blank values, then store `List.copyOf`;
- copy metadata into a `LinkedHashMap`, rejecting null/blank keys and null values, then store an
  unmodifiable insertion-ordered copy;
- use an empty provider returning `Map.of()` by default.

In `onStart`, write string-array `langsmith.span.tags` when non-empty. Build and write one ordered
base metadata map from safe defaults followed by static values. Then call the provider and write its
valid entries using the same keys so they override the base attributes. This ordering preserves safe
and static metadata if the provider throws. Ignore invalid dynamic entries. Set each entry using
`"langsmith.metadata." + key`. Do not swallow provider exceptions inside the handler; the core
dispatcher must isolate them and continue with subsequent handlers.

- [ ] **Step 6: Add provider-exception integration proof**

Install a LangSmith handler whose provider throws `IllegalStateException("provider failed")`,
followed by a recording `OtelTracingHandler`. Invoke an agent call and assert the call completes,
the span ends, and the recording handler receives `onStart` and `onStop`. This validates extension
behavior against the real core dispatcher rather than a handler-local catch.

- [ ] **Step 7: Run module tests and commit enrichment**

```powershell
mvn -pl agentscope-extensions/agentscope-extensions-langsmith -am -Dtest=LangSmithTracingHandlerTest -Dsurefire.failIfNoSpecifiedTests=false test
mvn -pl agentscope-extensions/agentscope-extensions-langsmith spotless:check
git add agentscope-extensions/agentscope-extensions-langsmith/src
git commit -m "feat: enrich LangSmith spans with metadata"
```

Expected: all mapping, validation, ordering, null, and failure tests pass before the commit.

---

### Task 3: Prove transport neutrality and document LangSmith OTLP setup

**Files:**
- Modify: `agentscope-extensions/agentscope-extensions-langsmith/src/test/java/io/agentscope/extensions/langsmith/LangSmithTracingHandlerTest.java`
- Modify: `docs/v2/en/docs/building-blocks/middleware.md`
- Modify: `docs/v2/zh/docs/building-blocks/middleware.md`

**Interfaces:**
- Consumes: completed `LangSmithTracingHandler` and core content options.
- Produces: no-network/global-SDK proof, application configuration examples, privacy guidance, and extension coverage evidence.

- [ ] **Step 1: Add no-global-mutation and no-network tests**

Register a test `OpenTelemetrySdk`, capture `GlobalOpenTelemetry.get()` before constructing and using
the handler, and assert `assertSame(before, GlobalOpenTelemetry.get())` afterward. The module POM
must have no exporter or HTTP client dependency. The only spans observed are those sent to the local
`InMemorySpanExporter`; do not mock or open sockets.

- [ ] **Step 2: Document dependency and standard OTLP environment configuration**

Add matching English and Chinese LangSmith subsections. Show:

```xml
<dependency>
    <groupId>io.agentscope</groupId>
    <artifactId>agentscope-extensions-langsmith</artifactId>
    <version>${agentscope.version}</version>
</dependency>
```

and:

```text
OTEL_EXPORTER_OTLP_ENDPOINT=https://api.smith.langchain.com/otel
OTEL_EXPORTER_OTLP_HEADERS=x-api-key=<key>,Langsmith-Project=<project>
OTEL_SERVICE_NAME=<service>
```

Show middleware construction:

```java
OtelTracingMiddleware tracing =
        OtelTracingMiddleware.builder()
                .options(
                        OtelTracingOptions.builder()
                                .includeInputMessages(true)
                                .includeOutputMessages(true)
                                .includeToolCallContent(false)
                                .build())
                .handler(
                        LangSmithTracingHandler.builder()
                                .tags(List.of("production", "agentscope"))
                                .metadata(Map.of("service", "customer-support"))
                                .metadataProvider(context -> {
                                    RuntimeContext runtime = context.getRuntimeContext();
                                    String tenant =
                                            runtime != null
                                                    ? runtime.get("tenantId", String.class)
                                                    : null;
                                    return tenant != null
                                            ? Map.of("tenant_id", tenant)
                                            : Map.of();
                                })
                                .build())
                .build();
```

State that OTLP export, batching, sampling, resources, collectors, fan-out, and shutdown remain
application-owned. State that all content options are default-off and can contain sensitive data.
Explain why AgentScope session IDs are metadata and how applications can add explicit LangSmith trace
session fields with their own generic handler if routing is intended.

- [ ] **Step 3: Run module packaging, tests, and Spotless**

```powershell
mvn -pl agentscope-extensions/agentscope-extensions-langsmith -am clean verify
```

Expected: the new module plus required upstream modules build successfully, all tests pass, Javadocs
package, and Spotless reports no violations.

- [ ] **Step 4: Extract handler coverage**

Run:

```powershell
[xml]$report = Get-Content agentscope-extensions/agentscope-extensions-langsmith/target/site/jacoco/jacoco.xml
$class = $report.report.package.class |
    Where-Object { $_.name -eq 'io/agentscope/extensions/langsmith/LangSmithTracingHandler' }
$class.counter | Where-Object { $_.type -in @('LINE','BRANCH') } | ForEach-Object {
    $total = [double]$_.covered + [double]$_.missed
    $pct = if ($total -eq 0) { 100 } else { [math]::Round(100 * [double]$_.covered / $total, 1) }
    "{0}: {1}% ({2} covered, {3} missed)" -f $_.type,$pct,$_.covered,$_.missed
}
```

Expected: `LINE: 100%` and `BRANCH` at least 90%. For a counter below its gate, open
`agentscope-extensions/agentscope-extensions-langsmith/target/site/jacoco/io.agentscope.extensions.langsmith/LangSmithTracingHandler.html`,
identify the red or yellow condition, add one test named after that condition to
`LangSmithTracingHandlerTest`, rerun clean verify, and repeat the extraction.

- [ ] **Step 5: Commit documentation and transport-neutrality tests**

```powershell
git add agentscope-extensions/agentscope-extensions-langsmith/src/test/java/io/agentscope/extensions/langsmith/LangSmithTracingHandlerTest.java docs/v2/en/docs/building-blocks/middleware.md docs/v2/zh/docs/building-blocks/middleware.md
git commit -m "docs: add LangSmith OpenTelemetry setup"
```

- [ ] **Step 6: Run final two-PR boundary verification**

```powershell
mvn -pl agentscope-core,agentscope-extensions/agentscope-extensions-langsmith -am clean verify
git diff --check
git status --short
```

Expected: all affected reactor modules succeed. Review `git log` and confirm the core commits can be
submitted as the first PR while all LangSmith-module/docs commits form a second PR that depends only
on the public core tracing API. Existing untracked `.codegraph/` content remains uncommitted.
