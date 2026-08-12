# LangSmith Tracing Extension Design

## Status

Reviewed direction: extend AgentScope Java tracing through a vendor-neutral OpenTelemetry
lifecycle API, then provide LangSmith support as an optional extension. The review covers public
API compatibility, repository conventions, failure semantics, and measurable test coverage.

## Context

`OtelTracingMiddleware` currently creates nested spans for agent, model, and tool operations and
propagates OpenTelemetry context through Reactor. It exposes no supported way to enrich those spans
with backend-specific attributes or observe their event lifecycle. It also records structural
attributes only; prompt, completion, tool arguments, and tool results are not exported.

LangSmith accepts standard OTLP traces and maps standard GenAI attributes such as
`gen_ai.input.messages`, `gen_ai.output.messages`, `gen_ai.tool.call.arguments`, and
`gen_ai.tool.call.result`. It also supports optional `langsmith.*` attributes for run type, tags,
and metadata.

Spring AI and Spring AI Alibaba treat prompt, completion, tool arguments, and tool results as
potentially sensitive, high-cardinality data. They do not export that content by default and
require explicit opt-in. AgentScope will follow the same policy.

## Goals

- Preserve the existing no-argument `OtelTracingMiddleware` behavior and public API.
- Add a vendor-neutral lifecycle extension point for spans created by the middleware.
- Export standard GenAI input/output attributes only when explicitly enabled.
- Provide a small optional LangSmith adapter without coupling `agentscope-core` to LangSmith.
- Keep OpenTelemetry SDK creation, global registration, sampling, resources, and shutdown under
  application ownership.
- Keep handler and serialization failures from affecting agent execution.
- Make the work separable into a core PR and a LangSmith extension PR.

## Non-goals

- Do not depend on the LangSmith Java SDK.
- Do not introduce a second tracing middleware that duplicates span creation.
- Do not register or replace `GlobalOpenTelemetry` from the LangSmith extension.
- Do not automatically route traces to LangSmith projects or evaluation experiments.
- Do not export prompt, completion, or tool content by default.
- Do not upload binary or base64 media payloads as span attributes.

## Architecture

The implementation has two layers:

1. `agentscope-core` owns span creation, context propagation, standard GenAI attributes, optional
   content capture, and lifecycle dispatch.
2. `agentscope-extensions-langsmith` contributes a lifecycle handler that adds LangSmith-specific
   attributes. OTLP transport remains ordinary OpenTelemetry configuration.

```text
OtelTracingMiddleware
  +-- creates invoke_agent/chat/execute_tool spans
  +-- records standard GenAI structural attributes
  +-- optionally records standard GenAI content attributes
  +-- dispatches OtelTracingHandler lifecycle callbacks
          +-- application handlers
          +-- LangSmithTracingHandler
```

## Core API

### `OtelTracingOperation`

An enum identifies the span type:

```java
public enum OtelTracingOperation {
    AGENT,
    MODEL,
    TOOL
}
```

### `OtelTracingOutcome`

The terminal outcome is explicit so handlers can distinguish cancellation from successful
completion without inspecting implementation-specific span state:

```java
public enum OtelTracingOutcome {
    COMPLETED,
    ERROR,
    CANCELLED
}
```

### `OtelTracingContext`

One context instance is created for each span subscription. It contains:

- the mutable OpenTelemetry `Span`;
- `OtelTracingOperation`;
- the `Agent` and call-scoped `RuntimeContext`;
- exactly one input: `AgentInput`, `ModelCallInput`, or `ActingInput`;
- a per-invocation concurrent state map for lifecycle handlers.

The class uses package-private static factories so invalid operation/input combinations cannot be
constructed. Input access is class-based so adding a future operation does not require another
public getter on the context. State access uses explicit names to avoid confusion with
`RuntimeContext` attributes:

```java
public Span getSpan();
public OtelTracingOperation getOperation();
public Agent getAgent();
public RuntimeContext getRuntimeContext();
public <T> T getInput(Class<T> inputType);
public void putState(Object key, Object value);
public <T> T getState(Object key, Class<T> type);
public void removeState(Object key);
```

`getInput` and `getState` return `null` when the requested type does not match, following the
existing typed `RuntimeContext` convention. Object keys prevent state collisions between
independent handlers. Keys, types, and stored values are non-null; removal is explicit.

`RuntimeContext` may be null because the current middleware contract and existing tests allow it.
Every built-in and extension handler must handle that case.

### `OtelTracingHandler`

Handlers observe the full span lifecycle:

```java
public interface OtelTracingHandler {
    default boolean supports(OtelTracingContext context) {
        return true;
    }

    default void onStart(OtelTracingContext context) {}

    default void onEvent(OtelTracingContext context, AgentEvent event) {}

    default void onError(OtelTracingContext context, Throwable error) {}

    default void onStop(OtelTracingContext context, OtelTracingOutcome outcome) {}
}
```

Lifecycle order is:

```text
span.start
handler.onStart
zero or more handler.onEvent calls
handler.onError (error path only)
handler.onStop(context, outcome)
span.end
```

Each handler callback, including `supports`, is isolated with `try/catch`. A failing callback
produces a warning containing the handler class and lifecycle phase, then dispatch continues to the
remaining handlers. A failed `supports` call is treated as `false`. Ordinary handler failures never
change the result of the agent operation. JVM/Reactor fatal errors are rethrown through
`Exceptions.throwIfFatal` and are never swallowed.

`supports` is evaluated once when the tracing context is created. The resulting handler list is
stored on that context so a stateful decision cannot change during the stream.

Synchronous exceptions from `next.apply(input)` follow the same `ERROR` lifecycle as asynchronous
publisher failures. The dispatcher records the exception, calls `onError`, calls `onStop` once, and
ends the span before rethrowing. This closes an existing span-leak edge case without changing the
exception observed by callers.

### `OtelTracingOptions`

Content capture uses immutable options:

```java
OtelTracingOptions.builder()
        .includeInputMessages(false)
        .includeOutputMessages(false)
        .includeToolCallContent(false)
        .build();
```

All three flags default to `false`.

- `includeInputMessages` controls `gen_ai.input.messages` on agent and model spans.
- `includeOutputMessages` controls `gen_ai.output.messages` on agent and model spans.
- `includeToolCallContent` controls `gen_ai.tool.call.arguments` and
  `gen_ai.tool.call.result` on tool spans.

Structural attributes such as names, counts, token usage, tool call IDs, duration, status, and
exceptions remain independent of these flags.

### Middleware construction

The existing constructor remains unchanged:

```java
new OtelTracingMiddleware();
```

It is equivalent to a builder with default options and no handlers:

```java
OtelTracingMiddleware.builder().build();
```

The builder API is:

```java
OtelTracingMiddleware.builder()
        .options(options)
        .handler(handler)
        .handlers(handlers)
        .build();
```

The builder follows existing repository conventions: `Objects.requireNonNull` validation,
immutable defensive copies through `List.copyOf`, fluent methods returning the same builder, and a
public no-argument constructor retained for compatibility. Null options, handlers, handler
collections, or collection elements are rejected at construction time.

## Standard Content Encoding

Content is serialized only when its corresponding option is enabled.

- Input and output messages use JSON values compatible with the OpenTelemetry GenAI
  `gen_ai.input.messages` and `gen_ai.output.messages` attributes.
- Text, thinking, tool calls, and textual tool results are represented explicitly.
- Image, audio, video, and binary data include type and safe reference metadata only; inline base64
  or raw bytes are never copied into span attributes.
- Model streaming output is accumulated from block and tool-call delta events in the per-span
  context and written during `onStop`.
- Agent output is taken from `AgentResultEvent`.
- Tool output is accumulated per tool call from tool-result events.
- Empty or unavailable content does not produce an attribute.

The current acting middleware intentionally creates one span for a batch of tool calls. To avoid
silently losing content while keeping that existing span model:

- a single tool call serializes its argument object and result directly;
- multiple tool calls serialize an ordered JSON array whose entries contain call ID, name, and the
  corresponding arguments or result;
- the encoding remains valid JSON and deterministic, but the documentation calls out that the
  multi-call value is an AgentScope batching convention rather than a one-call-per-span semantic
  convention.

Serialization failures are logged and omit only the affected content attribute.

Content capture is implemented as an internal core recorder that runs before application handlers.
Literal attribute names are isolated in one package-private constants class, avoiding a runtime
dependency on incubating semantic-convention artifacts while those attributes remain unstable.

## LangSmith Extension

The optional Maven module is:

```text
agentscope-extensions/agentscope-extensions-langsmith
```

The module is also registered in the official AgentScope BOM and as an optional dependency of
`agentscope-all`, following existing extension publication conventions.

Package:

```text
io.agentscope.extensions.langsmith
```

### `LangSmithTracingHandler`

The handler sets deterministic LangSmith run types:

| AgentScope operation | `langsmith.span.kind` |
|---|---|
| `AGENT` | `chain` |
| `MODEL` | `llm` |
| `TOOL` | `tool` |

It also adds safe default metadata when values are present:

| AgentScope value | LangSmith attribute |
|---|---|
| `RuntimeContext.userId` | `langsmith.metadata.user_id` |
| `RuntimeContext.sessionId` | `langsmith.metadata.session_id` |
| agent ID | `langsmith.metadata.agent_id` |
| agent name | `langsmith.metadata.agent_name` |

AgentScope session IDs are intentionally metadata. The handler does not automatically set
`langsmith.trace.session_id` or `langsmith.trace.session_name`, because LangSmith also uses those
fields for project/experiment session routing. Applications may add them explicitly through the
metadata/customization API when that routing is intended.

Configuration supports static tags and string metadata plus per-call string metadata:

```java
LangSmithTracingHandler.builder()
        .tags(List.of("production", "agentscope"))
        .metadata(Map.of("service", "customer-support"))
        .metadataProvider(
                context -> {
                    RuntimeContext runtimeContext = context.getRuntimeContext();
                    String tenantId =
                            runtimeContext != null
                                    ? runtimeContext.get("tenantId", String.class)
                                    : null;
                    return tenantId != null ? Map.of("tenant_id", tenantId) : Map.of();
                })
        .build();
```

Tags and metadata are written to all supported spans. This is deterministic for nested agents and
for traces with a non-AgentScope parent; it avoids exposing an unreliable notion of "root" in the
first public API. Model and tool runs therefore remain independently filterable.

Metadata precedence is safe defaults, then static metadata, then provider metadata. Applications
can deliberately override the defaults. Static configuration rejects blank keys, blank tags, and
null values at build time. Invalid dynamic entries are ignored because they are produced during an
active agent call. Provider failures are isolated by the core handler dispatcher.

### OTLP transport

The extension does not create or register an OpenTelemetry SDK. Applications configure standard
OTLP export, for example:

```text
OTEL_EXPORTER_OTLP_ENDPOINT=https://api.smith.langchain.com/otel
OTEL_EXPORTER_OTLP_HEADERS=x-api-key=<key>,Langsmith-Project=<project>
OTEL_SERVICE_NAME=<service>
```

Self-hosted and regional LangSmith endpoints therefore require no AgentScope API changes. Sampling,
resource attributes, batching, fan-out through an OpenTelemetry Collector, and SDK shutdown remain
normal OpenTelemetry concerns.

## Error and Cancellation Semantics

- Existing middleware status and exception recording behavior is preserved.
- `onError` runs after the span records the operation error and before `onStop`.
- Cancellation does not call `onError`; it retains the existing `cancelled` span status and calls
  `onStop(context, CANCELLED)` before ending the span.
- `onStop` runs at most once, guarded by the same atomic end guard as the span.
- Non-fatal handler exceptions never replace application exceptions; JVM/Reactor fatal errors are
  rethrown.
- A cold tracing publisher creates a fresh span, tracing context, state map, and supported-handler
  snapshot for every subscription.

## Backward Compatibility

- Existing `new OtelTracingMiddleware()` callers receive equivalent default telemetry: no prompt,
  completion, or tool content.
- Existing span names and structural attributes do not change.
- Existing Reactor hook registration remains idempotent.
- No new runtime dependency is added to `agentscope-core`.
- The LangSmith module is optional and has no effect unless explicitly added and configured.

## Testing Strategy

Core tests use `InMemorySpanExporter` and cover:

- the existing constructor and default attributes;
- handler lifecycle and event delivery for agent, model, and tool operations;
- lifecycle order for success, asynchronous error, synchronous `next.apply` failure, and
  cancellation;
- handler filtering plus exceptions from `supports` and every callback phase;
- ordering across multiple handlers and exactly-once terminal callbacks;
- a fresh span and state map for repeated subscriptions;
- per-span state isolation under concurrent calls;
- builder null validation and immutable defensive copies;
- content attributes absent by default;
- each content flag independently enabling only its attributes;
- text, thinking, tool-use, textual tool-result, safe media-reference, and omitted-base64 encoding;
- streaming model output and single- and multi-tool results being accumulated correctly;
- empty, missing, and serialization-failure paths;
- parent-child relationships remaining correct across Reactor thread hops.

LangSmith extension tests cover:

- operation-to-kind mapping;
- safe default metadata mapping;
- tags on each supported span;
- static and per-call metadata merging and precedence;
- absence of `langsmith.trace.session_id` and `langsmith.trace.session_name` by default;
- builder validation, invalid dynamic entries, null runtime context, and provider failure handling;
- no mutation of `GlobalOpenTelemetry` and no network traffic.

No test sends network traffic to LangSmith.

### Coverage gates

The root build currently generates JaCoCo reports but does not enforce a repository-wide threshold.
This change will not add a global gate that could fail unrelated modules. Instead, implementation
acceptance requires:

- at least 90% line coverage and 80% branch coverage for every new public tracing class;
- at least 90% branch coverage for the lifecycle dispatcher and content serializer, where most
  defects are conditional or terminal-state defects;
- 100% line coverage for `LangSmithTracingHandler`;
- no decrease in `agentscope-core` aggregate line or branch coverage from the pre-change baseline.

As a review reference, running only the existing `OtelTracingMiddlewareTest` currently covers 84%
of that middleware's lines and 50% of its branches. This targeted result is not the aggregate
baseline, but it confirms that lifecycle edge cases need substantially more branch coverage.

The implementation handoff records the exact JaCoCo counters and the commands used to produce
them. Spotless, module tests, and aggregate tests for affected reactor modules must all pass.

## Code and API Conventions

- Java 17 and the repository's Spotless AOSP Google Java Format configuration are authoritative.
- Every new Java file includes the repository Apache 2.0 header; every public type and public method
  has useful Javadoc and valid doclint.
- Imports are explicit and unused imports are removed; wildcard imports are forbidden.
- Public configuration objects are immutable, builders validate eagerly, and mutable collections
  are never retained or exposed.
- Tests use JUnit 5, descriptive method names consistent with the existing tracing test, and no
  timing sleeps when a latch or deterministic Reactor assertion can be used.
- Core code depends only on vendor-neutral OpenTelemetry APIs already present in
  `agentscope-core`. The LangSmith module follows the repository's optional/provided core dependency
  convention and adds no LangSmith SDK or exporter dependency.

## Documentation

Documentation will include:

- generic handler and content option examples;
- LangSmith OTLP environment configuration;
- Java SDK initialization example for applications not using OTel autoconfiguration;
- privacy warnings matching the default-off behavior;
- a note that only one global OpenTelemetry SDK should be registered;
- an OpenTelemetry Collector fan-out example.

## Upstream PR Strategy

The work should be submitted as two reviewable changes:

1. Core PR: lifecycle handler API, builder, default-off standard GenAI content attributes, tests,
   and vendor-neutral documentation.
2. Extension PR: optional LangSmith handler, module wiring, tests, and LangSmith setup guide.

The second PR depends only on the public API introduced by the first. Reviewers can accept the
general observability improvement without accepting a vendor integration.
