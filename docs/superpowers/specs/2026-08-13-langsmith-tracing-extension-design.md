# LangSmith Tracing Extension Design

## Status

Approved direction: extend AgentScope Java tracing through a vendor-neutral OpenTelemetry
lifecycle API, then provide LangSmith support as an optional extension.

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

### `OtelTracingContext`

One context instance is created for each span. It contains:

- the mutable OpenTelemetry `Span`;
- `OtelTracingOperation`;
- the `Agent` and call-scoped `RuntimeContext`;
- exactly one typed input: `AgentInput`, `ModelCallInput`, or `ActingInput`;
- a per-invocation concurrent state map for lifecycle handlers.

The class uses package-private static factories so invalid operation/input combinations cannot be
constructed. It exposes nullable typed getters because the operation enum is the discriminator:

```java
public Span getSpan();
public OtelTracingOperation getOperation();
public Agent getAgent();
public RuntimeContext getRuntimeContext();
public AgentInput getAgentInput();
public ModelCallInput getModelCallInput();
public ActingInput getActingInput();
public void put(Object key, Object value);
public <T> T get(Object key, Class<T> type);
```

Object keys prevent state collisions between independent handlers. Passing `null` as a value
removes the entry.

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

    default void onStop(OtelTracingContext context) {}
}
```

Lifecycle order is:

```text
span.start
handler.onStart
zero or more handler.onEvent calls
handler.onError (error path only)
handler.onStop
span.end
```

Each handler callback is isolated with `try/catch`. A failing handler produces a warning containing
the handler class and lifecycle phase, then dispatch continues to the remaining handlers. It never
changes the result of the agent operation.

`supports` is evaluated once when the tracing context is created. The resulting handler list is
stored on that context so a stateful decision cannot change during the stream.

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

Null handlers are rejected at construction time. The middleware stores an immutable defensive copy
of the handler list.

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

Serialization failures are logged and omit only the affected content attribute.

## LangSmith Extension

The optional Maven module is:

```text
agentscope-extensions/agentscope-extensions-langsmith
```

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

Configuration supports static tags and metadata plus per-call metadata:

```java
LangSmithTracingHandler.builder()
        .tags(List.of("production", "agentscope"))
        .metadata(Map.of("service", "customer-support"))
        .metadataProvider(
                context -> Map.of("tenant_id", context.get("tenantId")))
        .build();
```

Static tags are written to the root agent span only to avoid duplicating tags on every child run.
Metadata is written to all supported spans so model and tool runs remain independently filterable.
Null keys and values from a provider are ignored. Provider failures are isolated by the core
handler dispatcher.

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
  `onStop` before ending the span.
- `onStop` runs at most once, guarded by the same atomic end guard as the span.
- Handler exceptions never replace application exceptions.

## Backward Compatibility

- Existing `new OtelTracingMiddleware()` callers receive byte-for-byte-equivalent default
  telemetry: no prompt, completion, or tool content.
- Existing span names and structural attributes do not change.
- Existing Reactor hook registration remains idempotent.
- No new runtime dependency is added to `agentscope-core`.
- The LangSmith module is optional and has no effect unless explicitly added and configured.

## Testing Strategy

Core tests use `InMemorySpanExporter` and cover:

- the existing constructor and default attributes;
- handler lifecycle order for success, error, and cancellation;
- handler filtering through `supports`;
- callback exception isolation;
- per-span state isolation under concurrent calls;
- content attributes absent by default;
- each content flag independently enabling only its attributes;
- streaming model output and multiple tool results being accumulated correctly;
- parent-child relationships remaining correct across Reactor thread hops.

LangSmith extension tests cover:

- operation-to-kind mapping;
- safe default metadata mapping;
- root-only tags;
- static and per-call metadata merging;
- absence of `langsmith.trace.session_id` and `langsmith.trace.session_name` by default;
- null and provider failure handling.

No test sends network traffic to LangSmith.

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
