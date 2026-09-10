# OpenTelemetry Tracing Core Extension Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a vendor-neutral, failure-isolated tracing lifecycle API and opt-in standard GenAI content attributes to `OtelTracingMiddleware` without changing its default telemetry.

**Architecture:** `OtelTracingMiddleware` continues to own span creation and Reactor context propagation. Small public value types describe an invocation, a package-private dispatcher isolates lifecycle handlers, and a package-private content handler uses a focused serializer/accumulator to write standard GenAI JSON attributes only when enabled. The application remains responsible for the global OpenTelemetry SDK and exporters.

**Tech Stack:** Java 17, Reactor `Flux`, OpenTelemetry API and Reactor instrumentation, Jackson through AgentScope `JsonCodec`, SLF4J, JUnit 5, OpenTelemetry `InMemorySpanExporter`, Maven, Spotless, JaCoCo.

## Global Constraints

- Preserve `new OtelTracingMiddleware()` and all existing span names and structural attributes.
- All content flags default to `false`; prompt, completion, tool arguments, and tool results are never exported without explicit opt-in.
- Never copy raw bytes or inline base64 data into span attributes; only safe media type/reference metadata may be emitted.
- Do not add an incubating OpenTelemetry semantic-conventions runtime dependency.
- Do not create, replace, shut down, or otherwise mutate `GlobalOpenTelemetry` outside existing tests.
- Handler selection and every non-fatal handler callback failure must be isolated and must never change the agent result or replace an application exception; JVM/Reactor fatal errors must be rethrown with `Exceptions.throwIfFatal`.
- A fresh span, context, supported-handler snapshot, terminal guard, and state map must be created for every subscription.
- Java 17 and the repository Spotless AOSP Google Java Format configuration are authoritative.
- Every new Java file needs the repository Apache 2.0 header; all public APIs require useful Javadoc that passes doclint.
- New public tracing classes require at least 90% line and 80% branch coverage; the dispatcher and content serializer require at least 90% branch coverage.
- Do not reduce aggregate `agentscope-core` line or branch coverage.

## File Map

- `agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingOperation.java`: public operation discriminator.
- `agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingOutcome.java`: public terminal outcome discriminator.
- `agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingOptions.java`: immutable default-off content settings and builder.
- `agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingContext.java`: per-subscription invocation data and concurrent handler state.
- `agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingHandler.java`: public lifecycle extension contract.
- `agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingDispatcher.java`: package-private handler filtering, ordering, logging, and failure isolation.
- `agentscope-core/src/main/java/io/agentscope/core/tracing/GenAiTracingAttributes.java`: package-private attribute-name constants.
- `agentscope-core/src/main/java/io/agentscope/core/tracing/GenAiContentSerializer.java`: package-private deterministic JSON conversion with safe media handling.
- `agentscope-core/src/main/java/io/agentscope/core/tracing/GenAiContentAccumulator.java`: package-private per-span streaming aggregation.
- `agentscope-core/src/main/java/io/agentscope/core/tracing/GenAiContentTracingHandler.java`: package-private option-aware attribute handler.
- `agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingMiddleware.java`: builder, public handler wiring, and common terminal lifecycle orchestration.
- `agentscope-core/src/test/java/io/agentscope/core/tracing/OtelTracingApiTest.java`: public API, validation, state, and defensive-copy tests.
- `agentscope-core/src/test/java/io/agentscope/core/tracing/OtelTracingDispatcherTest.java`: callback order/filtering/failure-isolation tests.
- `agentscope-core/src/test/java/io/agentscope/core/tracing/GenAiContentSerializerTest.java`: exact JSON and privacy tests.
- `agentscope-core/src/test/java/io/agentscope/core/tracing/OtelTracingMiddlewareTest.java`: integration, terminal semantics, content flags, concurrency, and propagation.
- `docs/v2/en/docs/building-blocks/middleware.md`: English generic tracing handler/content documentation.
- `docs/v2/zh/docs/building-blocks/middleware.md`: Chinese generic tracing handler/content documentation.

---

### Task 1: Public tracing value types and immutable options

**Files:**
- Create: `agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingOperation.java`
- Create: `agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingOutcome.java`
- Create: `agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingOptions.java`
- Create: `agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingContext.java`
- Test: `agentscope-core/src/test/java/io/agentscope/core/tracing/OtelTracingApiTest.java`

**Interfaces:**
- Consumes: `Agent`, nullable `RuntimeContext`, `AgentInput`, `ModelCallInput`, `ActingInput`, and OpenTelemetry `Span`.
- Produces: `OtelTracingOperation`, `OtelTracingOutcome`, `OtelTracingOptions.builder()`, and package-private `OtelTracingContext.forAgent/forModel/forTool` factories used by later tasks.

- [ ] **Step 1: Write failing API tests**

Create `OtelTracingApiTest` with tests equivalent to the following concrete cases:

```java
@Test
void optionsDefaultToNoContent() {
    OtelTracingOptions options = OtelTracingOptions.defaults();
    assertFalse(options.isIncludeInputMessages());
    assertFalse(options.isIncludeOutputMessages());
    assertFalse(options.isIncludeToolCallContent());
}

@Test
void optionsBuilderEnablesFlagsIndependently() {
    OtelTracingOptions options =
            OtelTracingOptions.builder()
                    .includeInputMessages(true)
                    .includeOutputMessages(false)
                    .includeToolCallContent(true)
                    .build();
    assertTrue(options.isIncludeInputMessages());
    assertFalse(options.isIncludeOutputMessages());
    assertTrue(options.isIncludeToolCallContent());
}

@Test
void contextDiscriminatesInputAndProvidesTypedState() {
    AgentInput input = new AgentInput(List.of());
    OtelTracingContext context =
            OtelTracingContext.forAgent(
                    Span.getInvalid(), null, RuntimeContext.empty(), input);
    Object key = new Object();
    context.putState(key, "value");

    assertEquals(OtelTracingOperation.AGENT, context.getOperation());
    assertSame(input, context.getInput(AgentInput.class));
    assertNull(context.getInput(ModelCallInput.class));
    assertEquals("value", context.getState(key, String.class));
    assertNull(context.getState(key, Integer.class));
    context.removeState(key);
    assertNull(context.getState(key, String.class));
}

@Test
void contextRejectsNullStateArguments() {
    OtelTracingContext context =
            OtelTracingContext.forAgent(
                    Span.getInvalid(), null, null, new AgentInput(List.of()));
    assertThrows(NullPointerException.class, () -> context.putState(null, "value"));
    assertThrows(NullPointerException.class, () -> context.putState(new Object(), null));
    assertThrows(NullPointerException.class, () -> context.getState(null, String.class));
    assertThrows(NullPointerException.class, () -> context.getState(new Object(), null));
}
```

Also assert all three factories reject null span/input values, while nullable agent and runtime
context remain accepted.

- [ ] **Step 2: Run the API tests and verify the compile failure**

Run:

```powershell
mvn -pl agentscope-core -Dtest=OtelTracingApiTest test
```

Expected: test compilation fails because the four public tracing types do not exist.

- [ ] **Step 3: Implement the enums, options, and context**

Implement the public contracts with these exact signatures:

```java
public enum OtelTracingOperation { AGENT, MODEL, TOOL }

public enum OtelTracingOutcome { COMPLETED, ERROR, CANCELLED }

public final class OtelTracingOptions {
    public static OtelTracingOptions defaults();
    public static Builder builder();
    public boolean isIncludeInputMessages();
    public boolean isIncludeOutputMessages();
    public boolean isIncludeToolCallContent();

    public static final class Builder {
        public Builder includeInputMessages(boolean includeInputMessages);
        public Builder includeOutputMessages(boolean includeOutputMessages);
        public Builder includeToolCallContent(boolean includeToolCallContent);
        public OtelTracingOptions build();
    }
}
```

Use one `private static final OtelTracingOptions DEFAULTS` instance and return it from `defaults()`.
Implement `OtelTracingContext` with final invocation fields and a `ConcurrentHashMap<Object,Object>`:

```java
public final class OtelTracingContext {
    static OtelTracingContext forAgent(
            Span span, Agent agent, RuntimeContext runtimeContext, AgentInput input);
    static OtelTracingContext forModel(
            Span span, Agent agent, RuntimeContext runtimeContext, ModelCallInput input);
    static OtelTracingContext forTool(
            Span span, Agent agent, RuntimeContext runtimeContext, ActingInput input);

    public Span getSpan();
    public OtelTracingOperation getOperation();
    public Agent getAgent();
    public RuntimeContext getRuntimeContext();
    public <T> T getInput(Class<T> inputType);
    public void putState(Object key, Object value);
    public <T> T getState(Object key, Class<T> type);
    public void removeState(Object key);
}
```

Validate required inputs with messages such as `"span must not be null"` and use
`inputType.isInstance(input) ? inputType.cast(input) : null` for typed input access.

- [ ] **Step 4: Run focused tests and formatting**

Run:

```powershell
mvn -pl agentscope-core -Dtest=OtelTracingApiTest test
mvn -pl agentscope-core spotless:check
```

Expected: both commands succeed.

- [ ] **Step 5: Commit the public value types**

```powershell
git add agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingOperation.java agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingOutcome.java agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingOptions.java agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingContext.java agentscope-core/src/test/java/io/agentscope/core/tracing/OtelTracingApiTest.java
git commit -m "feat: add OpenTelemetry tracing context API"
```

---

### Task 2: Failure-isolated handler lifecycle dispatcher

**Files:**
- Create: `agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingHandler.java`
- Create: `agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingDispatcher.java`
- Test: `agentscope-core/src/test/java/io/agentscope/core/tracing/OtelTracingDispatcherTest.java`

**Interfaces:**
- Consumes: `OtelTracingContext`, `OtelTracingOutcome`, `AgentEvent`, and an ordered `List<OtelTracingHandler>`.
- Produces: `OtelTracingHandler` public callbacks and package-private `OtelTracingDispatcher.create/start/event/error/stop` methods.

- [ ] **Step 1: Write failing dispatcher tests**

Create this nested test handler, with constructor-assigned `name` and `calls` fields, in
`OtelTracingDispatcherTest`:

```java
private static final class RecordingHandler implements OtelTracingHandler {
    private final String name;
    private final List<String> calls;

    private RecordingHandler(String name, List<String> calls) {
        this.name = name;
        this.calls = calls;
    }

    @Override
    public void onStart(OtelTracingContext context) {
        calls.add(name + ":start");
    }

    @Override
    public void onEvent(OtelTracingContext context, AgentEvent event) {
        calls.add(name + ":event");
    }

    @Override
    public void onError(OtelTracingContext context, Throwable error) {
        calls.add(name + ":error");
    }

    @Override
    public void onStop(OtelTracingContext context, OtelTracingOutcome outcome) {
        calls.add(name + ":stop:" + outcome);
    }
}
```

Cover the exact order and failure cases:

```java
@Test
void dispatchesSupportedHandlersInRegistrationOrder() {
    List<String> calls = new CopyOnWriteArrayList<>();
    OtelTracingContext context = agentContext();
    OtelTracingDispatcher dispatcher =
            OtelTracingDispatcher.create(
                    context,
                    List.of(new RecordingHandler("first", calls),
                            new RecordingHandler("second", calls)));

    dispatcher.start();
    dispatcher.event(new AgentStartEvent("s", "r", "agent"));
    dispatcher.stop(OtelTracingOutcome.COMPLETED);

    assertEquals(
            List.of("first:start", "second:start", "first:event", "second:event",
                    "first:stop:COMPLETED", "second:stop:COMPLETED"),
            calls);
}

@Test
void supportsIsEvaluatedOnceAndFalseHandlersReceiveNothing() {
    AtomicInteger supportsCalls = new AtomicInteger();
    OtelTracingHandler handler = new OtelTracingHandler() {
        @Override
        public boolean supports(OtelTracingContext context) {
            supportsCalls.incrementAndGet();
            return false;
        }
    };
    OtelTracingDispatcher dispatcher =
            OtelTracingDispatcher.create(agentContext(), List.of(handler));
    dispatcher.start();
    dispatcher.stop(OtelTracingOutcome.COMPLETED);
    assertEquals(1, supportsCalls.get());
}
```

Add five named tests whose first handler throws `IllegalStateException` independently from `supports`, `onStart`, `onEvent`,
`onError`, and `onStop`; each test adds a non-throwing `RecordingHandler` second and asserts its
corresponding callback is recorded and no exception escapes. Add
`emptyHandlerListDispatchesWithoutFailure` and
`dispatcherRejectsNullContextListEventErrorAndOutcome`, using `assertDoesNotThrow` for the former and
`assertThrows(NullPointerException.class, ...)` for every null argument in the latter.
Add `fatalHandlerErrorIsRethrown`, have `onStart` throw `LinkageError("fatal")`, and assert the same
error escapes `dispatcher.start()`.

- [ ] **Step 2: Run the dispatcher test and verify the compile failure**

```powershell
mvn -pl agentscope-core -Dtest=OtelTracingDispatcherTest test
```

Expected: compilation fails because `OtelTracingHandler` and `OtelTracingDispatcher` are absent.

- [ ] **Step 3: Implement the public handler contract**

Use exactly this extensible interface:

```java
public interface OtelTracingHandler {
    default boolean supports(OtelTracingContext context) { return true; }
    default void onStart(OtelTracingContext context) {}
    default void onEvent(OtelTracingContext context, AgentEvent event) {}
    default void onError(OtelTracingContext context, Throwable error) {}
    default void onStop(OtelTracingContext context, OtelTracingOutcome outcome) {}
}
```

Document callback ordering, single-thread assumptions not being guaranteed, and the fact that
handler failures are isolated by the middleware.

- [ ] **Step 4: Implement dispatcher filtering and safe callback execution**

Implement a final package-private dispatcher with an immutable supported list:

```java
final class OtelTracingDispatcher {
    static OtelTracingDispatcher create(
            OtelTracingContext context, List<OtelTracingHandler> handlers);
    void start();
    void event(AgentEvent event);
    void error(Throwable error);
    void stop(OtelTracingOutcome outcome);
}
```

`create` evaluates `supports` once per handler, treats a thrown exception as unsupported, and logs:

```java
log.warn("Tracing handler {} failed during {}", handler.getClass().getName(), phase, error);
```

All other phases loop in registration order and call a private `invoke(handler, phase, action)`
method that catches `Throwable`, immediately calls `reactor.core.Exceptions.throwIfFatal(error)`,
then logs and continues only for non-fatal errors. Apply the same rule to `supports`. Validate
context, handler list, events, errors, and outcomes with `Objects.requireNonNull`. Store the result
using `List.copyOf`.

- [ ] **Step 5: Run dispatcher tests, API tests, and Spotless**

```powershell
mvn -pl agentscope-core -Dtest=OtelTracingApiTest,OtelTracingDispatcherTest test
mvn -pl agentscope-core spotless:check
```

Expected: both commands succeed and the exception-isolation cases emit warnings without failing.

- [ ] **Step 6: Commit the lifecycle contract**

```powershell
git add agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingHandler.java agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingDispatcher.java agentscope-core/src/test/java/io/agentscope/core/tracing/OtelTracingDispatcherTest.java
git commit -m "feat: add failure-isolated tracing handlers"
```

---

### Task 3: Middleware builder and complete terminal semantics

**Files:**
- Modify: `agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingMiddleware.java`
- Modify: `agentscope-core/src/test/java/io/agentscope/core/tracing/OtelTracingApiTest.java`
- Modify: `agentscope-core/src/test/java/io/agentscope/core/tracing/OtelTracingMiddlewareTest.java`

**Interfaces:**
- Consumes: Tasks 1-2 public APIs and dispatcher.
- Produces: `OtelTracingMiddleware.builder().options(...).handler(...).handlers(...).build()` and complete per-subscription lifecycle integration for agent/model/tool spans.

- [ ] **Step 1: Add failing builder validation and defensive-copy tests**

Append to `OtelTracingApiTest`:

```java
@Test
void middlewareBuilderRejectsNullsAndCopiesHandlers() {
    OtelTracingHandler handler = new OtelTracingHandler() {};
    List<OtelTracingHandler> handlers = new ArrayList<>();
    handlers.add(handler);
    OtelTracingMiddleware middleware =
            OtelTracingMiddleware.builder().handlers(handlers).build();
    handlers.clear();

    assertNotNull(middleware);
    assertThrows(
            NullPointerException.class,
            () -> OtelTracingMiddleware.builder().options(null));
    assertThrows(
            NullPointerException.class,
            () -> OtelTracingMiddleware.builder().handler(null));
    assertThrows(
            NullPointerException.class,
            () -> OtelTracingMiddleware.builder().handlers(null));
    assertThrows(
            NullPointerException.class,
            () -> OtelTracingMiddleware.builder().handlers(Arrays.asList(handler, null)));
}
```

Verify the defensive copy by invoking `middleware.onAgent(...)` with a recording handler and
asserting it still receives callbacks after the caller clears the original list.

- [ ] **Step 2: Add failing lifecycle integration tests**

Append focused integration tests to `OtelTracingMiddlewareTest` for:

```java
@Test
void handlerSeesAgentModelAndToolContexts() {
    List<OtelTracingOperation> operations = new CopyOnWriteArrayList<>();
    OtelTracingHandler handler = new OtelTracingHandler() {
        @Override
        public void onStart(OtelTracingContext context) {
            operations.add(context.getOperation());
        }
    };
    OtelTracingMiddleware traced =
            OtelTracingMiddleware.builder().handler(handler).build();
    Agent agent = stubAgent("handler-agent", "handler-agent-id");
    ModelCallInput modelInput =
            new ModelCallInput(List.of(), null, null, new StubModel("handler-model"));
    ActingInput toolInput =
            new ActingInput(List.of(
                    ToolUseBlock.builder()
                            .id("handler-call")
                            .name("handler-tool")
                            .input(Map.of())
                            .build()));
    traced.onAgent(
                    agent,
                    RuntimeContext.empty(),
                    new AgentInput(List.of()),
                    ignored ->
                            traced.onModelCall(
                                            agent,
                                            RuntimeContext.empty(),
                                            modelInput,
                                            modelIgnored -> Flux.empty())
                                    .thenMany(
                                            traced.onActing(
                                                    agent,
                                                    RuntimeContext.empty(),
                                                    toolInput,
                                                    toolIgnored -> Flux.empty())))
            .then()
            .block();
    assertEquals(List.of(AGENT, MODEL, TOOL), operations);
}
```

Write concrete tests that record the complete phase list for successful completion, asynchronous
`Flux.error`, synchronous `next.apply` throwing `IllegalStateException("sync boom")`, and disposal
of `Flux.never()`. Expected terminal outcomes are `COMPLETED`, `ERROR`, `ERROR`, and `CANCELLED`.
For both error paths, assert the original exception remains observable and `onError` precedes
`onStop`. For cancellation, assert `onError` is absent. Replace the existing cancellation
`Thread.sleep(50)` with a `CountDownLatch` released from handler `onStop`.

- [ ] **Step 3: Run the focused tests and observe failure**

```powershell
mvn -pl agentscope-core -Dtest=OtelTracingApiTest,OtelTracingMiddlewareTest test
```

Expected: builder methods do not compile and lifecycle assertions fail until handlers are wired.

- [ ] **Step 4: Add middleware construction API**

Retain `public OtelTracingMiddleware()` and delegate it to default options and no public handlers.
Add:

```java
public static Builder builder();

public static final class Builder {
    public Builder options(OtelTracingOptions options);
    public Builder handler(OtelTracingHandler handler);
    public Builder handlers(List<? extends OtelTracingHandler> handlers);
    public OtelTracingMiddleware build();
}
```

The middleware stores `OtelTracingOptions` and `List<OtelTracingHandler>` in final fields. Each
builder method validates immediately; `build()` takes another `List.copyOf` defensive snapshot.
Keep Reactor hook registration in the middleware constructor and preserve its existing idempotent
double-checked locking.

- [ ] **Step 5: Refactor common stream lifecycle without changing structural attributes**

For each `Flux.deferContextual` subscription:

1. Create the span with the current name and structural attributes.
2. Create the operation-specific `OtelTracingContext`.
3. Create `OtelTracingDispatcher` from the configured handlers and call `start()`.
4. Invoke `next.apply(input)` inside `try/catch (Throwable error)`.
5. Dispatch every event after existing structural event handling.
6. Use the existing atomic terminal guard to run one of these exact sequences:

```java
// completed
dispatcher.stop(OtelTracingOutcome.COMPLETED);
span.setStatus(StatusCode.OK);
span.end();

// failed
span.setStatus(StatusCode.ERROR, error.getMessage());
span.recordException(error);
dispatcher.error(error);
dispatcher.stop(OtelTracingOutcome.ERROR);
span.end();

// cancelled
span.setStatus(StatusCode.ERROR, "cancelled");
dispatcher.stop(OtelTracingOutcome.CANCELLED);
span.end();
```

On a synchronous exception, finish the span with the failed sequence and return `Flux.error(error)`
so downstream callers see the same throwable. Before treating a caught `Throwable` as an ordinary
failure, call `Exceptions.throwIfFatal(error)`; fatal errors are rethrown and are not converted into
a reactive error signal. Do not call a handler outside the protected dispatcher.

- [ ] **Step 6: Add repeated-subscription and concurrent-state tests**

Add `repeatedSubscriptionCreatesFreshSpanContextAndState`: construct one result `Flux`, subscribe
with `.then().block()` twice, collect identity-hash codes for contexts and initial state values in a
recording handler, and assert two different context identities, two different exported span IDs,
two null initial values, two starts, and two completed stops. Add
`concurrentSubscriptionsKeepHandlerStateIsolated`: subscribe twice on `Schedulers.parallel()`, have
the handler create a UUID during `onStart`, store it with `putState`, and add the retrieved value to
a concurrent set during `onStop`; use a `CountDownLatch(2)` and assert two non-null distinct UUIDs.

- [ ] **Step 7: Run tracing regression tests and Spotless**

```powershell
mvn -pl agentscope-core -Dtest=OtelTracingApiTest,OtelTracingDispatcherTest,OtelTracingMiddlewareTest test
mvn -pl agentscope-core spotless:check
```

Expected: all existing span-name, attribute, parent/child, thread-hop, error, and cancellation tests
still pass together with the new lifecycle tests.

- [ ] **Step 8: Commit middleware lifecycle integration**

```powershell
git add agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingMiddleware.java agentscope-core/src/test/java/io/agentscope/core/tracing/OtelTracingApiTest.java agentscope-core/src/test/java/io/agentscope/core/tracing/OtelTracingMiddlewareTest.java
git commit -m "feat: expose OpenTelemetry tracing lifecycle"
```

---

### Task 4: Deterministic and privacy-safe GenAI content serializer

**Files:**
- Create: `agentscope-core/src/main/java/io/agentscope/core/tracing/GenAiTracingAttributes.java`
- Create: `agentscope-core/src/main/java/io/agentscope/core/tracing/GenAiContentSerializer.java`
- Test: `agentscope-core/src/test/java/io/agentscope/core/tracing/GenAiContentSerializerTest.java`

**Interfaces:**
- Consumes: `JsonCodec`, `Msg`, `ContentBlock`, `ToolUseBlock`, and ordered tool-result records.
- Produces: package-private JSON serialization methods consumed by the internal handler in Task 5.

- [ ] **Step 1: Write exact JSON-shape and privacy tests**

Use `JacksonJsonCodec` in the test and parse returned strings back to `List`/`Map` before asserting,
so tests validate semantics without depending on map implementation formatting. Cover:

```java
@Test
void serializesTextThinkingAndToolCallsInMessageOrder() {
    Msg message = Msg.builder()
            .role(MsgRole.ASSISTANT)
            .content(List.of(
                    TextBlock.builder().text("answer").build(),
                    ThinkingBlock.builder().thinking("reason").build(),
                    ToolUseBlock.builder()
                            .id("call-1")
                            .name("search")
                            .input(Map.of("q", "agentscope"))
                            .build()))
            .build();

    String json = serializer.serializeMessages(List.of(message));
    List<?> messages = codec.fromJson(json, List.class);
    Map<?, ?> encoded = (Map<?, ?>) messages.get(0);
    assertEquals("assistant", encoded.get("role"));
    assertEquals(List.of("text", "reasoning", "tool_call"), partTypes(encoded));
}
```

Add URL-backed image/audio/video cases and assert each part contains `type=uri`, `uri`, `mime_type`
when present, and the correct `modality`. Add Base64-backed cases and assert the part contains
`type=blob`, `mime_type`, and `modality` but the complete serialized JSON contains neither the base64
payload nor a `data`/`content` field for that payload. Cover `ToolResultBlock` textual outputs and
unknown content blocks by omission rather than failure.

- [ ] **Step 2: Write single- and multi-tool encoding tests**

Assert these shapes:

```json
{"q":"agentscope"}
```

for one call's `gen_ai.tool.call.arguments`, and:

```json
[
  {"id":"call-1","name":"search","arguments":{"q":"agentscope"}},
  {"id":"call-2","name":"calc","arguments":{"expression":"1+1"}}
]
```

for multiple calls. Test results with the analogous keys `id`, `name`, and `result`, preserving the
original tool-call order even when result events arrive out of order. Empty inputs/results must
return `null` so no empty attribute is written.

- [ ] **Step 3: Run serializer tests and verify compile failure**

```powershell
mvn -pl agentscope-core -Dtest=GenAiContentSerializerTest test
```

Expected: compilation fails because serializer/constants are not present.

- [ ] **Step 4: Implement isolated attribute constants**

Create a non-instantiable package-private class containing only:

```java
static final String INPUT_MESSAGES = "gen_ai.input.messages";
static final String OUTPUT_MESSAGES = "gen_ai.output.messages";
static final String TOOL_CALL_ARGUMENTS = "gen_ai.tool.call.arguments";
static final String TOOL_CALL_RESULT = "gen_ai.tool.call.result";
```

- [ ] **Step 5: Implement serializer conversion using plain maps/lists**

Create a package-private final serializer with constructor injection:

```java
GenAiContentSerializer(JsonCodec codec);
String serializeMessages(List<Msg> messages);
String serializeToolArguments(List<ToolUseBlock> toolCalls);
String serializeToolResults(
        List<ToolUseBlock> toolCalls, Map<String, String> resultsByCallId);
```

Build `LinkedHashMap` and `ArrayList` values, then call `codec.toJson`. Role mapping is `system`,
`user`, `assistant`, and `tool`. Parts use `type/content` for text, `type=reasoning/content` for
thinking, `type=tool_call/id/name/arguments` for tool use, `type=tool_result/id/name/result` for
tool results, and the safe media shapes described in Step 1. Never call `Base64Source.getData()`.
Return `null` for null/empty effective content. Let `JsonCodec` exceptions propagate to the internal
handler, which owns isolation and logging.

- [ ] **Step 6: Run serializer tests and branch-focused coverage**

```powershell
mvn -pl agentscope-core -Dtest=GenAiContentSerializerTest test
```

Expected: all serializer and privacy cases pass.

- [ ] **Step 7: Commit the serializer**

```powershell
git add agentscope-core/src/main/java/io/agentscope/core/tracing/GenAiTracingAttributes.java agentscope-core/src/main/java/io/agentscope/core/tracing/GenAiContentSerializer.java agentscope-core/src/test/java/io/agentscope/core/tracing/GenAiContentSerializerTest.java
git commit -m "feat: serialize privacy-safe GenAI trace content"
```

---

### Task 5: Opt-in content recording and streaming aggregation

**Files:**
- Create: `agentscope-core/src/main/java/io/agentscope/core/tracing/GenAiContentAccumulator.java`
- Create: `agentscope-core/src/main/java/io/agentscope/core/tracing/GenAiContentTracingHandler.java`
- Modify: `agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingMiddleware.java`
- Modify: `agentscope-core/src/test/java/io/agentscope/core/tracing/OtelTracingMiddlewareTest.java`

**Interfaces:**
- Consumes: `OtelTracingOptions`, serializer methods from Task 4, block delta events, `AgentResultEvent`, and tool result events.
- Produces: standard content attributes written before public handler `onStop` and therefore visible to backend handlers.

- [ ] **Step 1: Write default-off and independent-flag integration tests**

Create input/output messages containing the sentinel strings `PRIVATE_INPUT`, `PRIVATE_OUTPUT`,
`PRIVATE_ARGUMENT`, and `PRIVATE_RESULT`. Assert the no-argument middleware exports none of those
sentinels and has no four content attribute keys. Then build three middleware instances, enabling
one flag at a time, and assert only the corresponding keys appear:

```java
OtelTracingMiddleware inputOnly =
        OtelTracingMiddleware.builder()
                .options(OtelTracingOptions.builder().includeInputMessages(true).build())
                .build();
```

Use actual agent/model/tool calls through the middleware rather than testing the handler directly.

- [ ] **Step 2: Write failing streaming aggregation tests**

For a model span, emit ordered `TextBlockDeltaEvent`, `ThinkingBlockDeltaEvent`, and
`ToolCallDeltaEvent` values followed by `ModelCallEndEvent`; assert `gen_ai.output.messages` contains
the concatenated text/reasoning/tool arguments once. For a tool span, emit interleaved
`ToolResultTextDeltaEvent` values for two IDs followed by their `ToolResultEndEvent` values; assert
`gen_ai.tool.call.result` retains input tool order and aggregates each result independently.

For an agent span, emit `AgentResultEvent` and assert its result is the output message. Verify an
error or cancellation still flushes content accumulated before termination. Verify empty deltas do
not create attributes.

- [ ] **Step 3: Add serialization-failure isolation test**

Construct the package-private content handler in the same-package test with a `JsonCodec` whose
`toJson` throws `JsonException("encode failed")`. Attach a public recording handler after it through
the dispatcher or middleware test seam. Assert the agent event stream completes unchanged, the
recording handler reaches `onStop`, and the affected content attribute is absent.

- [ ] **Step 4: Run content integration tests and observe failures**

```powershell
mvn -pl agentscope-core -Dtest=OtelTracingMiddlewareTest test
```

Expected: content attributes and aggregation behavior are absent.

- [ ] **Step 5: Implement the per-span accumulator**

Create a package-private final `GenAiContentAccumulator` that owns only per-invocation mutable data:

```java
void onEvent(AgentEvent event);
List<Msg> getAgentOutput();
List<Msg> getModelOutput();
Map<String, String> getToolResults();
```

Use insertion-ordered maps keyed by block/tool call IDs and `StringBuilder` values. Synchronize
mutation/access methods because event delivery and cancellation may race. Preserve event order for
model parts, concatenate deltas for the same ID, and keep tool results keyed by call ID so the
serializer can restore `ActingInput.toolCalls()` order. Store an `AgentResultEvent` result directly.
When `getModelOutput()` is called, build one assistant `Msg` whose blocks follow their first start or
delta event order: `TextBlock` for text, `ThinkingBlock` for reasoning, and `ToolUseBlock` for tool
calls. A streamed tool call uses its event ID/name plus the concatenated argument JSON in
`ToolUseBlock.content`; omit blocks with neither identity nor accumulated content.

- [ ] **Step 6: Implement the internal content handler**

Create a package-private handler with:

```java
GenAiContentTracingHandler(OtelTracingOptions options, JsonCodec codec);
boolean isEnabled();
```

On start, serialize enabled agent/model inputs and tool arguments. Create/store an accumulator under
a private static object key. On event, feed the accumulator. On stop, serialize enabled outputs and
tool results. Wrap each individual serialization/attribute write in a private method that catches
`Throwable` and logs the attribute name; one failed attribute must not suppress another. Do not log
the content value.

- [ ] **Step 7: Register the internal handler before application handlers**

At middleware construction, create the internal handler from the immutable options. Per
subscription, build the effective ordered handler list as internal content handler first when
`isEnabled()` is true, followed by application handlers. This guarantees public handlers see the
final standard attributes during their `onStop`. With all flags false, do not allocate the content
handler/accumulator and preserve default overhead.

- [ ] **Step 8: Run all core tracing tests and Spotless**

```powershell
mvn -pl agentscope-core -Dtest='io.agentscope.core.tracing.*Test' test
mvn -pl agentscope-core spotless:check
```

Expected: all API, dispatcher, serializer, existing middleware, content, concurrency, and
parent/child tests pass.

- [ ] **Step 9: Commit opt-in content capture**

```powershell
git add agentscope-core/src/main/java/io/agentscope/core/tracing/GenAiContentAccumulator.java agentscope-core/src/main/java/io/agentscope/core/tracing/GenAiContentTracingHandler.java agentscope-core/src/main/java/io/agentscope/core/tracing/OtelTracingMiddleware.java agentscope-core/src/test/java/io/agentscope/core/tracing/OtelTracingMiddlewareTest.java
git commit -m "feat: add opt-in GenAI tracing content"
```

---

### Task 6: Core documentation, formatting, tests, and coverage evidence

**Files:**
- Modify: `docs/v2/en/docs/building-blocks/middleware.md`
- Modify: `docs/v2/zh/docs/building-blocks/middleware.md`
- Modify: the exact tracing test file owning any missed condition reported by JaCoCo in Step 4.

**Interfaces:**
- Consumes: the final core builder/options/handler API.
- Produces: user-facing English/Chinese guidance and recorded acceptance evidence for the core PR.

- [ ] **Step 1: Document generic handlers and default-off content**

Add matching English and Chinese sections showing this exact usage shape:

```java
OtelTracingMiddleware tracing =
        OtelTracingMiddleware.builder()
                .options(
                        OtelTracingOptions.builder()
                                .includeInputMessages(true)
                                .includeOutputMessages(true)
                                .includeToolCallContent(false)
                                .build())
                .handler(new MyTracingHandler())
                .build();
```

State explicitly that all content is disabled by default because it can contain sensitive and
high-cardinality data; enabling it is an application privacy decision. Explain that handlers enrich
spans but do not configure exporters and that exactly one application-owned global OpenTelemetry SDK
should be registered.

- [ ] **Step 2: Add Javadoc/doclint and formatting verification**

Run:

```powershell
mvn -pl agentscope-core spotless:check
mvn -pl agentscope-core -DskipTests package
```

Expected: formatting and Javadoc packaging succeed with no missing-type/doclint errors.

- [ ] **Step 3: Run the complete core test suite and generate JaCoCo**

```powershell
mvn -pl agentscope-core clean test
```

Expected: `BUILD SUCCESS` and `agentscope-core/target/site/jacoco/jacoco.xml` exists.

- [ ] **Step 4: Extract class coverage and close only measured gaps**

Run this PowerShell command:

```powershell
[xml]$report = Get-Content agentscope-core/target/site/jacoco/jacoco.xml
$names = @(
  'io/agentscope/core/tracing/OtelTracingOperation',
  'io/agentscope/core/tracing/OtelTracingOutcome',
  'io/agentscope/core/tracing/OtelTracingOptions',
  'io/agentscope/core/tracing/OtelTracingContext',
  'io/agentscope/core/tracing/OtelTracingHandler',
  'io/agentscope/core/tracing/OtelTracingDispatcher',
  'io/agentscope/core/tracing/GenAiContentSerializer',
  'io/agentscope/core/tracing/GenAiContentAccumulator',
  'io/agentscope/core/tracing/GenAiContentTracingHandler'
)
$report.report.package.class |
  Where-Object { $names -contains $_.name } |
  ForEach-Object {
    $class = $_
    $class.counter | Where-Object { $_.type -in @('LINE','BRANCH') } |
      ForEach-Object {
        $total = [double]$_.covered + [double]$_.missed
        $pct = if ($total -eq 0) { 100 } else { [math]::Round(100 * [double]$_.covered / $total, 1) }
        "{0} {1} {2}%" -f $class.name,$_.type,$pct
      }
  }
```

Expected: each new public class is at least 90% line/80% branch; dispatcher and serializer are at
least 90% branch. For every counter below its gate, open
`agentscope-core/target/site/jacoco/io.agentscope.core.tracing/<ClassName>.html`, identify the red or
yellow condition, add one test named after that condition to `OtelTracingApiTest`,
`OtelTracingDispatcherTest`, `GenAiContentSerializerTest`, or `OtelTracingMiddlewareTest`, rerun
`mvn -pl agentscope-core clean test`, and repeat the extraction until every gate passes.

- [ ] **Step 5: Compare aggregate coverage with a baseline worktree/build**

Use the clean pre-feature worktree created for execution to run the same
`mvn -pl agentscope-core clean test` command, then compare the `LINE` and `BRANCH` bundle counters
from both JaCoCo XML files. Record both percentages in the implementation handoff. Expected: neither
aggregate percentage decreases.

- [ ] **Step 6: Commit documentation and any coverage-specific tests**

```powershell
git add docs/v2/en/docs/building-blocks/middleware.md docs/v2/zh/docs/building-blocks/middleware.md agentscope-core/src/test/java/io/agentscope/core/tracing
git commit -m "docs: explain extensible OpenTelemetry tracing"
```

- [ ] **Step 7: Run the core PR final verification**

```powershell
mvn -pl agentscope-core clean verify
git diff --check
git status --short
```

Expected: Maven and diff checks succeed; only intentionally untracked workspace files such as the
existing `.codegraph/` directory remain outside commits.
