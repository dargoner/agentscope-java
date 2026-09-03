# `streamEvents` 文本输出处置实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不改变 `ReActAgent#streamEvents()` 默认事件序列的前提下，为实时文本增加可选的 `INTERMEDIATE` / `TERMINAL` 生命周期标注，并让 Harness、Remote、AG-UI 与 Web 数据面消费同一语义。

**Architecture:** Core 新增强类型处置事件、订阅级 `ReplyLifecycleTracker` 和 post-middleware `AgentEventStreams` 标注器；`FinalAnswerFilterMiddleware` 只复用跟踪规则，不改变原有缓存策略。协议和 UI 适配器显式启用标注器，始终以 `AgentResultEvent` 为权威结果，并把处置事件限制为展示生命周期信号。

**Tech Stack:** Java 17、Project Reactor、JUnit 6、Reactor Test、Jackson、AgentScope Harness、Agent Protocol、AG-UI、Spring WebFlux、React/TypeScript。

**Spec:** `docs/superpowers/specs/2026-09-02-stream-events-text-output-disposition-design.md`

## Global Constraints

- `ReActAgent#streamEvents(...)` 的签名和默认事件数量不得改变。
- Core 能力必须 opt-in，公共入口固定为 `AgentEventStreams.withTextOutputDisposition(Flux<AgentEvent>)`。
- `TERMINAL` 只表示调用结束前最后一个可见回复；`AgentResultEvent` 始终是权威结果。
- 顶层成功顺序固定为 `AgentResultEvent -> TextOutputDispositionEvent(TERMINAL) -> AgentEndEvent`。
- `FinalAnswerFilterMiddleware` 的公共行为不得改变，并与标注器复用内部 `ReplyLifecycleTracker`。
- 来源隔离键固定为 `source + metadata.taskId`；子 Agent 每次调用必须附加唯一 `AgentEvent.METADATA_TASK_ID`。
- Remote 必须复用 `AGENT_EVENT`，`payload` 继续保持 JSON 字符串；不得新增 `RemoteEventType`。
- AG-UI 中间文本不得伪装成 Reasoning；最终完整消息使用标准 `MESSAGES_SNAPSHOT`。
- Web 中间文本只通过 preview 展示，不写入持久化事件；权威结果在 `AgentEndEvent` 边界提交。
- 本计划不处理既有嵌套 `source` 路径覆盖问题，不重构子 Agent `call()`/emitter 架构。
- Windows 基线中 `DangerousPathBypassTest` 的 2 个 symlink 用例因当前账号缺少 `SeCreateSymbolicLinkPrivilege` 报错；功能回归必须单独执行，Linux/具备符号链接权限的环境再跑完整 Core 套件。

---

### Task 1: Core 事件模型与 JSON 兼容

**Files:**
- Create: `agentscope-core/src/main/java/io/agentscope/core/event/TextOutputDisposition.java`
- Create: `agentscope-core/src/main/java/io/agentscope/core/event/TextOutputDispositionEvent.java`
- Modify: `agentscope-core/src/main/java/io/agentscope/core/event/AgentEventType.java`
- Modify: `agentscope-core/src/main/java/io/agentscope/core/event/AgentEvent.java`
- Test: `agentscope-core/src/test/java/io/agentscope/core/event/TextOutputDispositionEventTest.java`

**Interfaces:**
- Produces: `TextOutputDisposition.INTERMEDIATE`, `TextOutputDisposition.TERMINAL`。
- Produces: `TextOutputDispositionEvent(String replyId, TextOutputDisposition disposition, GenerateReason generateReason)`。
- Produces: JSON subtype 名称 `TEXT_OUTPUT_DISPOSITION`。

- [ ] **Step 1: 写事件构造、字段与 JSON round-trip 的失败测试**

```java
@Test
void jsonRoundTripPreservesDispositionAndInheritedFields() {
    TextOutputDispositionEvent event =
            new TextOutputDispositionEvent(
                    "reply-1", TextOutputDisposition.TERMINAL, GenerateReason.MODEL_STOP);
    event.withSource("parent/researcher")
            .withMetadataEntry(AgentEvent.METADATA_TASK_ID, "task-1");

    String json = JsonUtils.toJson(event);
    AgentEvent decoded = JsonUtils.fromJson(json, AgentEvent.class);

    TextOutputDispositionEvent restored =
            assertInstanceOf(TextOutputDispositionEvent.class, decoded);
    assertEquals(AgentEventType.TEXT_OUTPUT_DISPOSITION, restored.getType());
    assertEquals("reply-1", restored.getReplyId());
    assertEquals(TextOutputDisposition.TERMINAL, restored.getDisposition());
    assertEquals(GenerateReason.MODEL_STOP, restored.getGenerateReason());
    assertEquals("parent/researcher", restored.getSource());
    assertEquals("task-1", restored.getMetadata().get(AgentEvent.METADATA_TASK_ID));
}
```

- [ ] **Step 2: 运行测试并确认 subtype 尚不存在**

Run: `mvn -pl agentscope-core -Dtest=TextOutputDispositionEventTest test`

Expected: FAIL，编译器报告 `TextOutputDispositionEvent` / `TEXT_OUTPUT_DISPOSITION` 不存在。

- [ ] **Step 3: 实现枚举、事件和 Jackson subtype**

```java
public enum TextOutputDisposition {
    INTERMEDIATE,
    TERMINAL
}

public final class TextOutputDispositionEvent extends AgentEvent {
    private final String replyId;
    private final TextOutputDisposition disposition;
    private final GenerateReason generateReason;

    public TextOutputDispositionEvent(
            String replyId,
            TextOutputDisposition disposition,
            GenerateReason generateReason) {
        this.replyId = replyId;
        this.disposition = Objects.requireNonNull(disposition, "disposition");
        this.generateReason = generateReason;
    }

    @JsonCreator
    public TextOutputDispositionEvent(
            @JsonProperty("id") String id,
            @JsonProperty("createdAt") String createdAt,
            @JsonProperty("replyId") String replyId,
            @JsonProperty("disposition") TextOutputDisposition disposition,
            @JsonProperty("generateReason") GenerateReason generateReason) {
        super(id, createdAt);
        this.replyId = replyId;
        this.disposition = Objects.requireNonNull(disposition, "disposition");
        this.generateReason = generateReason;
    }

    @Override
    public AgentEventType getType() {
        return AgentEventType.TEXT_OUTPUT_DISPOSITION;
    }
}
```

在 `AgentEvent` 上注册 `@JsonSubTypes.Type(value = TextOutputDispositionEvent.class, name = "TEXT_OUTPUT_DISPOSITION")`，并在 `AgentEventType` 增加同名 canonical value。

- [ ] **Step 4: 运行事件测试**

Run: `mvn -pl agentscope-core -Dtest=TextOutputDispositionEventTest,EventTest test`

Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add agentscope-core/src/main/java/io/agentscope/core/event agentscope-core/src/test/java/io/agentscope/core/event/TextOutputDispositionEventTest.java
git commit -m "feat(core): 新增文本输出处置事件"
```

### Task 2: 统一回复生命周期跟踪器

**Files:**
- Create: `agentscope-core/src/main/java/io/agentscope/core/internal/stream/ReplyLifecycleTracker.java`
- Create: `agentscope-core/src/test/java/io/agentscope/core/internal/stream/ReplyLifecycleTrackerTest.java`
- Modify: `agentscope-core/src/main/java/io/agentscope/core/middleware/FinalAnswerFilterMiddleware.java`
- Modify: `agentscope-core/src/test/java/io/agentscope/core/middleware/FinalAnswerFilterMiddlewareTest.java`

**Interfaces:**
- Produces 位于明确 internal 包中的 `ReplyLifecycleTracker`，按 `source + taskId` 生成 `SourceKey`；类型跨包可见但不属于稳定公共 API。
- Produces `Observation observe(AgentEvent event)`，至少返回 `replyId`、`textSeen`、`toolCallSeen`、`textBecameIntermediate` 和来源键。
- Consumes Task 1 的事件类型，但跟踪器本身不发送事件、不缓存文本内容。

- [ ] **Step 1: 写来源隔离和 reply/tool 关联失败测试**

```java
@Test
void isolatesConcurrentSameSourceByTaskId() {
    ReplyLifecycleTracker tracker = new ReplyLifecycleTracker();
    AgentEvent taskA = new ModelCallStartEvent("reply-a").withSource("parent/worker")
            .withMetadataEntry(AgentEvent.METADATA_TASK_ID, "task-a");
    AgentEvent taskB = new ModelCallStartEvent("reply-b").withSource("parent/worker")
            .withMetadataEntry(AgentEvent.METADATA_TASK_ID, "task-b");

    tracker.observe(taskA);
    tracker.observe(taskB);

    assertNotEquals(tracker.sourceKey(taskA), tracker.sourceKey(taskB));
}
```

同时覆盖 `TextBlockDeltaEvent` 仅在 delta 非空时设置 `textSeen`，`ToolCallStartEvent` 只影响相同 replyId。

- [ ] **Step 2: 运行跟踪器测试并确认失败**

Run: `mvn -pl agentscope-core -Dtest=ReplyLifecycleTrackerTest test`

Expected: FAIL，`ReplyLifecycleTracker` 不存在。

- [ ] **Step 3: 实现无公共 API 承诺的跟踪器**

```java
final class ReplyLifecycleTracker {
    record SourceKey(String source, String taskId) {}

    record Observation(
            SourceKey sourceKey,
            String replyId,
            boolean textSeen,
            boolean toolCallSeen,
            boolean textBecameIntermediate) {}

    private final Map<SourceKey, ReplyState> states = new LinkedHashMap<>();

    SourceKey sourceKey(AgentEvent event) {
        Object taskId = event.getMetadata() == null
                ? null
                : event.getMetadata().get(AgentEvent.METADATA_TASK_ID);
        return new SourceKey(
                event.getSource() == null ? "" : event.getSource(),
                taskId == null ? "" : taskId.toString());
    }
}
```

让 `observe` 统一识别 `ModelCallStartEvent`、三个 TextBlock 事件、`ToolCallStartEvent` 和 `ModelCallEndEvent`，但把“缓存/丢弃/派生事件”留给调用方策略。

- [ ] **Step 4: 将 `FinalAnswerFilterMiddleware.RoundState` 改为组合 tracker**

保留现有 `bufferedTextEvents` 和输出时机；删除其独立 `replyId` / `toolCallSeen` 判断，改用 tracker observation。现有测试必须原样通过，并补一个非当前 reply 的文本仍透传的回归用例。

- [ ] **Step 5: 运行跟踪器与 Middleware 测试**

Run: `mvn -pl agentscope-core -Dtest=ReplyLifecycleTrackerTest,FinalAnswerFilterMiddlewareTest,ReActAgentMiddlewareIntegrationTest test`

Expected: PASS，且 `FinalAnswerFilterMiddleware` 的事件序列不变。

- [ ] **Step 6: 提交**

```bash
git add agentscope-core/src/main/java/io/agentscope/core/internal/stream/ReplyLifecycleTracker.java agentscope-core/src/main/java/io/agentscope/core/middleware/FinalAnswerFilterMiddleware.java agentscope-core/src/test/java/io/agentscope/core/internal/stream/ReplyLifecycleTrackerTest.java agentscope-core/src/test/java/io/agentscope/core/middleware/FinalAnswerFilterMiddlewareTest.java
git commit -m "refactor(core): 统一回复生命周期跟踪规则"
```

### Task 3: 可选流标注器与严格生命周期校验

**Files:**
- Create: `agentscope-core/src/main/java/io/agentscope/core/event/AgentEventStreams.java`
- Create: `agentscope-core/src/test/java/io/agentscope/core/event/AgentEventStreamsTest.java`

**Interfaces:**
- Produces: `public static Flux<AgentEvent> withTextOutputDisposition(Flux<AgentEvent> source)`。
- Consumes: Task 1 的 `TextOutputDispositionEvent`，Task 2 的 `ReplyLifecycleTracker`。
- Guarantee: 每次订阅独立状态；顶层 End 最多暂存一个；每个输入事件最多派生一个 disposition。

- [ ] **Step 1: 写完整状态机的失败测试**

使用 `StepVerifier` 覆盖：文本后工具、工具后文本、无工具再次推理、正常终止、空结果、异常、取消、同 reply 多文本段、并发 source/taskId、End 后追加 Result、逐个 request 背压。

```java
@Test
void emitsResultTerminalThenEndOnNormalCompletion() {
    Msg result = AssistantMessage.builder()
            .content(List.of(TextBlock.builder().text("answer").build()))
            .metadata(Map.of(Msg.METADATA_GENERATE_REASON, GenerateReason.MODEL_STOP))
            .build();

    Flux<AgentEvent> annotated = AgentEventStreams.withTextOutputDisposition(
            Flux.just(
                    new ModelCallStartEvent("reply-1"),
                    new TextBlockDeltaEvent("reply-1", "block-1", "answer"),
                    new TextBlockEndEvent("reply-1", "block-1"),
                    new ModelCallEndEvent("reply-1", null),
                    new AgentResultEvent(result),
                    new AgentEndEvent("reply-1")));

    StepVerifier.create(annotated)
            .expectNextCount(5)
            .assertNext(event -> assertInstanceOf(AgentResultEvent.class, event))
            .assertNext(event -> {
                TextOutputDispositionEvent disposition =
                        assertInstanceOf(TextOutputDispositionEvent.class, event);
                assertEquals(TextOutputDisposition.TERMINAL, disposition.getDisposition());
                assertEquals(GenerateReason.MODEL_STOP, disposition.getGenerateReason());
            })
            .assertNext(event -> assertInstanceOf(AgentEndEvent.class, event))
            .verifyComplete();
}
```

- [ ] **Step 2: 运行标注器测试并确认失败**

Run: `mvn -pl agentscope-core -Dtest=AgentEventStreamsTest test`

Expected: FAIL，`AgentEventStreams` 不存在。

- [ ] **Step 3: 用 `Flux.defer` 和限预取 `concatMap` 实现标注器**

```java
public final class AgentEventStreams {
    private AgentEventStreams() {}

    public static Flux<AgentEvent> withTextOutputDisposition(Flux<AgentEvent> source) {
        Objects.requireNonNull(source, "source");
        return Flux.defer(() -> new DispositionAnnotator().apply(source));
    }
}
```

`DispositionAnnotator` 必须：

- 在新 `ModelCallStartEvent` 前关闭同来源上一轮为 `INTERMEDIATE`；
- 在文本后遇到工具时先发 `INTERMEDIATE` 再发工具事件；
- 工具先出现时在 `TextBlockEndEvent` 后发 `INTERMEDIATE`；
- 原样立即转发 `AgentResultEvent`，只保存同来源最后一个 Result；
- 暂存顶层 `AgentEndEvent`，仅在源正常完成时发送 `TERMINAL` 和 End；
- End 后出现任何同来源生命周期事件时抛 `IllegalStateException`，且不泄漏暂存 End/TERMINAL；
- error/cancel/无 Result 的顶层 End 不产生 `TERMINAL`；
- 子来源 End 可产生 `TERMINAL(generateReason=null)`。

- [ ] **Step 4: 运行全部 Core 目标测试**

Run: `mvn -pl agentscope-core -Dtest=AgentEventStreamsTest,TextOutputDispositionEventTest,ReplyLifecycleTrackerTest,FinalAnswerFilterMiddlewareTest,AgentStreamingTest test`

Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add agentscope-core/src/main/java/io/agentscope/core/event/AgentEventStreams.java agentscope-core/src/test/java/io/agentscope/core/event/AgentEventStreamsTest.java
git commit -m "feat(core): 增加流式文本处置标注器"
```

### Task 4: Harness 子 Agent 每次调用的 taskId 传播

**Files:**
- Modify: `agentscope-harness/src/main/java/io/agentscope/harness/agent/tool/AgentSpawnTool.java`
- Modify: `agentscope-harness/src/main/java/io/agentscope/harness/agent/subagent/protocol/RemoteEventCodec.java`
- Test: `agentscope-harness/src/test/java/io/agentscope/harness/agent/HarnessAgentSubagentStreamEventsTest.java`
- Test: `agentscope-harness/src/test/java/io/agentscope/harness/agent/tool/AgentSpawnToolRemoteHelpersTest.java`

**Interfaces:**
- Consumes: `TaskRecord#getTaskId()`。
- Produces: 子 Agent 原始事件及合成 `AgentStartEvent` / `AgentEndEvent` 都含 `metadata.taskId`。

- [ ] **Step 1: 写相同 source 并发调用仍有不同 taskId 的失败测试**

```java
assertTrue(childEvents.stream().allMatch(event ->
        taskId.equals(event.getMetadata().get(AgentEvent.METADATA_TASK_ID))));
```

集成测试必须使用真实 `AgentSpawnTool + ReActAgent` 事件转发，而不是只测 helper。

- [ ] **Step 2: 运行 Harness 目标测试并确认失败**

Run: `mvn -pl agentscope-harness -am -Dtest=HarnessAgentSubagentStreamEventsTest,AgentSpawnToolRemoteHelpersTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: FAIL，合成事件或模型事件缺少 `taskId`。

- [ ] **Step 3: 在本地和远程转发入口统一附加 taskId**

```java
private AgentEvent tagForwardedEvent(AgentEvent event, String sourcePath, String taskId) {
    return event.withSource(sourcePath)
            .withMetadataEntry(AgentEvent.METADATA_TASK_ID, taskId);
}
```

调用该 helper 处理 tagged emitter、合成 Start/End 和 `RemoteEventCodec.toAgentEvent(...)` 恢复后的事件。不要改变既有 source 拼接策略。

- [ ] **Step 4: 运行 Harness 流转发测试**

Run: `mvn -pl agentscope-harness -am -Dtest=HarnessAgentSubagentStreamEventsTest,AgentSpawnToolRemoteHelpersTest,RemoteEventCodecTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add agentscope-harness/src/main/java/io/agentscope/harness/agent/tool/AgentSpawnTool.java agentscope-harness/src/main/java/io/agentscope/harness/agent/subagent/protocol/RemoteEventCodec.java agentscope-harness/src/test/java/io/agentscope/harness/agent/HarnessAgentSubagentStreamEventsTest.java agentscope-harness/src/test/java/io/agentscope/harness/agent/tool/AgentSpawnToolRemoteHelpersTest.java
git commit -m "fix(harness): 为子代理流事件附加任务标识"
```

### Task 5: Remote 与 Agent Protocol 透传处置和权威结果

**Files:**
- Modify: `agentscope-harness/src/main/java/io/agentscope/harness/agent/subagent/protocol/RemoteEventCodec.java`
- Modify: `agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agent-protocol/src/main/java/io/agentscope/extensions/agentprotocol/AgentProtocolTaskStore.java`
- Test: `agentscope-harness/src/test/java/io/agentscope/harness/agent/subagent/protocol/RemoteEventCodecTest.java`
- Test: `agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agent-protocol/src/test/java/io/agentscope/extensions/agentprotocol/AgentProtocolStreamDetailTest.java`

**Interfaces:**
- Produces: `RemoteAgentEvent.type == AGENT_EVENT`、`eventType == TEXT_OUTPUT_DISPOSITION`、`payload` 为 JSON 字符串。
- Changes: `detail=full` 放行 `TEXT_OUTPUT_DISPOSITION` 和 `AGENT_RESULT`；其他通用 `AGENT_EVENT` 仍要求 verbose。

- [ ] **Step 1: 写 codec round-trip 与 detail 过滤失败测试**

```java
RemoteAgentEvent remote = RemoteEventCodec.fromAgentEvent(
        new TextOutputDispositionEvent(
                "reply-1", TextOutputDisposition.INTERMEDIATE, null));
assertEquals(RemoteEventType.AGENT_EVENT, remote.getType());
assertEquals("TEXT_OUTPUT_DISPOSITION", remote.getEventType());
assertInstanceOf(String.class, remote.getPayload());
assertInstanceOf(TextOutputDispositionEvent.class,
        RemoteEventCodec.toAgentEvent(remote).orElseThrow());
```

- [ ] **Step 2: 运行协议测试并确认 full 尚未放行**

Run: `mvn -pl agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agent-protocol -am -Dtest=RemoteEventCodecTest,AgentProtocolStreamDetailTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: FAIL，full 流中缺少 disposition/result。

- [ ] **Step 3: 更新 codec 和 detail 规则**

保持 payload 字段类型不变；如果 disposition JSON 序列化失败或 payload 为空，记录 warn 并返回空结果，不发送空壳 `AGENT_EVENT`。

- [ ] **Step 4: 运行 Remote/Agent Protocol 测试**

Run: `mvn -pl agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agent-protocol -am -Dtest=RemoteEventCodecTest,RemoteEventCodecPassthroughTest,AgentProtocolStreamDetailTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add agentscope-harness/src/main/java/io/agentscope/harness/agent/subagent/protocol/RemoteEventCodec.java agentscope-harness/src/test/java/io/agentscope/harness/agent/subagent/protocol agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agent-protocol
git commit -m "feat(protocol): 透传文本处置和权威结果事件"
```

### Task 6: AG-UI opt-in 转换、分段消息与快照校准

**Files:**
- Modify: `agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui/src/main/java/io/agentscope/core/agui/adapter/AguiAdapterConfig.java`
- Modify: `agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui/src/main/java/io/agentscope/core/agui/adapter/AguiAgentAdapter.java`
- Modify: `agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui/src/main/java/io/agentscope/core/agui/adapter/strategy/AguiStreamContext.java`
- Create: `agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui/src/main/java/io/agentscope/core/agui/adapter/strategy/TextOutputDispositionConverter.java`
- Modify: `agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui/src/test/java/io/agentscope/core/agui/adapter/AguiAdapterConfigTest.java`
- Modify: `agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui/src/test/java/io/agentscope/core/agui/adapter/AguiAgentAdapterV2Test.java`

**Interfaces:**
- Produces config: `boolean textOutputDispositionEnabled`，默认 `false`。
- Produces Custom event name: `agentscope.text_output.disposition`。
- Produces text message IDs: `<replyId>:text:<zeroBasedIndex>`。
- Produces allowed final results as standard `MESSAGES_SNAPSHOT` before RunFinished。

- [ ] **Step 1: 写默认关闭、启用后 Custom 事件、多段 ID 和 snapshot 的失败测试**

断言关闭时现有事件序列完全不变；启用时对 `INTERMEDIATE` 输出：

```json
{
  "name": "agentscope.text_output.disposition",
  "value": {
    "replyId": "reply-1",
    "messageIds": ["reply-1:text:0", "reply-1:text:1"],
    "disposition": "INTERMEDIATE",
    "generateReason": null
  }
}
```

- [ ] **Step 2: 运行 AG-UI 测试并确认失败**

Run: `mvn -pl agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui -am -Dtest=AguiAdapterConfigTest,AguiAgentAdapterV2Test -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: FAIL，配置和 converter 不存在。

- [ ] **Step 3: 在 Adapter 入口 opt-in 包装同一条 Agent 流**

```java
Flux<AgentEvent> events = agent.streamEvents(messages, runtimeContext);
if (config.isTextOutputDispositionEnabled()) {
    events = AgentEventStreams.withTextOutputDisposition(events);
}
```

`AguiStreamContext` 按 replyId 保存全部文本段 ID；处置 converter 只输出 Custom event，不生成 Reasoning。正常完成原因仅允许 `MODEL_STOP`、`STRUCTURED_OUTPUT`、`MAX_ITERATIONS` 生成 `MESSAGES_SNAPSHOT`；暂停/权限/工具挂起沿用现有 interrupt 分支。

- [ ] **Step 4: 运行 AG-UI 模块测试**

Run: `mvn -pl agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui -am test -DskipITs`

Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui
git commit -m "feat(agui): 支持实时文本处置与结果快照"
```

### Task 7: Web 数据面 preview update 与 AgentEnd 提交

**Files:**
- Modify: `agentscope-service/service-common/src/main/java/io/agentscope/builder/web/managed/SessionEventTypes.java`
- Modify: `agentscope-service/service-dataplane/src/main/java/io/agentscope/builder/web/managed/SessionEventMapper.java`
- Modify: `agentscope-service/service-dataplane/src/main/java/io/agentscope/builder/web/managed/SessionEventPreviewBus.java`
- Modify: `agentscope-service/service-dataplane/src/main/java/io/agentscope/builder/web/managed/SessionTurnRunner.java`
- Modify: `agentscope-service/service-dataplane/src/main/java/io/agentscope/builder/web/api/DataSessionApiController.java`
- Modify: `agentscope-service/service-dataplane/src/test/java/io/agentscope/builder/web/managed/SessionEventMapperTest.java`

**Interfaces:**
- Produces: `EVENT_UPDATE = "event_update"`。
- Changes `PreviewFrame` to `(streamType, targetType, eventId, delta, attributes)`，保留四参数构造器。
- Produces: `SessionEventPreviewBus.emitUpdate(sessionId, targetType, eventId, attributes)`。
- Consumes: `AgentEventStreams.withTextOutputDisposition(agent.streamEvents(...))`。

- [ ] **Step 1: 写 update 传输、Result 暂存和 End 提交失败测试**

覆盖：INTERMEDIATE 不持久化、TERMINAL 只关闭 preview、正常 Result 在 End 时提交、structured-only 结果生成新 eventId、权威空结果发送 `authoritative=true/hasOutput=false` 的 update、暂停原因不提交普通 agent.message。

- [ ] **Step 2: 运行 Web mapper 测试并确认失败**

Run: `mvn -pl agentscope-service/service-dataplane -am -Dtest=SessionEventMapperTest -Dsurefire.failIfNoSpecifiedTests=false test`

Expected: FAIL，`event_update` 和处置映射不存在。

- [ ] **Step 3: 扩展 PreviewFrame 和 PreviewBus**

```java
public record PreviewFrame(
        String streamType,
        String targetType,
        String eventId,
        String delta,
        Map<String, Object> attributes) {
    public PreviewFrame(String streamType, String targetType, String eventId, String delta) {
        this(streamType, targetType, eventId, delta, Map.of());
    }
}
```

SSE controller 必须显式派发 `event_start`、`event_delta`、`event_update`，attributes 原样进入 JSON frame。

- [ ] **Step 4: 调整 `SessionTurnRunner` 与 mapper 状态边界**

在 runner 中 opt-in 包装 Agent 流。mapper 按顶层来源暂存最后一个 `AgentResultEvent`，维护 replyId 到 previewId 的映射；只在 `AgentEndEvent` 根据 `GenerateReason` 生成持久化 MappingResult 或权威空 update。持久化 payload 同时保留 `text` 和完整 `content` / metadata / generateReason / structured output。

- [ ] **Step 5: 运行 Web 数据面测试**

Run: `mvn -pl agentscope-service/service-dataplane -am test -DskipITs`

Expected: PASS。

- [ ] **Step 6: 提交**

```bash
git add agentscope-service/service-common/src/main/java/io/agentscope/builder/web/managed/SessionEventTypes.java agentscope-service/service-dataplane
git commit -m "feat(web): 增加文本处置预览与结果校准"
```

### Task 8: Web 前端四类显示状态

**Files:**
- Modify: `agentscope-service/frontend/src/api/managedSessions.ts`
- Modify: `agentscope-service/frontend/src/components/ChatPanel.tsx`
- Create: `agentscope-service/frontend/src/components/ChatPanel.test.tsx`

**Interfaces:**
- Consumes SSE: `event_start`、`event_delta`、`event_update`。
- State: `pendingSegments`、`commentarySegments`、`thinkingSegments`、`toolExecutions`、`finalAnswer`。
- Rule: Commentary 仅当前页面内存展示，刷新不恢复。

- [ ] **Step 1: 写前端状态迁移失败测试**

```typescript
it('moves intermediate preview to commentary and confirms only authoritative result', () => {
  applyFrame(startFrame('preview-1'));
  applyFrame(deltaFrame('preview-1', 'working'));
  applyFrame(updateFrame('preview-1', { disposition: 'INTERMEDIATE' }));
  expect(state.commentarySegments['preview-1'].text).toBe('working');
  expect(state.finalAnswer).toBeUndefined();

  applyFrame(persistedAgentMessage('preview-2', 'answer'));
  expect(state.finalAnswer?.text).toBe('answer');
});
```

再覆盖 `authoritative=true, hasOutput=false` 删除 pending preview，且不渲染 `[agent response]`。

- [ ] **Step 2: 运行前端测试并确认失败**

Run: `npm test -- --run ChatPanel.test.tsx`

Workdir: `agentscope-service/frontend`

Expected: FAIL，客户端不识别 `event_update`。

- [ ] **Step 3: 扩展 SSE 类型和 reducer**

`managedSessions.ts` 为 update frame 增加 `attributes?: Record<string, unknown>`；`ChatPanel.tsx` 将处置 update 转换为 pending/commentary 状态迁移，只有持久化的权威 `agent.message` 更新 finalAnswer。

- [ ] **Step 4: 运行前端测试和构建**

Run: `npm test -- --run && npm run build`

Workdir: `agentscope-service/frontend`

Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add agentscope-service/frontend/src/api/managedSessions.ts agentscope-service/frontend/src/components/ChatPanel.tsx agentscope-service/frontend/src/components/ChatPanel.test.tsx
git commit -m "feat(frontend): 区分过程文本与权威最终结果"
```

### Task 9: 文档、兼容性与全量验证

**Files:**
- Modify: `agentscope-examples/documentation/src/main/java/io/agentscope/examples/documentation2/streaming/AgentEventStreamExample.java`
- Modify: `agentscope-service/docs/managed_agents/guide/07-events.md`
- Modify: `agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui/README.md`

**Interfaces:**
- Documents: Core opt-in 用法、`TERMINAL != final answer`、Remote payload 兼容、AG-UI/Web 启用方式。

- [ ] **Step 1: 更新示例和协议文档**

示例必须使用：

```java
AgentEventStreams.withTextOutputDisposition(agent.streamEvents(input))
        .doOnNext(event -> {
            if (event instanceof TextOutputDispositionEvent disposition) {
                System.out.printf("%s -> %s%n",
                        disposition.getReplyId(), disposition.getDisposition());
            }
        })
        .blockLast();
```

- [ ] **Step 2: 运行格式检查**

Run: `mvn spotless:check -DskipTests`

Expected: PASS；如失败，执行 `mvn spotless:apply -DskipTests` 后重新检查，并只保留本计划相关格式变化。

- [ ] **Step 3: 运行分模块回归**

```bash
mvn -pl agentscope-core test -DskipITs -Dtest='!DangerousPathBypassTest'
mvn -pl agentscope-harness -am test -DskipITs
mvn -pl agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agent-protocol -am test -DskipITs
mvn -pl agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui -am test -DskipITs
mvn -pl agentscope-service/service-dataplane -am test -DskipITs
```

Expected: PASS。Windows 上另行记录 `DangerousPathBypassTest` 两个 symlink 用例的环境性失败；在 Linux 或启用 Windows Developer Mode/符号链接权限的环境运行完整 Core 测试。

- [ ] **Step 4: 运行前端回归**

Run: `npm test -- --run && npm run build`

Workdir: `agentscope-service/frontend`

Expected: PASS。

- [ ] **Step 5: 验证默认兼容性**

确认未启用标注时：

- `ReActAgent#streamEvents()` 的既有事件数量断言不变；
- AG-UI 旧配置的 messageId 与事件序列不变；
- Remote `payload` 仍为字符串，旧客户端可忽略未知 subtype；
- `FinalAnswerFilterMiddleware` 的缓存和过滤行为不变。

- [ ] **Step 6: 提交文档与验证修正**

```bash
git add agentscope-examples/documentation/src/main/java/io/agentscope/examples/documentation2/streaming/AgentEventStreamExample.java agentscope-service/docs/managed_agents/guide/07-events.md agentscope-extensions/agentscope-extensions-protocol/agentscope-extensions-agui/README.md
git commit -m "docs(streaming): 说明文本处置与结果校准用法"
```
