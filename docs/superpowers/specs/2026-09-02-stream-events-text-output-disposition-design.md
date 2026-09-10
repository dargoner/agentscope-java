# `streamEvents` 实时文本处置语义设计

## 1. 背景

`ReActAgent#streamEvents(...)` 是 AgentScope Java 的统一细粒度流式入口。它实时发送模型文本、Thinking、工具调用、工具结果、HITL 和 Agent 生命周期事件，并通过 `AgentResultEvent` 交付本次调用的结果 `Msg`。

官方主干已经引入 `FinalAnswerFilterMiddleware`。该中间件缓存一轮模型文本：同轮出现工具调用时丢弃文本，否则在 `ModelCallEndEvent` 前释放文本。它适合只接受最终轮文本且能容忍首 Token 延迟的渠道，但不能同时满足 WebUI 和 AG-UI 对真实流式与过程分区的要求。

交互式客户端需要：

- 文本 Token 到达后立即展示；
- 区分模型 Thinking、工具执行、中间文本和终止轮文本；
- 不增加第二套 Agent 执行 API；
- 保持现有 `streamEvents()` 默认行为和远程旧客户端兼容；
- 以完整 `AgentResultEvent` 作为权威结果完成校准。

## 2. 目标

本次改造实现以下能力：

1. `streamEvents()` 仍是唯一 Agent 执行流入口。
2. 提供可选的文本输出处置标注，将已实时发送的模型回复标记为 `INTERMEDIATE` 或 `TERMINAL`。
3. `TERMINAL` 只表达生命周期位置，不声称流式文本等于最终 `Msg`。
4. AG-UI、Web 数据面和远程 Agent 可以消费同一类处置信号。
5. 以最终 `AgentResultEvent` 的完整消息内容和 `GenerateReason` 决定是否形成用户最终答案。
6. 中间过程文本只实时展示，不写入持久化事件日志。

## 3. 非目标

本次改造不包括：

- 不新增 `streamOutput()` 或 `streamConversation()`。
- 不把 `streamEvents()` 改成仅包含面向 UI 的事件。
- 不默认改变所有 `streamEvents()` 调用的事件数量。
- 不删除或改变 `FinalAnswerFilterMiddleware` 的公共行为。
- 不把过程文本映射为模型 Thinking。
- 不持久化中间过程文本，也不新增 `agent.commentary` 历史类型。
- 不把 `TERMINAL` 解释为“最终答案内容已经确认”。
- 不在本次改造中重构本地子 Agent 的 `call()`/emitter 转发架构。
- 不顺带修复既有的嵌套 `source` 路径组合问题。

## 4. 核心判断

### 4.1 为什么不能只看 `ModelCallEndEvent`

`ModelCallEndEvent` 只表示一次模型调用结束。此后仍可能发生：

- PostReasoning Hook 请求 `gotoReasoning`；
- Structured Output 强制重试；
- 空文本或 Thinking-only 响应触发再次推理；
- Middleware 或 HITL 改变终止结果。

所以“模型调用结束且没有工具调用”不能作为通用最终答案判断。

### 4.2 为什么使用 `TERMINAL` 而不是 `FINAL_ANSWER`

最终 `Msg` 可能经过 PostReasoning、PostCall、Structured Output 或 Middleware 修改，也可能代表权限确认、工具暂停或中断。最后一轮实时文本只能证明它是本次调用结束前最后一个可见模型回复，不能证明它与最终消息逐字一致，也不能证明调用已经完成业务任务。

因此处置语义定义为：

```text
INTERMEDIATE  执行继续进入工具或下一次模型调用
TERMINAL      该文本是本次 Agent 调用结束前最后一个可见模型回复
```

`AgentResultEvent` 仍是唯一权威结果。适配器结合其完整 `Msg` 与 `GenerateReason` 决定最终展示、暂停状态和持久化行为。

## 5. 公共使用方式

`streamEvents()` 签名和默认输出保持不变。调用方在同一条事件流上显式增加标注：

```java
Flux<AgentEvent> events =
        AgentEventStreams.withTextOutputDisposition(agent.streamEvents(input));
```

该操作不会再次执行 Agent，也不是第二套流式 API；它是对现有 `Flux<AgentEvent>` 的可选、有序标注。

AG-UI 和 Web 数据面在启用对应能力时内部应用该操作，业务调用方不需要各自实现状态机。Core 初始 PR 保持 opt-in，以避免在 `2.0.x` patch 版本默认改变事件数量、JSON subtype 和严格序列断言。

## 6. 新增 Core 类型

新增枚举：

```java
public enum TextOutputDisposition {
    INTERMEDIATE,
    TERMINAL
}
```

新增事件：

```java
public final class TextOutputDispositionEvent extends AgentEvent {

    private final String replyId;
    private final TextOutputDisposition disposition;

    // 仅 TERMINAL 且存在 AgentResultEvent 时设置。
    private final GenerateReason generateReason;
}
```

并增加：

```java
AgentEventType.TEXT_OUTPUT_DISPOSITION
```

事件必须注册到 `AgentEvent.@JsonSubTypes`，提供包含 `id`、`createdAt` 的 `@JsonCreator`，并通过继承字段保留 `source` 与 metadata。

处置作用域是整个模型回复，而不是单个 TextBlock。事件使用现有 `replyId` 关联同一模型调用中的全部可见文本，不增加 `conversationId` 或新的 Core segment ID。

## 7. 标注组件与 Middleware 边界

新增公共流工具：

```java
public final class AgentEventStreams {

    public static Flux<AgentEvent> withTextOutputDisposition(Flux<AgentEvent> source);
}
```

该工具处理已经离开完整 `onAgent` Middleware 链的事件流，因此：

- 它看到的是 Middleware 最终实际对调用方发送的事件；
- Middleware 删除的文本不会产生处置事件；
- 新增处置事件不再次回流到 `onAgent` Middleware；
- tracing 或 recorder 如果需要记录处置事件，应包裹标注后的 Flux，而不是依赖 Agent 内部 Middleware。

这是明确的“post-middleware stream annotation”边界，不暗示处置事件属于原始执行事件。

标注组件使用 `Flux.defer` 创建订阅级状态，并使用保持顺序、背压感知的逐事件展开。它不承诺处置事件与相邻事件对下游原子可见；客户端不能仅凭 `TERMINAL` 持久化结果。

内部提取一个无公共 API 承诺的 `ReplyLifecycleTracker`，复用 replyId、文本和工具关联规则。`FinalAnswerFilterMiddleware` 与 disposition annotator 必须复用该 tracker，不保留两套独立的 reply/tool 关联状态机；两者只在“何时释放文本”和“何时产生生命周期处置”上保留不同策略。

## 8. 状态模型

标注器按事件来源维护状态。来源键由以下字段组成：

```text
source + metadata.taskId
```

顶层 Agent 使用空来源键。所有本地和远程子 Agent 转发路径必须把每次调用唯一的 TaskRecord ID 写入 `AgentEvent.METADATA_TASK_ID`，并同时附加到模型事件及合成的 AgentStart/AgentEnd。子任务按 `source + taskId` 隔离；source 只作为展示来源，不能单独承担并发调用关联。

每个来源保存：

```java
final class ReplyState {
    String currentReplyId;
    boolean textSeen;
    boolean toolCallSeen;
    boolean dispositionEmitted;
    AgentResultEvent lastResult;
}
```

标注器不缓存文本内容。多次订阅、重试订阅和并发订阅之间不存在共享状态。

## 9. 状态机

### 9.1 `ModelCallStartEvent`

同一来源的上一轮存在未处置文本时，先发送：

```text
TextOutputDispositionEvent(INTERMEDIATE)
```

然后转发新的模型调用开始事件并开启新的 reply 状态。该规则覆盖 PostReasoning `gotoReasoning`、Structured Output 重试和其他无工具的再次推理。

### 9.2 `TextBlockDeltaEvent`

Delta 立即原样转发，并记录对应 reply 已产生非空可见文本。标注器不等待处置结果，也不拼接文本。

### 9.3 `ToolCallStartEvent`

记录对应 reply 出现工具调用。

- 文本已经出现时，先发送 `INTERMEDIATE`，再转发工具开始事件。
- 工具先于文本出现时，只记录工具状态；文本结束后再发 `INTERMEDIATE`。
- 同一 reply 多个工具只发送一次处置事件。

### 9.4 `TextBlockEndEvent`

先转发原始 End。若工具先出现、文本随后出现且该 reply 尚未处置，则在 End 后发送 `INTERMEDIATE`。

同一个 reply 可以出现“文本 -> 工具 -> 文本”。处置作用于整个 reply：首次 `INTERMEDIATE` 后仍保留该 reply 状态直到 `ModelCallEndEvent`，后续文本继续属于同一个中间回复，不再次发送处置事件。

### 9.5 `ModelCallEndEvent`

只转发模型调用结束事件。若该 reply 已处置则清理其临时 block 状态；未处置文本继续等待下一次模型调用或 Agent 终止信号。

### 9.6 `AgentResultEvent`

事件立即原样转发，并作为同一来源最近一次权威结果保存。标注器此时不发送 `TERMINAL`，从而允许完整 Middleware 输出中存在结果替换或追加时，以 Agent 结束前最后一个 Result 为准。

### 9.7 `AgentEndEvent`

顶层来源：收到 `AgentEndEvent` 时先暂存，不立即向下游发送。若随后出现任何事件，说明 `onAgent` Middleware 破坏了顶层生命周期契约，标注器直接报错，并且不得先发送 `TERMINAL` 或 End。只有源 Flux 随后正常完成时，才根据最后一个 Result 发送可选的 `TERMINAL`，再发送暂存的 `AgentEndEvent` 并完成流。`generateReason` 来自 End 前最后一个结果消息。

子 Agent 来源：现有本地 emitter 路径不会把子 Agent 的 `AgentResultEvent` 转发给父流，但会发送带 source 的合成 `AgentEndEvent`。因此对非顶层来源，成功 End 可以关闭最后一个未处置文本并产生 `TERMINAL`，`generateReason` 为空。该语义只表示子调用结束，不表示父 Agent 的最终答案。

标注器要求每个来源的 `AgentEndEvent` 是该来源最后一个生命周期事件。顶层 End 通过“暂存至源完成”保证错误处置不会提前可见。若 End 后再次出现 Result、Model、Text 或 Tool 事件，标注器以明确异常终止。Core 测试必须覆盖 `next.apply(input).concatWith(Result)` 这一反例，并断言下游没有收到 `TERMINAL` 和 End。

### 9.8 异常与取消

`onError`、订阅取消或顶层只有 `AgentEndEvent` 而没有 `AgentResultEvent` 时，不产生 `TERMINAL`。如果源在暂存顶层 End 后报错或取消，暂存 End 也不发送。标注器不将 Reactor 错误转换为普通事件。

## 10. 典型事件顺序

### 10.1 文本后调用工具

```text
ModelCallStart
TextBlockStart
TextBlockDelta...
TextBlockEnd
TextOutputDisposition(INTERMEDIATE)
ToolCallStart
ModelCallEnd
```

### 10.2 无工具但再次推理

```text
第一轮 TextBlockEnd
第一轮 ModelCallEnd
TextOutputDisposition(INTERMEDIATE)
第二轮 ModelCallStart
```

### 10.3 正常终止

```text
ModelCallStart
TextBlockStart
TextBlockDelta...
TextBlockEnd
ModelCallEnd
AgentResultEvent
TextOutputDisposition(TERMINAL, MODEL_STOP)
AgentEndEvent
```

`AgentResultEvent` 先于 `TERMINAL`，确保即使下游在处置事件后取消，也已经获得权威结果。二者仍不是原子交付。

## 11. `GenerateReason` 与最终答案策略

`TERMINAL` 只关闭最后一个实时文本回复。是否形成用户最终答案由适配器结合 `GenerateReason` 判断。

默认可形成最终输出的原因：

```text
MODEL_STOP
STRUCTURED_OUTPUT
MAX_ITERATIONS
```

以下原因不得无条件持久化为普通 `agent.message`：

```text
TOOL_SUSPENDED
PERMISSION_ASKING
MIDDLEWARE_STOP_REQUESTED
INTERRUPTED
```

`REASONING_STOP_REQUESTED`、`ACTING_STOP_REQUESTED` 和 `ALL_TOOLS_DENIED` 继续沿用各协议现有的停止/状态策略；本次改造不将它们自动解释为最终答案。

## 12. 与 `FinalAnswerFilterMiddleware` 的关系

两种机制服务不同渠道：

| 场景 | 使用方式 |
| --- | --- |
| WebUI、AG-UI、交互式聊天 | 实时 Delta + opt-in disposition 标注 |
| Webhook、通知、只接受无工具轮文本的渠道 | `FinalAnswerFilterMiddleware` |
| 获取权威调用结果 | `AgentResultEvent` |

`FinalAnswerFilterMiddleware` 当前按同轮是否出现工具调用决定文本释放。Disposition 依据完整流生命周期，因此可能将该中间件已经释放的文本标成 `INTERMEDIATE`，例如 PostReasoning 或 Structured Output 随后要求重试。

Core PR 可以让两者复用内部 `ReplyLifecycleTracker`，但不修改 `FinalAnswerFilterMiddleware` 的公共行为，也不把两个不同语义强行合并成同一模式。

## 13. AG-UI 适配

### 13.1 能力启用

AG-UI 通过适配器配置显式启用 disposition。关闭时沿用现有转换，不改变消息 ID 和事件数量。启用时，适配器对 `streamEvents()` 应用统一标注工具。

### 13.2 多文本段

现有 AG-UI 上下文以 replyId 作为 messageId，无法正确表示同一 reply 中“文本 -> 工具 -> 文本”的两次文本生命周期。启用 disposition 后，AG-UI 为每次 TextBlock Start 分配独立 ID：

```text
<replyId>:text:0
<replyId>:text:1
```

标准 Start/Content/End 事件使用该段 ID。上下文记录一个 reply 下的全部 messageId。

处置 Custom Event 携带：

```json
{
  "name": "agentscope.text_output.disposition",
  "value": {
    "replyId": "reply-123",
    "messageIds": ["reply-123:text:0", "reply-123:text:1"],
    "disposition": "INTERMEDIATE",
    "generateReason": null
  }
}
```

过程说明不能映射为 AG-UI Reasoning；模型 Thinking 和中间文本保持独立语义。

### 13.3 权威结果

不新增只携带纯文本的 `agentscope.output.completed`。对于允许形成最终输出的 `GenerateReason`，适配器复用 AG-UI 标准 `MESSAGES_SNAPSHOT`，通过现有 `AguiMessageConverter` 投影完整结果消息，并按 AG-UI 标准顺序发送快照与 RunFinished。

快照不是只包含最后一个 Result。它由本次 `RunAgentInput` 中的权威会话消息加最终 Result 构成，按消息 ID 去重，并明确排除仅通过实时 TextBlock 事件展示的 intermediate 文本段，避免快照覆盖或污染客户端既有会话。

AgentScope 特有且 AG-UI Message 无法表达的 metadata 可以放入 `MessagesSnapshot.rawEvent`，但不得用纯文本替代完整消息。暂停、权限询问和工具挂起继续走现有 Interrupt/状态协议，不发送普通最终答案快照。

## 14. 远程 Agent 与 AgentProtocol

远程 Wire 层不增加新的 `RemoteEventType`。新增远程枚举会使旧 Java 客户端反序列化未知值时失败。

Disposition 复用已有 `AGENT_EVENT`，其中 `payload` 保持现有字符串类型：

```json
{
  "type": "AGENT_EVENT",
  "eventType": "TEXT_OUTPUT_DISPOSITION",
  "payload": "{\"type\":\"TEXT_OUTPUT_DISPOSITION\",\"replyId\":\"reply-123\",\"disposition\":\"INTERMEDIATE\"}"
}
```

新客户端从 payload 恢复强类型事件；旧客户端无法识别 subtype 时安全丢弃。不得把 payload 改成 JSON 对象。

`RemoteEventCodec#matchesDetail(RemoteAgentEvent, String)` 在 `detail=full` 下放行：

```text
TEXT_OUTPUT_DISPOSITION
AGENT_RESULT
```

其他通用 `AGENT_EVENT` 仍要求 `detail=verbose`。这样 full 客户端在收到终止处置时也能获得用于校准的权威 Result。

如果 disposition 序列化失败且 payload 为空，因为没有稳定 flat 字段可恢复，该事件直接丢弃并记录日志，不发送不可消费的空壳事件。

## 15. Web 数据面

### 15.1 Preview 帧协议

扩展 `PreviewFrame`：

```java
record PreviewFrame(
        String streamType,
        String targetType,
        String eventId,
        String delta,
        Map<String, Object> attributes) {

    PreviewFrame(String streamType, String targetType, String eventId, String delta) {
        this(streamType, targetType, eventId, delta, Map.of());
    }
}
```

新增：

```java
EVENT_UPDATE = "event_update";
```

不能只修改 record。`SessionTurnRunner`、`SessionEventPreviewBus` 和 SSE controller 必须增加 Start/Delta/Update 三分支派发及 `emitUpdate`，确保 attributes 实际到达客户端且不写入数据库。

### 15.2 Preview ID 生命周期

Web Preview 以 replyId 聚合模型回复。由于 Web 当前只消费 Delta、不把 TextBlockEnd 映射为消息关闭，同一 reply 的多次文本段可以继续复用同一个 eventId。

`INTERMEDIATE` 后不立即删除 replyId 到 eventId 的映射，以接收同 reply 中工具后的后续文本。已经标记为 `INTERMEDIATE` 的映射在对应 `ModelCallEndEvent` 后释放；未处置的终止候选映射必须跨越 `ModelCallEndEvent -> AgentResultEvent -> TERMINAL -> AgentEndEvent` 保留，直到完成持久化或权威空结果删除。

### 15.3 Result 暂存与校准

因为事件顺序是 `AgentResultEvent -> TERMINAL -> AgentEndEvent`，Web mapper 暂存顶层最后一个 Result。Disposition 只处置实时文本；真正的结果提交边界是 `AgentEndEvent`。即使没有任何 TextBlock 和 `TERMINAL`，AgentEnd 到达时也必须结合最后一个 Result 与 `GenerateReason` 处理 structured-only 或 Middleware 直接生成的结果：

- 正常完成且存在终止 Preview：复用对应 Preview eventId，持久化权威 `agent.message`；
- 权限询问、工具挂起或暂停：不持久化普通 agent.message，交给现有 HITL/状态处理；
- 没有文本 Preview 的正常结构化结果：在 AgentEnd 时生成新的持久化 eventId；
- 权威结果为空：发送 `event_update`，携带 `authoritative=true`、`hasOutput=false`，前端删除对应 Preview，不显示占位符。

持久化 payload 保留现有 `text` 兼容字段，同时增加由统一 Message DTO mapper 生成的完整 `content`、metadata、generateReason 和 structured output。不得继续把所有结果降格为单个伪造 TextBlock。

### 15.4 WebUI 状态

```text
turn
|- pendingSegments
|- commentarySegments
|- thinkingSegments
|- toolExecutions
`- finalAnswer
```

`INTERMEDIATE` 将对应 Preview 移入 Commentary；`TERMINAL` 关闭 Pending；只有正常完成的权威 Result 才确认 finalAnswer。权威空字符串与字段缺失必须区分，空结果删除 Preview，不能显示 `[agent response]`。

中间 Commentary 不持久化，页面刷新后不恢复。

## 16. 子 Agent 与来源处理

标注器不能假设子 Agent 一定先在自己的 `streamEvents()` 中完成标注。当前本地 `AgentSpawnTool` 使用子 Agent `call()`，模型事件经 external emitter 直接进入父流，子 Agent Result 不会同步转发。

因此顶层标注器必须处理所有 source，并按 `source + taskId` 隔离状态。`AgentSpawnTool`、本地 manager 和远程转发器必须统一为每次子调用附加 TaskRecord ID；缺少唯一 taskId 的并发同名子 Agent 事件视为不满足标注前置条件。对子 Agent，带 source 的成功 `AgentEndEvent` 只产生 `TERMINAL` 生命周期处置，不产生权威结果或父 Agent 最终答案。

本次方案不要求重构子 Agent 为消费 `streamEvents()`，但必须增加真实 `AgentSpawnTool + ReActAgent` 集成测试。既有 source 原地覆盖与嵌套路径组合问题单独处理，不在本 PR 借机扩大范围。

## 17. 背压与取消

标注器保持 Reactive Streams 背压感知和事件顺序，但不声称“不改变背压”。一个源事件可能展开为两个下游事件，并且顶层 AgentEnd 会暂存到源正常完成；实现必须限制预取，并明确最多缓存一个派生事件和一个顶层 End。

测试必须覆盖：

- `StepVerifier` 逐个 request；
- 收到 Result 后、TERMINAL 前取消；
- 收到 TERMINAL 后取消；
- 顶层 End 后追加 Result 时，在 TERMINAL/End 对下游可见前报错；
- 多次订阅与并发订阅；
- 下游慢消费者下的事件顺序。

客户端不能仅凭 disposition 持久化内容。Result 是权威数据，disposition 是展示生命周期信号。

## 18. 测试设计

### 18.1 Core

必须覆盖：

1. 单轮文本正常结束产生 `TERMINAL(MODEL_STOP)`。
2. 文本后工具在 ToolCallStart 前产生 `INTERMEDIATE`。
3. 工具先于文本时在文本 End 后产生 `INTERMEDIATE`。
4. 同一 reply 的“文本 -> 工具 -> 文本”只分类一次，后续文本仍属于同一 reply。
5. 无工具但下一轮开始时上一轮为 `INTERMEDIATE`。
6. 空响应、Thinking-only 与 Structured Output 重试。
7. permission asking、tool suspended、middleware stop 和 interrupted 的 disposition/reason。
8. Result 与流式文本不一致、为空和 structured-only。
9. 多个 AgentResult 时以 End 前最后一个为准；End 后追加 Result 必须触发生命周期契约错误。
10. `onAgent` Middleware 删除、替换和追加事件后的标注结果。
11. JSON round-trip 保留 id、createdAt、source 和 metadata。
12. 多次与并发订阅状态隔离。
13. 逐 request 背压与取消。
14. 真实本地子 Agent 转发、两级来源，以及同一父会话并发调用两次相同 agentId 时通过 taskId 隔离。

### 18.2 AG-UI

- 同一 reply 多 TextBlock 使用不同 messageId，标准 Start/Content/End 合法。
- disposition Custom Event 携带全部 messageIds。
- 正常结果通过 `MESSAGES_SNAPSHOT` 校准完整内容。
- permission asking 和 tool suspended 不产生普通最终答案快照。
- capability 关闭时现有事件完全不变。

### 18.3 Remote/AgentProtocol

- payload 在完整 SSE JSON 中仍是字符串。
- full 包含 disposition 与 AgentResult。
- 其他 AGENT_EVENT 仍仅 verbose 可见。
- 新客户端恢复 subtype；模拟旧客户端安全丢弃。
- 序列化失败不发送空壳 disposition。

### 18.4 Web 数据面与前端

- `event_update` 真实通过 runner、preview bus、controller 到达前端且不持久化。
- 同 reply 的工具后文本不产生新 Preview ID 或永久 Pending。
- permission asking/tool suspended 不持久化普通 agent.message。
- 完整 ContentBlock、structured output 和 metadata 不丢失。
- 权威空结果删除 Preview，不显示占位符。
- 中断和刷新后的状态收敛符合“不持久化 Commentary”的约束。

## 19. 官方贡献策略

先提交 RFC/Issue，不直接发送完整实现 PR。推荐标题：

> RFC: Lifecycle disposition for immediately streamed text without buffering

核心说明：

> `FinalAnswerFilterMiddleware` introduced by #2926 addresses final-round-only delivery by buffering text until a reasoning round can be classified. Interactive consumers instead need immediate token delivery and can retrospectively reclassify already-rendered text.
>
> We propose an opt-in lifecycle disposition signal for visible text. A reply is `INTERMEDIATE` when execution proceeds to a tool or another model call, and `TERMINAL` when the invocation ends with that reply as its last visible model output. `AgentResultEvent` remains the sole authoritative result; `TERMINAL` does not imply byte-for-byte equality with the final `Msg`.

RFC 先确认三件事：

1. `INTERMEDIATE/TERMINAL` 生命周期语义；
2. opt-in 的 Core 流标注方式；
3. disposition 与 `AgentResultEvent`、Middleware 的边界。

达成共识后拆分：

1. **Core PR**：事件、`AgentEventStreams`、内部 tracker、JSON/背压/生命周期测试和双语文档。
2. **Remote PR**：RemoteEventCodec 与 AgentProtocol mixed-version 测试。
3. **AG-UI PR**：多文本段 messageId、Custom disposition 和标准 MessagesSnapshot。
4. **Web PR**：后端 Update 通道、完整消息 payload 与前端状态机同一 PR 提交。

Core PR 不默认改变 `streamEvents()`。本地受控的 AG-UI/Web 集成可以内部启用标注；是否在未来默认启用，只能在官方确认兼容策略后另行决定，本次方案不预设后续版本。

## 20. 验收标准

1. 未启用标注时，`streamEvents()` 与现有行为一致。
2. 启用后，文本 Delta 仍立即输出，不等待模型调用或 Agent 结束。
3. 工具调用或下一轮模型开始前，对应回复被标为 `INTERMEDIATE`。
4. 调用正常结束时，最后一个未处置回复被标为 `TERMINAL`，但最终内容仍以 Result 为准。
5. HITL、暂停和中断不会被无条件持久化成普通最终答案。
6. AG-UI 同一 reply 的多文本段保持合法消息生命周期。
7. full 远程客户端同时获得 disposition 和 AgentResult，旧客户端不会因新增远程枚举失败。
8. Web 中间文本不持久化，最终消息保留完整内容和结构化数据。
9. 子 Agent emitter 路径、唯一 taskId、背压、取消和并发订阅都有真实回归测试。

## 21. 子代理评审结论处理

设计采纳了评审发现的以下阻断问题：

- 将 `FINAL_ANSWER` 修正为 `TERMINAL` 生命周期语义；
- Core 初始能力改为 opt-in；
- 明确 post-middleware 标注边界；
- 跟踪所有 source，兼容本地子 Agent emitter 旁路；
- full 远程流同时放行 disposition 和 AgentResult；
- 处理同 reply 的工具后文本、AG-UI 多 TextBlock、HITL 和空权威结果；
- 使用完整消息快照/DTO，不把权威结果降格为纯文本；
- 明确 Preview Update 的全链路改造和背压限制。

本次没有采纳“立即把本地子 Agent 全面改为消费 `streamEvents()`”的建议。该改动会扩大到 AgentSpawnTool、任务生命周期、结果提取和远程/本地一致性，不是实现 disposition 的必要前提；本方案通过按来源跟踪和子 Agent End 语义解决当前展示需求，并要求真实集成测试验证。
