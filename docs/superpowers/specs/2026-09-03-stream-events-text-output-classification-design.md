# `streamEvents` 实时文本分类设计

## 1. 背景

`ReActAgent#streamEvents(...)` 是 AgentScope Java 未来统一的细粒度流式入口。它会实时发送模型文本、Thinking、工具调用、工具结果、HITL 和 Agent 生命周期事件，并以 `AgentResultEvent` 提供本次调用的权威最终 `Msg`。

官方主干已引入 `FinalAnswerFilterMiddleware`。该中间件缓存每轮模型文本：有工具调用时丢弃文本，没有工具调用时在 `ModelCallEndEvent` 前释放文本。它适合只接受最终轮文本、能够容忍首 Token 延迟的渠道，但不适合需要真实流式体验的 WebUI 和 AG-UI。

交互式客户端需要同时满足：

- 文本 Token 到达后立即展示；
- 区分模型 Thinking、工具执行、中间过程文本和最终答案；
- 不要求调用方同时订阅两套流；
- 不破坏现有 `streamEvents()`、AG-UI 和远程 Agent 客户端；
- 最终展示结果能够与 `AgentResultEvent` 校准。

## 2. 目标

本次改造实现以下能力：

1. 保留 `streamEvents()` 作为唯一公共流式入口。
2. 保持所有现有 `AgentEvent` 的类型、字段、彼此之间的相对顺序和实时发送行为；只在语义边界插入新的分类事件。
3. 默认增加文本输出分类事件，将一个模型回复中的可见文本标记为中间过程或最终答案。
4. 以 `AgentResultEvent` 作为本次 Agent 调用的权威最终结果。
5. 为 AG-UI、Web 数据面和远程 Agent 提供向后兼容的映射。
6. 中间过程文本只做实时展示，不写入持久化事件日志。

## 3. 非目标

本次改造不包括：

- 不新增 `streamOutput()` 或 `streamConversation()` 公共方法。
- 不将 `streamEvents()` 改成只包含面向 UI 的事件。
- 不删除或替代 `FinalAnswerFilterMiddleware`。
- 不公开模型私有 Thinking；Thinking 继续使用现有独立事件和渠道策略。
- 不持久化中间过程文本，也不新增 `agent.commentary` 历史事件类型。
- 不重新设计 Agent 会话、消息历史或工具事件协议。
- 不承诺错误或取消前的未确认文本是最终答案。

## 4. 核心设计决策

### 4.1 单一公共流式入口

公共 API 保持不变：

```java
Flux<AgentEvent> streamEvents(...)
```

新增的文本分类是 Agent 执行语义的一部分，因此作为新的 `AgentEvent` 加入同一事件流。AG-UI、WebUI、CLI、可观测工具和远程 Agent 可以消费同一条流，并自行选择展示范围。

### 4.2 不在 `ModelCallEndEvent` 判断最终答案

`ModelCallEndEvent` 仅代表一次模型调用结束。此后仍可能发生：

- PostReasoning Hook 请求 `gotoReasoning`；
- Structured Output 强制模型重试；
- 空文本或 Thinking-only 响应触发再次推理；
- Middleware 或 HITL 改变终止结果。

因此不能使用“模型调用结束且没有工具调用”作为通用最终答案判断。只有 `AgentResultEvent` 到达时，当前未分类文本才能确认为最终答案。

### 4.3 `AgentResultEvent` 是最终权威结果

`TextBlockDeltaEvent` 是实时预览，`AgentResultEvent#getResult()` 是权威结果。适配器在完成时比较流式文本和最终 `Msg`：

- 完全一致：直接完成；
- 最终结果包含额外后缀：补齐缺少部分；
- 内容不一致：用权威结果替换最终答案段；
- 最终文本为空：移除错误确认的文本，按最终结构化结果处理。

Core 分类器不拼接文本，也不生成补偿 Delta。校准属于协议适配层职责。

## 5. 新增 Core 事件

新增公开枚举：

```java
public enum TextOutputClassification {
    INTERMEDIATE,
    FINAL_ANSWER
}
```

新增公开事件：

```java
public final class TextOutputClassificationEvent extends AgentEvent {

    private final String replyId;
    private final TextOutputClassification classification;
}
```

并增加：

```java
AgentEventType.TEXT_OUTPUT_CLASSIFICATION
```

事件使用现有 `replyId` 关联同一次模型调用中的文本，不增加 `conversationId`、`segmentId` 或新的消息 ID。当前一个模型回复中的全部可见文本共同接受一次分类，即使 Text 和 Thinking 发生交错。

事件仅在观察到非空 `TextBlockDeltaEvent` 后发送。没有可见文本的模型调用不产生该事件。

## 6. 分类组件

新增包级、无共享状态的流操作器：

```java
final class TextOutputClassifier {

    static Flux<AgentEvent> classify(Flux<AgentEvent> source);
}
```

每次订阅通过 `Flux.defer` 创建独立状态：

```java
final class ClassificationState {
    String currentReplyId;
    boolean textSeen;
    boolean toolCallSeen;
    boolean classified;
}
```

使用 `concatMap` 将一个输入事件有序展开为零个、一个或两个输出事件。分类器不缓存文本内容、不阻塞线程、不改变背压，也不修改已有事件对象。

分类器应用于完整 Middleware 链之后：

```java
Flux<AgentEvent> events =
        MiddlewareChain.build(middlewares, this, context, MiddlewareBase::onAgent, core)
                .apply(input);

return TextOutputClassifier.classify(events);
```

这样分类反映 Middleware 处理后实际对外发送的事件。若 Middleware 删除了文本，分类器不会为不可见文本产生分类事件。

## 7. 分类状态机

### 7.1 `ModelCallStartEvent`

如果上一轮存在未分类的可见文本，先发送：

```text
TextOutputClassificationEvent(INTERMEDIATE)
```

然后转发新的 `ModelCallStartEvent` 并重置当前轮状态。该规则覆盖 PostReasoning `gotoReasoning`、Structured Output 重试和其他无工具的再次推理。

### 7.2 `TextBlockDeltaEvent`

Delta 立即原样转发，并记录当前 `replyId` 已产生可见文本。分类器不等待最终判定后再发送 Delta。

### 7.3 `ToolCallStartEvent`

记录当前回复出现工具调用。

- 如果文本已经出现且尚未分类，先发送 `INTERMEDIATE`，再转发工具开始事件。
- 如果工具先于文本出现，只记录工具状态；后续文本结束时再发送 `INTERMEDIATE`。
- 同一轮多个工具只产生一次分类事件。

### 7.4 `TextBlockEndEvent`

始终先转发原始 End 事件。如果当前轮已经出现工具调用、文本随后才出现且仍未分类，则在 End 后发送 `INTERMEDIATE`。

### 7.5 `ModelCallEndEvent`

只转发事件，不做最终答案判断，也不清除等待 Agent 决策的当前轮状态。

### 7.6 `AgentResultEvent`

如果当前轮存在未分类的可见文本，先发送 `FINAL_ANSWER`，再转发 `AgentResultEvent`。随后清除状态。

如果当前轮已经被标记为 `INTERMEDIATE`，或者没有可见文本，则不产生 `FINAL_ANSWER`。

### 7.7 异常、取消与非正常结束

`onError`、订阅取消或只有 `AgentEndEvent` 而没有 `AgentResultEvent` 时，不产生 `FINAL_ANSWER`。原始 Reactor 错误和取消语义保持不变；分类器不使用 `materialize()` 将错误转换为普通事件。

## 8. 典型事件顺序

### 8.1 文本后调用工具

```text
ModelCallStart
TextBlockStart
TextBlockDelta...
TextBlockEnd
TextOutputClassification(INTERMEDIATE)
ToolCallStart
ToolCallDelta...
ModelCallEnd
```

### 8.2 工具先于文本出现

```text
ModelCallStart
ToolCallStart
TextBlockStart
TextBlockDelta...
TextBlockEnd
TextOutputClassification(INTERMEDIATE)
ModelCallEnd
```

### 8.3 普通最终答案

```text
ModelCallStart
TextBlockStart
TextBlockDelta...
TextBlockEnd
ModelCallEnd
TextOutputClassification(FINAL_ANSWER)
AgentResultEvent
AgentEndEvent
```

### 8.4 无工具但 Hook 请求再次推理

```text
第一轮 TextBlockEnd
第一轮 ModelCallEnd
TextOutputClassification(INTERMEDIATE)
第二轮 ModelCallStart
```

## 9. 子 Agent 事件

分类器只处理当前 Agent 产生的、`source == null` 的事件。子 Agent 在自己的 `streamEvents()` 中完成分类；事件转发给父 Agent 时再附加 `source`。

父 Agent 分类器对带 `source` 的事件原样转发，避免父子事件交错时重复分类。新分类事件与其他子 Agent 事件一样保留 `source` 和远程任务 metadata。

## 10. 与 `FinalAnswerFilterMiddleware` 的关系

两种机制服务于不同渠道：

| 使用场景 | 机制 |
| --- | --- |
| WebUI、AG-UI、交互式聊天 | 实时 `TextBlockDeltaEvent` + 分类事件 |
| Webhook、通知、只接受最终轮文本的渠道 | `FinalAnswerFilterMiddleware` |
| 调试和可观测性 | 完整 `streamEvents()` |
| 获取权威最终结果 | `AgentResultEvent` |

`FinalAnswerFilterMiddleware` 当前以“同一模型轮是否出现工具调用”判断是否释放文本。分类事件以完整 Agent 生命周期为依据，语义更严格；在 PostReasoning 重试或 Structured Output 重试等情况下，分类器可能将该中间件已经释放的文本标记为 `INTERMEDIATE`。本次改造不改变该中间件的行为，避免扩大官方 PR 范围。

## 11. AG-UI 适配

现有标准文本映射保持不变：

```text
TextBlockStartEvent -> TextMessageStart
TextBlockDeltaEvent -> TextMessageContent
TextBlockEndEvent   -> TextMessageEnd
```

分类事件转换为 AG-UI Custom Event：

```json
{
  "name": "agentscope.text_output.classified",
  "value": {
    "replyId": "reply-123",
    "classification": "INTERMEDIATE"
  }
}
```

`AgentResultEvent` 转换为权威完成事件：

```json
{
  "name": "agentscope.output.completed",
  "value": {
    "replyId": "reply-456",
    "text": "authoritative final text"
  }
}
```

AG-UI 转换上下文记录最近一次 `FINAL_ANSWER` 的 `replyId`，用于完成事件关联。增强客户端根据分类移动或确认文本段，并以完成事件校准内容；旧客户端忽略 Custom Event，标准文本流不受影响。

过程说明不能映射为 Thinking。模型 Thinking 和 Agent 中间过程文本保持不同语义通道。

## 12. 远程 Agent 和 AgentProtocol

Core 使用新的强类型事件，但远程 Wire 层不增加新的 `RemoteEventType` 枚举值。新增远程枚举会使旧 Java 客户端反序列化未知枚举时失败。

分类事件复用已有 `AGENT_EVENT`：

```json
{
  "type": "AGENT_EVENT",
  "eventType": "TEXT_OUTPUT_CLASSIFICATION",
  "payload": {
    "type": "TEXT_OUTPUT_CLASSIFICATION",
    "replyId": "reply-123",
    "classification": "INTERMEDIATE"
  }
}
```

新客户端从 `payload` 恢复强类型事件。旧客户端认识 `AGENT_EVENT`，无法恢复新 payload 时安全丢弃该事件。

`RemoteEventCodec#matchesDetail(RemoteAgentEvent, String)` 增加事件级判断：

```text
AGENT_EVENT + TEXT_OUTPUT_CLASSIFICATION -> detail=full
其他 AGENT_EVENT                         -> detail=verbose
```

现有仅接收 `RemoteEventType` 的重载保留，避免破坏调用方；实际远程事件过滤应使用完整 DTO 重载。

## 13. Web 数据面

### 13.1 Preview Frame

兼容扩展现有 `PreviewFrame`：

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

新增流式类型：

```java
EVENT_UPDATE = "event_update";
```

分类帧示例：

```json
{
  "type": "event_update",
  "targetType": "agent.message",
  "eventId": "evt_xxx",
  "attributes": {
    "replyId": "reply-123",
    "classification": "INTERMEDIATE"
  }
}
```

### 13.2 Preview ID

`PreviewIds` 从单个当前消息 ID 改为按 `replyId` 管理：

```java
Map<String, String> messageEventIds;
String finalReplyId;
```

处理规则：

```text
TextBlockDelta(replyId)
  -> 创建或获取 replyId 对应的 eventId
  -> 发送 event_delta

TextOutputClassification(INTERMEDIATE)
  -> 发送 event_update
  -> 释放服务端该 replyId 的 Preview 状态

TextOutputClassification(FINAL_ANSWER)
  -> 发送 event_update
  -> 记录 finalReplyId

AgentResultEvent
  -> 复用 finalReplyId 对应的 eventId
  -> 持久化完整 agent.message
  -> 客户端用同一 eventId 校准实时内容
```

如果 `AgentResultEvent` 没有对应的最终文本 Preview，例如纯结构化结果，则生成新的持久化事件 ID。

### 13.3 WebUI 状态

客户端单轮状态建议为：

```text
turn
|- pendingSegments
|- commentarySegments
|- thinkingSegments
|- toolExecutions
`- finalAnswer
```

收到 `INTERMEDIATE` 时将对应 Pending Segment 移入 `commentarySegments`；收到 `FINAL_ANSWER` 时确认 `finalAnswer`；收到持久化 `agent.message` 时以完整 payload 校准最终内容。

中间文本不写入事件日志。页面刷新后不会恢复 Commentary，但最终答案、Thinking 和工具事件仍按现有规则恢复。

## 14. 兼容性

### 14.1 Java API

- `streamEvents()` 方法签名不变。
- 所有现有事件的字段和顺序不变，只在特定边界插入新事件。
- 增加 `AgentEventType` 枚举值可能要求使用穷举 `switch` 的源码调用方增加分支；这是扩展强类型事件体系的已知成本。
- 不增加默认配置项或 Builder 开关。分类事件默认启用，避免不同协议入口产生不一致语义。

### 14.2 AG-UI

旧客户端继续读取标准文本事件并忽略未知 Custom Event。它们不会获得过程/最终分类，但行为不会比改造前更差。

### 14.3 远程协议

复用现有 `AGENT_EVENT` Wire 类型，避免新增远程枚举破坏旧客户端。只有理解新 Core subtype 的客户端才能恢复分类事件。

### 14.4 Web 数据面

`PreviewFrame` 保留四参数构造方式。旧前端忽略 `event_update` 和新增 attributes；新前端启用分类展示。

## 15. 测试设计

### 15.1 Core 单元测试

必须覆盖：

1. 单轮文本回答在 `AgentResultEvent` 前产生 `FINAL_ANSWER`。
2. 文本后调用工具时在工具开始前产生 `INTERMEDIATE`。
3. 工具先于文本时在文本 End 后产生 `INTERMEDIATE`。
4. 同一轮多个工具只产生一次分类。
5. 无工具但出现下一次模型调用时，上一轮标记为 `INTERMEDIATE`。
6. 空响应和 Thinking-only 重试不产生文本分类。
7. Structured Output 强制重试时前一轮标记为 `INTERMEDIATE`。
8. 异常、取消和缺少 `AgentResultEvent` 时不产生 `FINAL_ANSWER`。
9. 多次订阅和并发订阅状态隔离。
10. 带 `source` 的子 Agent 事件不被父分类器重复分类。
11. Middleware 删除全部文本后不产生分类事件。
12. 与 `FinalAnswerFilterMiddleware` 组合时，分类以实际可见事件和最终 Agent 生命周期为准。

### 15.2 Core 集成测试

使用真实的两轮 ReAct 测试模型和工具，验证：

```text
第一轮实时文本
-> INTERMEDIATE
-> 工具执行
-> 第二轮实时文本
-> FINAL_ANSWER
-> AgentResultEvent
```

增加 PostReasoning `gotoReasoning` 集成用例，证明 `ModelCallEndEvent` 不会提前产生最终分类。

### 15.3 AG-UI 测试

- 标准文本事件顺序不变。
- 分类 Custom Event 位于对应工具事件或最终结果之前。
- `AgentResultEvent` 映射的完成事件携带权威文本和最终 `replyId`。
- 流式文本与最终结果不一致时，完成事件能够触发替换。

### 15.4 远程协议测试

- 分类事件编码为 `AGENT_EVENT`。
- `detail=full` 包含分类事件。
- 其他通用 `AGENT_EVENT` 仍要求 `detail=verbose`。
- 新客户端从 payload 恢复事件并保留 ID、时间、source 和 metadata。
- 无法识别新 subtype 的模拟旧客户端能够安全丢弃事件。

### 15.5 Web 数据面测试

- 不同 `replyId` 获得不同 Preview ID。
- 中间段只产生 `event_update`，不持久化。
- 最终段复用正确 Preview ID 持久化 `agent.message`。
- 多轮工具调用不会把中间文本拼入最终答案。
- 最终 `Msg` 与流式文本不一致时使用完整持久化 payload 校准。
- 中断后 Pending Segment 不会成为历史最终答案。

## 16. 官方贡献策略

在提交代码前先创建或补充官方 Issue，关联 `FinalAnswerFilterMiddleware` 对应的 PR #2926，明确两个能力的区别：

```text
#2926: final-round-only output, accepts buffering latency
本方案: immediate token streaming with retrospective classification
```

代码建议拆成三个可独立审查的 PR：

1. **Core PR**：事件、分类器、Core 测试和中英文文档。
2. **Protocol PR**：AG-UI Custom Event、RemoteEventCodec 和 AgentProtocol round-trip。
3. **Web data-plane PR**：按 `replyId` 管理 Preview Segment 和最终消息校准。

Core PR 不依赖 WebUI，能够被 CLI、SSE 或其他调用方直接消费。Protocol PR 不改变标准 AG-UI 文本事件。Web data-plane PR 不阻塞 Core 能力合入。

官方最可能质疑默认新增事件会影响严格断言事件数量的调用方。主方案仍采用默认启用，因为新增事件不修改现有事件语义，并且统一入口比每个适配器单独启用更可靠。只有维护者明确要求时，才将 Builder 开关作为上游协商备选，不在初始实现中增加该配置。

## 17. 验收标准

本次改造完成后应满足：

1. WebUI 和 AG-UI 在收到首个文本 Token 时立即展示，不等待模型调用结束。
2. 工具调用或再次推理发生前，对应文本被明确标记为 `INTERMEDIATE`。
3. 只有紧邻权威 `AgentResultEvent` 的未分类文本被标记为 `FINAL_ANSWER`。
4. Thinking、工具过程、中间文本和最终答案保持独立语义。
5. 老 AG-UI 和远程客户端能够忽略新语义，不因未知远程枚举而失败。
6. 中间过程文本不进入持久化会话历史。
7. `FinalAnswerFilterMiddleware` 保持现有行为。
8. 所有新增状态均按订阅隔离，事件顺序和 Reactor 背压不被破坏。
