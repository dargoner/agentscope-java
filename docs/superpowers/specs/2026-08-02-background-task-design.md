# AgentScope Java 后台任务机制初步规划

> 状态：Draft，待评审
>
> 日期：2026-08-02
>
> 范围：不修改 `agentscope-core`；必要时允许对 `agentscope-harness`
> 做少量兼容性扩展和正确性修复。

## 1. 背景与目标

AgentScope Java 已有异步工具和子代理任务的局部能力，但还缺少一套统一、
可持久化、面向用户的后台任务机制。

本方案需要满足：

- 用户不需要预判 AI 对话或工具是长任务还是短任务。
- 整轮 Agent 执行是顶层托管任务，即使没有调用任何 Tool 也能转后台、查询、
  取消和接收通知。
- 未转后台的正常短对话保持现有响应结构、SSE 事件、错误语义和 Agent 推理过程。
- 必须同步返回的场景可以明确禁止转后台。
- 允许后台执行的任务可以与 HTTP 请求解绑，而不迁移正在运行的 Java 调用栈。
- 多实例部署和进程重启后，任务状态、进度和历史仍可查询。
- 当前登录用户可以查询、订阅、取消自己的后台任务。
- 每个业务步骤以及 Agent、模型、工具、子代理事件都能形成进度通知。
- 成功、失败、取消都能可靠通知外部系统和原对话。
- 异步子代理是一等任务，能够查询父子关系、聚合进度和级联取消。

## 2. 非目标

第一阶段不处理：

- 修改 `agentscope-core`。
- 暂停并迁移任意 Java 线程、Reactor 调用栈或模型流。
- 超时后重新提交或重新执行已经开始的 Agent/Tool 调用。
- 向模型上下文注入“工具已转后台”的占位结果来完成整轮 Agent 后台化。
- 根据模型文本或 token 数虚构任务完成百分比。
- 为所有工具提供任意代码位置的断点续跑。
- 引入 `PAUSED` 状态；人工确认继续沿用 Agent 会话机制。
- 使用 Skill 的自然语言描述直接控制调度策略。
- 第一阶段在进程重启后自动重放普通 Agent、本地 Tool 或普通 MCP Tool。

## 3. 当前机制评估

### 3.1 Java 版

目前有两套局部机制。

1. 显式子代理任务

   `AgentSpawnTool -> TaskRepository -> WorkspaceTaskRepository`，已经具备
   `TaskRecord` 持久化、心跳、孤儿任务扫描、远程 Agent Protocol 轮询、
   Admin 查询和取消。

2. 长耗时工具 offload

   `AsyncToolMiddleware` 等待工具一段时间；未完成时返回占位结果，工具完成后
   通过 session inbox 和 wakeup 唤醒 Agent。

主要缺口和风险：

- 两套机制没有统一的任务模型和用户接口。
- 没有通用、持久化的步骤和进度事件。
- `WorkspaceTaskRepository` 的部分终态更新不是条件原子更新。
- `WorkspaceMessageBus.queueDrain` 不是严格的原子领取。
- 部分远程子代理完成路径没有触发完成回调。
- 本地子代理异常可能被包装为 `"Error: ..."` 后记录为 `COMPLETED`。
- 本地 `TaskRunSpec` 依赖不可序列化的 `Supplier`，进程重启后无法重建。
- 异步工具失败或取消时，原对话不一定能收到终态通知。

### 3.2 Python 版

Python 已有以下可复用思路：

- `BackgroundTaskManager`
- `ToolOffloadMiddleware`
- `CancelDispatcher`
- Redis 活跃任务登记
- inbox + wakeup
- fire-and-forget 对话执行
- session SSE 实时推送和有限回放

Python 有两种触发方式：

1. 整轮 Chat：请求进入后立即由 `ChatRunRegistry` 创建 `asyncio.Task`，POST
   立即返回 `started`，结果通过独立 session SSE 推送。
2. 单个 Tool：工具先执行；默认 10 秒仍未返回终态时，
   `ToolOffloadMiddleware` 将正在运行的 `asyncio.Task` 登记为后台任务并返回
   占位结果。`is_state_injected` 和 `is_external_tool` 保持同步。

Python 的后台触发不读取 Skill 或 MCP description，也不会自动创建子代理。

Python 版的不足：

- 任务完成后活跃登记被删除，没有持久终态历史。
- 本地 `asyncio.Task` 在进程重启后无法恢复。
- 没有通用的显式业务进度 API。
- 用户任务列表、详情和历史接口不完整。
- 后台失败、取消可能不投递到原对话。
- session SSE 回放是临时、有限且按 run 清理的，不等同于任务历史。

Java 方案保留 Python 的超时解绑、inbox/wakeup、取消分发等概念，同时补充
持久化、用户查询、可靠通知、进度和重启后历史查询能力。

### 3.3 Hermes Agent

Hermes 已实现运行时决定顶层异步委派、不可变来源路由、完成通知的
claim/ack/drop，以及基于最后进展时间的卡住检测。`delegate_task(background=true)`
还会把 delegation 元数据、终态结果和通知状态写入本地 SQLite `state.db` 的
`async_delegations` 表。这些概念可用于本方案的入口能力门禁、
`delivery_route_json`、可靠 outbox 和无进展指标。

Hermes 的实际子代理仍在线程池 daemon thread 中执行，SQLite 不是执行队列；所属
进程退出后不会由其他实例接管，遗留 `running/finalizing` 记录只会转成
`unknown`。其 terminal 后台进程另用 `processes.json` checkpoint，Kanban 则是
独立的 durable SQLite work queue。Java 第一阶段借鉴持久状态和通知语义，但不
复制这种多套 registry，也不把本地 SQLite 当作多实例任务调度器。

## 4. 核心设计原则

### 4.1 后台任务与子代理是两个维度

后台描述生命周期和结果交付方式；子代理描述由谁执行。

任务类型分为：

- `AGENT_RUN`：普通 Agent 对话或继续执行。
- `TOOL`：本地工具或 MCP 工具调用。
- `SUBAGENT`：委派给另一个 Agent 的任务。

普通 `AGENT_RUN` 或 `TOOL` 转后台后，任务类型不变。只有显式调用
`AgentSpawnTool` 或编排策略显式委派时，才创建 `SUBAGENT`。

### 4.2 前台和后台是交付方式

允许后台执行的 Agent 在现有调用入口立即执行，不先进入数据库队列，也不切换到
另一套 Worker 执行路径。`ManagedExecutionSupervisor` 从开始持有底层 Reactor
订阅，HTTP/SSE 只是观察同一次执行。这样客户端断开或前台等待结束时，不会取消
已经提升为后台的执行，也不会产生第二次订阅。

任务有独立于执行状态的交付阶段：`FOREGROUND` 和 `BACKGROUND`。`AUTO` 在前台
阶段只保留轻量内存控制块并透传原事件；达到等待期限或支持后台回传的连接断开
时，才原子持久化任务、当前进度快照和投递路由，并切换为 `BACKGROUND`。正在
运行的 Java 调用栈保持原位继续执行，不迁移线程或实例，不重新排队或重放。

如果后台持久化或可靠回传能力不可用，系统不能完成转换：连接仍在时继续同步
执行；连接已断开时按原调用取消语义结束。后台基础设施故障不能让本来可以正常
同步完成的对话失败。

用户发起一轮对话时，入口适配器首先创建内存中的顶层 `AGENT_RUN` 控制块。该轮
执行中的模型调用形成进度事件，本地 Tool、MCP Tool 和子代理形成带
`parent_task_id` 的子任务：

```text
AGENT_RUN (用户本轮对话，顶层任务)
  -> TOOL (LOCAL)
  -> TOOL (MCP)
  -> SUBAGENT
       -> TOOL (LOCAL | MCP)
```

因此，即使 Agent 只进行多轮模型推理而没有调用 Tool，整轮执行仍然受后台任务
机制管理。顶层请求超过等待预算时向调用方返回的是 `AGENT_RUN.taskId`；子任务
状态可单独查询，但其结果先交回父 Agent，最终对话回复由顶层 `AGENT_RUN` 统一
投递，避免多个 Tool 分别向前端发送终态消息。

“返回 `taskId`”只适用于本身支持异步确认的入口。已有 SSE 在连接存在时继续
推送原事件；OpenAI 兼容的无状态 Chat Completions 默认保持原协议，不自动改成
`202`。Stateful Gateway/Chat UI 等具备 session 和异步投递能力的入口才默认允许
自动提升后台。

### 4.3 托管入口内所有 Tool 调用统一管理

在已托管的 `AGENT_RUN` 内，Agent 仍按现有方式调用本地 Tool 或 MCP Tool。框架
在统一 Tool 调用边界观察调用并创建子任务，不要求 Skill 感知后台任务，也不要求
每个业务编写提交工具或专用 `TaskHandler`。独立 Tool API 调用也经过相同边界，
只是没有 `AGENT_RUN` 父任务。

```text
Agent -> Tool Call -> ManagedToolMiddleware -> 原有 Local/MCP Tool 调用链
                              \-> ManagedTaskService（记录子任务/事件）
```

无论调用来自顶层 Agent、Skill 引导后的模型决策还是子代理，本地 Tool 和 MCP
Tool 都经过该边界。前台短对话阶段只在内存记录 Tool 生命周期并透传原事件；父
`AGENT_RUN` 提升后台时，仍在运行的 Tool 子任务和当前快照一起持久化，之后的
Tool 事件持续落库。已经在前台完成的 Tool 可只保留在父任务提升快照中，避免每个
短对话工具调用都同步写数据库。

对话内 Tool 不独立执行 10 秒超时，也不向模型注入占位 `ToolResult`。普通本地
Tool 和普通 MCP Tool 跟随父 `AGENT_RUN` 的前台/后台生命周期，执行结果仍按原
调用链交给父 Agent。只有独立 Tool API 或工具契约本身明确为原生异步任务时，
才以 Tool 自身作为顶层任务应用执行模式。

托管表示统一观察状态、事件和取消语义，不表示所有 Tool 都必须独立转后台。
依赖活跃 Agent state、人工确认或进程内对象的 Tool 保持内联执行；进程重启导致
执行丢失时记录明确失败，不尝试重放。

### 4.4 数据库是事实来源

关系数据库持久化后台任务、事件、owner 心跳和 outbox。Redis 只用于加速任务唤醒、
取消广播和实时事件分发。即使 Redis 数据丢失，也不能影响任务最终状态、历史
查询和通知正确性。

### 4.5 无感与兼容红线

- 功能关闭时完全走现有路径。
- 功能开启但任务在前台完成时，不改变响应、SSE、异常、取消和 Agent context。
- Supervisor 只能订阅底层执行一次，后台提升不能重新调用 Agent/Tool。
- 不向模型注入后台占位结果，不额外触发模型推理。
- 前台短任务不做同步数据库写入；只有后台提升必须在解绑前持久化成功。
- 后台存储或通知组件异常时保持同步或按原语义取消，不能使正常对话失败。
- OpenAI 兼容接口、未托管 Gateway 和直接 core 调用保持现有协议与行为。

## 5. 执行模式与触发规则

### 5.1 三种模式

| 模式 | 行为 | 场景 |
| --- | --- | --- |
| `SYNC_ONLY` | 保持原同步执行并禁止自动解绑；可记录内存指标但不创建后台任务 | 事务内校验、必须立即返回结果、协议不支持异步确认 |
| `AUTO` | 原调用立即执行；达到等待预算后原地持久化并与请求解绑 | Stateful Agent run、支持异步确认的独立 Tool 调用 |
| `ASYNC_ONLY` | 执行前持久化并立即返回 `taskId` | 显式异步 Tool/子代理、MCP 原生异步任务 |

`SYNC_ONLY` 是硬约束。请求超时后返回超时错误并请求取消，不能静默转成后台
任务。要运行超长同步请求，必须同时配置客户端、网关和服务端超时。

三种模式决定顶层调用与其调用方之间的交付边界。对话内 Tool 的 `SYNC_ONLY`
只表示它必须留在当前父 Agent 调用链内，不能单独解绑；父 `AGENT_RUN` 仍可由
Supervisor 连同当前调用栈一起原地提升后台。真正要求用户请求必须同步返回时，
应把顶层 Agent 入口设为 `SYNC_ONLY`。

### 5.2 谁设置模式

模式由入口能力、工具硬约束和部署配置设置，不由模型根据 Skill 文本决定。

优先级：

```text
入口协议和异步投递能力
> 工具硬约束
> 服务端显式配置
> 调用方 Prefer 提示
> 系统默认值
```

调用方只能在入口和工具硬约束允许的范围内选择。例如 `Prefer: respond-async` 不能覆盖
`SYNC_ONLY`。

因为当前 `Toolkit.ToolRegistration` 没有执行模式元数据，且不修改 core，第一
阶段只在托管入口使用简单的服务端策略配置：

```text
tool:<tool-name>
mcp:<server-id>/<tool-name>
agent:<agent-id>
```

普通 MCP description 和 Skill 描述只给模型阅读，不参与任务调度。MCP 自定义
执行元数据留待后续阶段；第一阶段只识别协议层已经返回的原生异步任务标识。

正常应用主要配置入口默认值，不需要逐个 Tool/MCP 配置：

```yaml
agentscope:
  background-task:
    defaults:
      agent-run:
        mode: AUTO
        foreground-wait: 10s
      standalone-tool:
        mode: AUTO
        foreground-wait: 10s
```

OpenAI 兼容无状态入口默认 `SYNC_ONLY`；Stateful Gateway 在具备可靠 session
回传时默认 `AUTO`。对话内 Tool 跟随父任务，不单独配置等待预算。只有人工确认、
必须绑定当前请求的入口，或 MCP 原生异步任务，才加入少量 `exceptions`。

### 5.3 异步投递能力门禁

有关联对话且承诺“完成后自动回到原对话”的任务，在解绑前必须确认至少存在一种
可靠回传路径：当前 channel 支持异步消息，或存在可持久化的 session wakeup
目标。能力由运行时提供，不由模型判断。

```text
AUTO + 可回传       -> 允许提升后台
AUTO + 不可回传     -> 保持原同步执行
ASYNC_ONLY + 不可回传 -> 拒绝提交，返回 ASYNC_DELIVERY_UNSUPPORTED
```

如果接口契约明确要求调用方通过任务 API 查询结果，则任务 API 本身可作为回传
路径，不要求同时具备对话唤醒能力。系统不能先承诺自动通知，再把结果留在无人
消费的后台任务里。

### 5.4 AUTO 等待时间

等待时间不是对 AI 运行时长的预测，而是请求允许保持连接的时间预算。

```text
有效等待时间 = min(
    任务类型配置上限,
    调用方声明的 wait,
    网关/请求剩余超时 - 安全余量
)
```

计时从 Supervisor 启动底层 Agent/Tool 执行时开始，模型调用、工具、子代理和结果
整理都计入。本机等待使用单调时钟；数据库时间用于审计和指标。

到达等待期限时先与 Supervisor 的终态原子竞争：如果任务刚好完成，返回普通结果；
否则完成持久化和后台提升。支持异步确认的非流式入口返回 `202 + taskId`。

SSE/WebSocket 不在 10 秒时强制断开。连接仍在时继续推送原事件；只有连接断开且
入口支持异步回传时才提升后台，重新连接后从持久事件表回放。

### 5.5 示例

用户通过 Stateful Chat 要求 Agent 生成年度销售报告，顶层 `AGENT_RUN` 默认
`AUTO`，等待预算 10 秒。Agent 在执行中调用普通工具 `report/export_report`，
该工具可以是本地 Tool 或 MCP Tool：

```text
00:00  Supervisor 启动一次原有 Agent 执行，前端正常等待
00:02  Agent 正常调用 report/export_report
00:02  ManagedToolMiddleware 在内存跟踪 TOOL 子任务 T，不改变调用结果
00:10  A 尚未完成，原子持久化 A、T、进度和路由并提升后台
00:10  Stateful Chat 返回异步确认和 A.taskId
00:40  T 完成，结果交回 A，A 继续推理并整理回复
00:42  A 完成，提交终态、事件和 outbox
00:43  最终回复进入原 session，前端对话收到消息
```

如果 A 在 6 秒内完成，对话接口的响应和未启用后台机制时完全相同，不产生用户
可见后台任务。用户查询已提升后台的 A 时能看到 T 的进度，也可以查询 T 的详情。
独立 Tool API 没有父 Agent 时，才直接按自身的
`SYNC_ONLY/AUTO/ASYNC_ONLY` 模式决定同步返回还是返回它自己的 `taskId`。

## 6. 模块与组件

新增可选扩展模块，建议位于 `agentscope-extensions`，依赖
`agentscope-harness`。应用按需启用存储、Supervisor、接口和通知适配器。

必要的 harness 修改仅限：

- 增加无法在扩展层实现的集成 SPI。
- 修复已有任务状态、回调和错误分类问题。
- 保持已有 API 兼容。

核心组件：

- `ManagedTaskService`：后台提升、查询、取消和授权。
- `ManagedTaskRepository`：任务、事件和 outbox 的事务访问。
- `ManagedAgentRunAdapter`：在支持的对话/API 入口接入 Supervisor，不修改 Agent
  core，也不改变原响应协议。
- `ManagedExecutionSupervisor`：持有底层执行的唯一订阅，协调前台观察、后台提升、
  心跳、取消和终态提交。
- `ManagedToolMiddleware`：观察本地和 MCP Tool 调用并关联父任务，不替换底层
  Tool 执行链。
- `TaskExecutionContext`：取消令牌和显式步骤/进度 API。
- `ExecutionPolicyResolver`：根据入口能力和少量服务端配置解析执行模式。
- `AsyncDeliveryCapability`：确认任务完成后是否能回到调用方或原会话。
- `AgentProgressMiddleware`：把 Agent、模型、工具和子代理事件映射为任务事件。
- `OutboxDispatcher`：通过数据库 claim/ack 可靠投递会话终态通知。
- `TaskCancelDispatcher`：本地和跨实例取消分发。
- `LostOwnerReconciler`：扫描心跳过期的 owner，把普通进程内任务条件更新为
  `FAILED/PROCESS_RESTARTED`，并为原生远程任务触发状态查询。
- `ConversationNotificationAdapter`：幂等保存最终回复并通知原 session；只有需要
  Agent 再推理的 detached 子任务结果才写 inbox 并发送 wakeup。
- `ManagedTaskController`：当前用户 REST 和 SSE 接口。
- `TaskEventProjection`：生成当前进度和父子任务摘要。

第一阶段不引入通用 `TaskWorker`、`TaskHandlerRegistry`、本地 Tool 重建执行器或
业务 Handler。MCP 原生远程异步任务使用小型协议适配器查询其 `remote_task_id`，
不扩展为通用任务重放框架。

## 7. 持久化模型

### 7.1 `managed_task`

主要字段：

- 标识与归属：`id`、`user_id`、`tenant_id`、`idempotency_key`。
- 类型与状态：`type`、`status`、`execution_mode`。前台短任务不写入该表，存在
  记录即表示已提升后台或本来就是 `ASYNC_ONLY`。
- 执行摘要：脱敏后的 `input_summary_json`、`result_json`、`remote_task_id`。
- 错误：`error_code`、`error_message`。
- 会话：`session_id`、`agent_id`、`delivery_route_json`。
- 子任务：`parent_task_id`、`root_task_id`、`parent_session_id`、
  `sub_session_id`、`spawn_depth`、`transport`。
- 进度：`progress_current`、`progress_total`、`progress_percent`、
  `progress_message`、`current_step`、`last_progress_at`。
- 执行归属：`owner_instance_id`、`heartbeat_at`。
- 时间和并发：`created_at`、`backgrounded_at`、`started_at`、`completed_at`、
  `updated_at`、`version`。

任务记录用于查询、取消、通知和审计，不用于第一阶段重建本地调用栈。`Supplier`、
lambda、打开的流、活跃 Agent state 引用和密钥不得写入数据库。

持久化的 TOOL 摘要统一保存 `toolKind = LOCAL | MCP`、`toolName` 和脱敏后的参数
摘要；MCP 额外保存 `serverId`。MCP 提供原生异步任务 ID 时写入
`remote_task_id`，用于重启后继续查询和取消。

本地 Tool 和普通 MCP Tool 在父 Agent 所属进程内继续原调用。进程退出时不由
其他实例重新解析或重放；后台任务历史保留并进入明确失败终态。

`delivery_route_json` 在任务创建时固化原 session、channel、thread/message 和
wakeup 目标，创建后不可修改，避免上下文压缩、session rotation 或嵌套子代理
把结果投递到错误会话。

显式 `ASYNC_ONLY` 提交使用 `tenant_id + user_id + target_key + idempotency_key`
防止调用方重试产生重复任务。`AUTO` 后台提升使用本次 Supervisor 生成的稳定
run id，确保阈值、断连和终态并发时最多创建一条任务记录。

### 7.2 `managed_task_event`

主要字段：

- `id`、`task_id`、`user_id`、`session_id`。
- `sequence`：任务内单调递增。
- `type`、`source`。
- `step_id`、`parent_step_id`。
- `current`、`total`、`percent`、`message`。
- `payload_json`、`created_at`。

事件类型至少包括：

- `TASK_BACKGROUND_STARTED`、`TASK_STARTED`
- `STEP_STARTED`、`STEP_PROGRESS`、`STEP_COMPLETED`、`STEP_FAILED`
- `MODEL_CALL_STARTED`、`MODEL_CALL_COMPLETED`
- `TOOL_STARTED`、`TOOL_COMPLETED`、`TOOL_FAILED`
- `SUBTASK_CREATED`、`SUBTASK_COMPLETED`、`SUBTASK_FAILED`
- `CANCEL_REQUESTED`、`TASK_CANCELLED`
- `PROCESS_LOST`
- `TASK_COMPLETED`、`TASK_FAILED`

### 7.3 `managed_task_outbox`

主要字段：

- `id`、`task_id`、`event_id`。
- `destination_type`、`destination_key`。
- `payload_json`。
- `status`、`attempt`、`next_attempt_at`、`claim_id`、`claim_until`。
- `created_at`、`delivered_at`、`last_error`。

需要可靠会话通知的状态变更，其任务状态、任务事件和 outbox 必须在同一数据库
事务中提交。外部通知不在该事务内执行，由 Dispatcher 重试投递。普通进度事件
只需与任务当前进度投影在同一事务中提交，Task SSE 从事件表读取。

outbox 状态最小集合为 `PENDING`、`CLAIMED`、`DELIVERED`、`DROPPED`。Dispatcher
使用数据库条件更新领取；只有 session 适配器确认最终回复已持久保存或目标已实际
接收后才写入 `DELIVERED`。会话正在运行、目标暂时不可用或 Dispatcher 崩溃时
释放/过期 claim 并重试，不能像当前 wakeup drain 路径一样删除后跳过。目标永久
失效或达到最大尝试次数时写入 `DROPPED`，不能伪装为已送达。Task SSE 直接读取
`managed_task_event` 并用 Redis 信号加速，不经过 outbox，也不要求客户端逐事件
ack。

## 8. 状态机

非终态：`PENDING`、`RUNNING`、`CANCEL_REQUESTED`。

终态：`COMPLETED`、`FAILED`、`CANCELLED`。

主要转换：

```text
PENDING -> RUNNING -> COMPLETED
                   -> FAILED
                   -> CANCEL_REQUESTED -> CANCELLED

PENDING -> CANCELLED
RUNNING --owner process lost--> FAILED(PROCESS_RESTARTED)
```

所有转换都使用包含预期状态和 `version` 的条件更新。终态不可修改。完成和取消
并发时，先成功提交合法转换的一方获胜，另一方重新读取终态。

`AUTO` 提升时直接创建 `RUNNING` 记录；`PENDING` 只用于执行前已经持久化的
`ASYNC_ONLY` 任务。

`RUNNING` 记录 `owner_instance_id` 和心跳。实例重启或心跳过期后，普通 Agent、
本地 Tool 和普通 MCP Tool 条件更新为 `FAILED/PROCESS_RESTARTED`，保留原进度和
事件；不自动重放。只有持有 MCP 原生 `remote_task_id` 的任务可由协议适配器继续
查询远端终态。无进展检测第一阶段只产生指标和告警，不自动终止任务。

## 9. 进度机制

### 9.1 显式业务进度

需要显式步骤的 Agent、Tool 或应用适配器可通过 `TaskExecutionContext` 上报：

```text
stepStarted(stepId, name, total)
reportProgress(current, total, message)
stepCompleted(stepId, resultSummary)
stepFailed(stepId, error)
checkCancelled()
```

任务已提升后台时，每次上报写入事件表并更新当前投影，事务提交后推送 Task SSE。
仍处于前台时只更新 Supervisor 的内存快照，避免正常短对话同步写库。

### 9.2 自动 Agent 进度

`AgentProgressMiddleware` 观察已有 AgentScope 事件，不修改 core，自动记录：

- Agent reply 开始和结束。
- 模型推理开始和结束。
- 工具调用开始、结束和失败。
- 子代理创建、进度和终态。

自动事件用于回答“当前在做什么”和“哪些步骤已完成”。第一阶段默认不计算聚合
百分比；只有 Tool/MCP 自身提供明确 `current/total` 时才展示其原始百分比。

### 9.3 父子任务聚合

父任务展示各状态的子任务数量，例如“5 个子代理中 2 个已完成”。第一阶段不做
权重配置和父任务百分比聚合。

### 9.4 Tool 进度边界

框架对所有 Tool 都能记录开始调用、等待、完成或失败。普通本地 Tool 如果
没有调用 `TaskExecutionContext`，以及普通同步 MCP 如果没有 progress
notification，框架都不能推测其内部步骤。

本地 Tool 可选择使用 `TaskExecutionContext`，MCP 可提供 progress notification
或原生任务进度接口；只有这些信号存在时才映射更细的步骤事件。Java 侧不会要求
每个 Tool 编写专用 Handler，也不会通过轮询模型文本伪造进度。

## 10. 异步子代理

`SUBAGENT` 任务必须持久化：

- `parent_task_id`、`root_task_id`
- `parent_session_id`、`sub_session_id`
- `agent_id`、`spawn_depth`
- `transport = LOCAL | AGENT_PROTOCOL`
- 可序列化的 Agent/handler 标识和输入

支持的父任务等待方式：

- `WAIT_ALL`：全部子任务终态后继续。
- `DETACHED`：父任务立即继续，子任务结果通过 inbox/wakeup 返回。

父任务取消默认级联到未完成的子任务。只有提交时明确声明独立归属的 detached
子任务可以不级联。

顶层 Agent 显式派生的异步子代理默认 `ASYNC_ONLY`；编排型子代理内部需要子任务
结果继续推理时默认 `WAIT_ALL`。模型不能通过自然语言绕过这一运行时规则。

## 11. 取消与重启处理

### 11.1 取消

- `PENDING`：原子转为 `CANCELLED`。
- `RUNNING`：先转为 `CANCEL_REQUESTED`，通过 Redis/数据库轮询通知
  `owner_instance_id` 对应的 Supervisor。
- 顶层 Agent 取消沿用 Reactor subscription/cancellation token，并默认级联取消
  未完成的非独立 Tool 和子代理任务。
- 本地 Tool 通过协作式 cancellation token 停止；不支持令牌的旧 Tool 只能进行
  best effort 线程/订阅取消。
- MCP 原生支持取消时转发远程取消；普通 MCP 调用只能取消本地等待和连接，属于
  best effort。
- 远程子代理通过 Agent Protocol 发送取消请求。
- 只有执行真正停止或远端确认后才写入 `CANCELLED`。
- 无法立即停止时保持 `CANCEL_REQUESTED`，不能向用户伪报已取消。

### 11.2 进程重启

实例启动时和运行期间扫描心跳过期的 `RUNNING/CANCEL_REQUESTED` 任务：

1. 普通 Agent、本地 Tool、普通 MCP Tool 和本地子代理原子转为
   `FAILED/PROCESS_RESTARTED`。
2. 保留最后进度、完整事件和失败通知，用户重启后仍可查询。
3. 已持久化 MCP 原生 `remote_task_id` 的任务重新连接远端状态接口，只查询或
   转发取消，不重新提交工具调用。
4. 远程 Agent Protocol 任务仅在协议明确支持按原 task id 查询时采用相同方式。

普通任务自动重放、幂等重试和用户主动 retry API 不进入第一阶段，避免重复执行
已经发生的模型调用和外部副作用。

## 12. 用户接口

`user_id/tenant_id` 必须来自认证上下文，不能由请求参数任意指定。

```text
GET    /v1/background-tasks
GET    /v1/background-tasks/{taskId}
GET    /v1/background-tasks/{taskId}/events
GET    /v1/background-tasks/{taskId}/events/stream
DELETE /v1/background-tasks/{taskId}
```

列表支持状态、类型、session、创建时间和 cursor 分页。详情包含当前步骤、进度、
执行实例/重启失败信息、父子任务摘要以及脱敏结果或错误。默认列表只展示已提升
后台的根任务；详情或 `includeChildren=true` 展示 Tool/MCP/子代理子任务。

Admin API 与用户 API 分离并单独授权。现有 Admin 子代理任务接口可以逐步适配
统一服务，但不能作为用户鉴权边界。

## 13. 通知与对话回传

### 13.1 通知通道

持久化 `TaskEvent` 是主契约，可配置以下适配器：

- Task SSE，支持 `Last-Event-ID` 回放。
- session 最终回复/通知。
- detached 子任务所需的 inbox + wakeup。

Redis pub/sub 可加速实时 SSE，断线回放必须读取数据库事件表。

后台阶段的步骤和任务事件全部持久化并通过 Task SSE 通知。它们默认不转换成聊天
消息，避免每个模型或工具事件干扰正常对话。顶层终态通知不合并、不丢弃；
Dispatcher 至少投递一次，session 消费者按幂等键只产生一次用户可见结果。

### 13.2 对话回传

有关联 `session_id` 的任务完成后：

```text
Supervisor 提交顶层终态 + TaskEvent + outbox
  -> OutboxDispatcher claim 通知
  -> ConversationNotificationAdapter 幂等保存最终 Agent 回复
  -> 向原 session 发布完成信号/消息
  -> 实际保存或接收成功后 ack outbox
```

失败和取消使用同一链路投递结构化终态通知，修复 Python 版可能跳过通知的问题。

对话内普通 Tool 结果通过原调用链直接交回父 Agent，不走 inbox/wakeup。原生
异步 Tool 或 detached 子代理终态才投递给父 `AGENT_RUN`，用于恢复父任务或发起
新的推理轮次。只有顶层 `AGENT_RUN` 向原对话投递最终终态；Task SSE 仍可展示
所有父子任务事件。独立 Tool API 没有父任务时，才直接通知其调用方。

投递幂等键由 `taskId + terminalEventId + destination` 组成。会话正在执行时不能
删除通知并跳过，而应保持/释放 claim 后延迟重试。通知失败不回滚任务终态，
outbox 保留记录并继续重试。消费者必须根据任务提升时固化的
`delivery_route_json` 正向校验归属；无法证明归属时拒绝消费并记录投递失败。

前端在线时收到实时事件；断线后可通过 `Last-Event-ID` 补齐 TaskEvent，并从
会话存储重新加载持久化对话消息。进程重启和实例切换不影响历史查询。

## 14. 多实例协调

- Supervisor 在后台提升时写入 `owner_instance_id`，只有所属实例能更新执行心跳
  和普通任务终态。
- 实例丢失后其他实例只能将普通进程内任务标记为 `PROCESS_RESTARTED`，不能领取
  并重放执行。
- Outbox Dispatcher 通过数据库条件更新和 claim 超时实现多实例竞争消费。
- Redis 可发送“有新任务”、取消和实时事件信号。
- Redis 信号丢失时，数据库轮询作为正确性兜底。
- 同一 session 的 Agent 状态更新继续使用现有分布式 session 锁串行化。

## 15. 安全与保留

- 任务、事件和取消都校验用户及租户归属。
- payload、result、error 和事件支持配置化脱敏。
- 密钥和模型凭证不得写入任务 payload。
- 取消和管理员操作写入审计事件。
- 终态历史建议默认保留 30 天，可配置归档和更长审计周期。

## 16. 与现有 Java 机制的集成

1. 在 Stateful Gateway/Chat 入口增加 `ManagedAgentRunAdapter`，由 Supervisor
   持有现有 Agent 执行的唯一订阅；前台请求观察同一事件流并按预算等待。
2. OpenAI 兼容无状态 Chat Completions 和未启用托管的入口保持原调用与响应协议，
   不默认返回 `202`。
3. 在统一 Tool 调用边界增加 `ManagedToolMiddleware`；Agent、Skill、本地 Tool
   和 MCP Tool 保持现有调用方式。中间件只跟踪父子关系和事件，不替换底层执行
   链，也不注入后台占位 ToolResult。
4. 将 `AgentSpawnTool` 逐步适配到 `ManagedTaskService`，保留远程 Agent
   Protocol 能力。
5. 托管 `AGENT_RUN` 内禁用现有 `AsyncToolMiddleware` 的超时占位/二次 wakeup
   行为；独立 Tool API 和明确的原生异步 Tool 走新的顶层任务适配器。
6. 普通 Tool 结果沿原调用链返回；inbox/wakeup 只保留给 detached 子代理和原生
   异步任务结果，以数据库事件和 outbox 提供可靠性。
7. 修复现有 wakeup 队列“drain 后遇到运行中 session 直接跳过”的丢通知窗口，
   改为 claim、实际消费后 ack、忙时重试。
8. 不修改 `agentscope-core` 时，托管保证范围限定为经过 Managed Gateway/Harness
   的调用；应用直接调用 core Agent/Tool 不做全局拦截承诺。

## 17. 分阶段实施

### Phase 0：无感执行基线

- 为现有 Stateful Gateway 增加可关闭的 Supervisor/Adapter，不改变 Agent API。
- 验证同一 Agent 执行只订阅一次，前台断开不会触发重复执行。
- 短对话保持原响应、SSE、错误和模型上下文。
- 后台能力不可用时保持同步执行，不影响正常对话。

### Phase 1：后台提升和持久历史

- Repository SPI 和首个 SQL 实现。
- task、event、outbox 数据库迁移。
- `AUTO` 阈值/断连与任务终态的原子竞争。
- 提升时持久化 Agent、活动 Tool/子代理、进度快照和不可变投递路由。
- Supervisor owner 心跳、取消令牌和 `PROCESS_RESTARTED` 终态处理。
- `LostOwnerReconciler` 扫描失联实例，不接管或重放普通任务。

### Phase 2：用户查询和通知

- 当前用户列表、详情、事件、取消接口。
- Task SSE 实时推送和回放。
- 带 claim/ack/drop 的 Outbox Dispatcher 和对话回传适配器。
- 修复 session busy 时的 wakeup 丢失，完成、失败、取消均可靠通知。

### Phase 3：AgentScope 集成

- Agent run 和本地/MCP Tool 统一跟踪，验证无需定制提交工具或 Handler。
- 对话内 Tool 不独立超时、不注入占位结果，结果仍交回同一父 Agent。
- 本地及远程异步子代理。
- 异步投递能力门禁和不可变 session 路由。
- Agent、模型、工具、子代理事件自动映射。

### 后续候选：按实际需求增加

- 普通任务幂等重放、自动/用户重试和通用 Worker。
- Webhook/MQ 通知适配器。
- MCP 自定义执行模式元数据。
- `WAIT_ANY` 和带权父任务百分比聚合。
- 自动卡死终止策略。
- 历史归档和清理。
- 更完整的指标、告警和压力测试。

## 18. 测试范围

- 完成与取消并发的状态转换测试。
- 前台完成与后台提升并发测试，确保只产生一次结果和至多一条任务记录。
- 单次底层订阅测试，确保转换不会重新执行 Agent/Tool。
- 短对话启用前后响应体、SSE 序列、错误和模型上下文等价测试。
- 后台存储/通知不可用时的同步降级测试。
- owner 实例丢失后 `PROCESS_RESTARTED` 和历史保留测试。
- outbox 崩溃窗口和重复投递测试。
- outbox 领取超时、重试耗尽和 `DROPPED` 测试。
- session 正在运行时通知不丢失并延迟重试测试。
- 用户/租户数据隔离测试。
- SSE 断线、回放、游标和慢消费者测试。
- 三种模式及配置优先级测试。
- OpenAI 兼容无状态接口默认保持同步协议测试。
- Stateful Gateway 具备投递能力时自动提升后台测试。
- 对话内本地/MCP Tool 跟随父任务且不产生占位 ToolResult 测试。
- MCP 原生 `remote_task_id` 重启后继续查询/取消测试。
- 异步投递能力缺失时的同步回退和拒绝测试。
- 幂等键重复提交测试。
- Supervisor 存活但任务无进展的指标/告警测试。
- 不调用 Tool 的长耗时 Agent run 后台化、查询、取消和通知测试。
- `AGENT_RUN -> TOOL/SUBAGENT` 父子关系及顶层单次对话终态投递测试。
- 成功、失败、取消回传原对话的测试。
- 路由快照隔离和对话进度合并测试。
- 父子任务聚合和级联取消测试。
- 本地子代理进程丢失失败、远程子代理按原 task id 查询测试。
- Redis 故障、数据库轮询兜底测试。
- 数据脱敏和保留策略测试。

## 19. 初始默认值

- Stateful Gateway Agent run：具备可靠 session 回传时默认 `AUTO`，初始等待预算
  10 秒，可配置。
- OpenAI 兼容无状态 Chat Completions：默认 `SYNC_ONLY`，保持原协议。
- SSE/WebSocket：连接存在时持续流式返回，断连且可回传时才提升后台。
- 对话内普通本地/MCP Tool：跟随父 `AGENT_RUN`，不设置独立等待预算。
- 独立 Tool API：只有接口支持异步确认时可配置 `AUTO`。
- MCP 原生异步任务：识别协议返回的 task id 后使用 `ASYNC_ONLY`。
- 显式异步子代理：`ASYNC_ONLY`。
- 编排型子代理内部派生任务：默认 `WAIT_ALL`。
- 普通 Agent/Tool 自动重放和自动重试：第一阶段不支持。
- 父任务取消：默认级联到非独立子任务。
- 终态历史：默认保留 30 天，可配置。
- 数据库负责正确性；Redis 负责加速和实时信号。

## 20. 验收标准

1. 登录用户只能查询和操作自己的后台任务。
2. 执行进程重启后，任务终态、进度和事件历史仍可查询。
3. 普通进程内任务在 owner 丢失后进入 `FAILED/PROCESS_RESTARTED`，不会被其他
   实例自动重放；MCP/远程 Agent 原生任务只按原 task id 查询。
4. 未达到后台阈值的正常对话，其响应结构、HTTP 状态、SSE 事件顺序、错误语义、
   Tool 结果和模型上下文与未启用该机制时一致。
5. 后台转换不会重新订阅、重新排队或重新执行已经开始的 Agent/Tool。
6. 对话内普通 Tool/MCP 不注入后台占位结果，不触发独立的 10 秒超时。
7. 后台存储或通知不可用时保持同步执行，不能导致正常对话失败。
8. 用户取消后可以观察到准确的取消请求和最终结果。
9. 后台阶段每个已上报步骤和终态都能持久化并通过 Task SSE 回放。
10. 关联对话的任务完成、失败或取消后，原对话都能收到持久通知。
11. session 正在运行、Dispatcher 崩溃或重复投递时，通知不会丢失且不会产生重复
    用户消息。
12. 后台 Agent 或工具不会隐式变成子代理。
13. `SYNC_ONLY` 不会被静默转换为后台任务。
14. 承诺自动回传原对话的任务在没有可靠投递能力时不会被后台化。
15. 重复幂等提交或并发后台提升不会产生两次执行。
16. 普通 Agent 只需按现有方式调用本地或 MCP Tool，不需要业务提交工具或专用
    Handler 即可获得托管、查询、取消和通知能力。
17. 整轮 Agent 执行由 Supervisor 管理；即使不调用 Tool，也支持无感后台提升、
    查询、取消和通知。
18. 顶层 Agent、Skill 引导调用和子代理发起的本地/MCP Tool 都进入同一托管
    边界，且一次逻辑调用只产生一个任务。
19. 对话场景只由顶层 `AGENT_RUN` 投递最终回复，子任务终态不会产生重复回复。
20. 不修改 `agentscope-core` 时，所有保证限定于 Managed Gateway/Harness 入口，
    且已有未托管入口保持兼容。
