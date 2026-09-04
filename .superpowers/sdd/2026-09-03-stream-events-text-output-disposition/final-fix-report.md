# 最终修复接管报告

日期：2026-09-04

工作树：`D:\ai-code\agentscope-java\.worktrees\stream-events-text-disposition`

分支：`codex/stream-events-text-disposition`

## 接管时状态

- 接管 HEAD：`d14a735b87705e83161b95e444cb24470cdba8bb`（`docs(streaming): 修正事件序列与兼容性证据`）。
- 工作树位于独立 git worktree，未在 `main` 上工作。
- 接管时共有 14 个已修改、未提交文件；`git diff --stat` 为 411 insertions / 66 deletions。
- 仓库根不存在 `.codegraph/`，因此按 AGENTS.md 跳过 CodeGraph，使用 `git diff`、`rg` 和逐文件读取审计。
- 未执行 reset、checkout、stash、清理或覆盖前代理改动；本轮只在现有实现上补齐缺口。
- 接管文件中 `SessionEventMapperTest` 的额外修改经审计判定为必要且正确，但它当时只有回归测试、缺少对应生产修复。本轮因此新增修改 `SessionEventMapper.java`，最终在途文件数由 14 变为 15（本报告另计）。

## 最终评审 findings 逐项处理

### 1. Critical：真实 replyId、AgentEnd 关联及子 Agent outcome

审计结论：finding 正确，接管改动方向正确。

- `AgentEventStreams` 不再要求 `AgentEndEvent.replyId` 等于当前模型 replyId，而是按 `source + metadata.taskId` 取得该 invocation 的最后一个未处置可见 reply。
- 顶层调用继续暂存 `AgentEndEvent`，正常完成时按 `AgentResultEvent -> TERMINAL -> AgentEndEvent` 输出；真实 `ReActAgent` 的模型 reply ID 与调用 End ID 不同也能关闭模型 reply。
- `AgentEndEvent` 增加合成调用 outcome 元数据常量：`success`、`error`、`cancelled`。
- `AgentSpawnTool` 在本地同步子 Agent 的合成 End 上标注 outcome：
  - `doOnSuccess` 标注 `success`；
  - `doOnError` 标注 `error`；
  - `doFinally(CANCEL)` 标注 `cancelled`；
  - `AtomicBoolean` 保证竞争条件下只发一个 End。
- `AgentEventStreams` 只允许 `success` 或兼容的未标记 End 生成 `TERMINAL`；`error`、`cancelled` 以及未来未知非 success outcome 均不会把失败/取消文本伪装成成功终态。
- 覆盖测试：
  - `AgentEventStreamsTest.realReActAgentClosesLastModelReplyAtInvocationEnd` 使用真实 `ReActAgent` + `MockModel`，并明确断言模型 reply ID 与 AgentEnd ID 不同；
  - `HarnessAgentSubagentStreamEventsTest` 使用真实 Harness/ReAct 路径验证子 Agent 事件转发与 taskId；
  - `AgentSpawnToolCancelEndEventTest` 覆盖 success/error/cancel 三种 outcome，并通过真实 `AgentSpawnTool` 转发路径断言只有 success 产生 `TERMINAL`。

### 2. Important：Agent Protocol 真实入口包装同一 raw Flux

审计结论：finding 正确，接管实现满足要求。

- `AgentProtocolTaskStore` 在真实 `submit` 路径中直接执行：
  `AgentEventStreams.withTextOutputDisposition(agent.streamEvents(msg, ctx))`。
- 没有构造第二条 agent 流，也没有手工注入 disposition。
- `AgentProtocolStreamDetailTest.agentRun()` 只返回 raw Agent 事件；`TEXT_OUTPUT_DISPOSITION` 由生产包装器派生。
- 测试用 `Flux.defer` 计数并断言 `agent.streamEvents` 仅订阅一次；同时验证真实模型 reply ID 被写入派生 disposition。

### 3. Important：AG-UI 同 reply 新 segment 继承/补发 disposition

审计结论：finding 正确，接管实现满足要求。

- `AguiStreamContext` 按 replyId 保存最近一次 `TextOutputDisposition` 和 generate reason。
- 收到 Core disposition 时立即发 Custom，并保存状态。
- 工具后同一 reply 出现新文本 segment 时，在新的 `TextMessageStart` 后补发同一 disposition Custom；`messageIds` 包含当前已知的全部 segment ID。
- `TextOutputDispositionConverter` 只委托 context 处理，不生成 Reasoning。
- `testToolFollowupSegmentInheritsSingleIntermediateDisposition` 的 raw Core 输入只会派生一次 `INTERMEDIATE`；AG-UI 输出两个 Custom（首次通知 + 新 segment 补发），第二个 Custom 带两个 messageIds。测试没有手工放入第二个 Core `INTERMEDIATE`。

### 4. Important：前端按 eventId 记忆 INTERMEDIATE 并保持 wire 顺序

审计结论：finding 正确，接管实现满足要求。

- `knownDispositions[eventId]` 记住首次 `INTERMEDIATE` / `TERMINAL`。
- disposition 先于后续 start/delta 时，后续 `agent.message` delta 会直接进入 Commentary，而不会重新进入 pending。
- `activeCommentarySegmentIds` 维护同一 wire segment；遇到工具事件时关闭当前 active commentary，工具后的同 eventId delta 会创建新的 Commentary block，并按到达顺序插入 `displayOrder`。
- 权威 `agent.message` 到达时清理所有 pending preview，再追加 final；不会遗留 pending/preview block。
- `preserves wire order and presentation across repeated reply segments and tools` 只发送一次 `INTERMEDIATE` update，之后连续发送工具和同 eventId delta，断言顺序为 Commentary -> Tool -> Commentary -> Final。

### 5. Minor：计划中的 npm 命令

审计结论：finding 正确，接管修改必要。

- 定点命令改为 `npm test -- ChatPanel.test.tsx`。
- 全量命令改为 `npm test`，不再向已经包含 `vitest run` 的脚本重复传 `--run`。

### 附加发现：DataPlane 的真实 AgentEnd ID 不一致

`SessionEventMapperTest.agentEndReusesLastModelPreviewWhenInvocationReplyIdDiffers` 必须保留。它捕获的是同一根因在 DataPlane 消费端的表现：preview ID 绑定模型 reply，而真实 AgentEnd 携带 invocation reply ID。

接管时生产代码仍按 `AgentEndEvent.replyId` 查 preview，因此测试真实失败。最终修复如下：

- `PreviewIds` 在收到 `TERMINAL` disposition 时，按 invocation key 保存已经明确终结的模型 `PreviewKey`；
- `AgentEndEvent` 提交权威结果时优先消费该 terminal preview；
- 若没有 terminal 记录，则回退到旧的 End replyId 查找，保留未标记兼容路径；
- 不扫描或猜测任意旧 preview，避免把结构化-only 结果错误绑定到无关历史文本。

## RED / GREEN 证据

### 本轮新跑 RED

`SessionEventMapperTest` 是本轮唯一新增生产修复对应的可诚实重现 RED：

1. 首次 Maven 命令因 PowerShell 未引用 `-Dsurefire.failIfNoSpecifiedTests=false`，被拆成无效生命周期参数；Tests run: 0，不计为 RED。
2. 修正命令后默认 JDK 21 被 Maven Enforcer 拒绝；Tests run: 0，不计为 RED。
3. 切换到进程级 JDK 17 后，接管测试文件先被 Spotless 格式检查阻断；JUnit 未运行，不计为 RED。用 `apply_patch` 只调整格式。
4. 真正 RED：`SessionEventMapperTest` 运行 17 个测试，1 failure、0 errors：
   - `agentEndReusesLastModelPreviewWhenInvocationReplyIdDiffers`
   - 期望模型 preview 的 `evt_*`，实际得到新分配的另一个 `evt_*`。

### 本轮新跑 GREEN

- DataPlane 首次实现后：17 tests，0 failures/errors。
- 调整 terminal preview 优先级并完成格式修正后再次新跑：17 tests，0 failures/errors，15 个 reactor 模块 SUCCESS。
- 其余五个评审 finding 的代码在接管前已经存在，因此本轮不伪造“先撤销实现再跑 RED”。其原始 RED 可查同目录 `task-*.md`；本报告只把本轮实际新跑的验证列为 GREEN。

## 本轮命令与结果

所有成功 Maven 测试和 Spotless 命令均只在命令进程内设置：
`JAVA_HOME=C:\Program Files\Java\jdk-17`，未修改机器或仓库持久配置。

| 命令 | 结果 |
| --- | --- |
| `mvn -pl agentscope-core -Dtest=AgentEventStreamsTest,TextOutputDispositionEventTest,ReplyLifecycleTrackerTest,FinalAnswerFilterMiddlewareTest,ReActAgentNewLoopReplyTest test` | PASS；41 tests，0 failures/errors。包含真实 ReActAgent ID 差异和未包装默认事件序列。 |
| `mvn -pl agentscope-harness -am -Dtest=AgentEventStreamsTest,AgentSpawnToolCancelEndEventTest,HarnessAgentSubagentStreamEventsTest -Dsurefire.failIfNoSpecifiedTests=false test` | PASS；Core 17 tests，Harness 9 tests，0 failures/errors；3 模块 SUCCESS。 |
| `mvn -pl agentscope-extensions/.../agentscope-extensions-agent-protocol,agentscope-extensions/.../agentscope-extensions-agui -am -Dtest=AgentProtocolStreamDetailTest,AguiAgentAdapterV2Test -Dsurefire.failIfNoSpecifiedTests=false test` | PASS；AG-UI 102 tests、Agent Protocol 3 tests，0 failures/errors；7 模块 SUCCESS。 |
| `mvn -pl agentscope-service/service-dataplane -am -Dtest=SessionEventMapperTest -Dsurefire.failIfNoSpecifiedTests=false test` | 最终 PASS；17 tests，0 failures/errors；15 模块 SUCCESS。 |
| `npm test -- ChatPanel.test.tsx` | PASS；1 file，6/6 tests。 |
| `npm test` | PASS；1 file，6/6 tests。 |
| `npx tsc --noEmit --pretty false --jsx react-jsx --target ES2022 --module ESNext --moduleResolution Bundler --allowSyntheticDefaultImports --esModuleInterop --skipLibCheck src/api/managedSessions.ts src/components/ChatPanel.tsx src/components/ChatPanel.test.tsx src/components/MessageBlock.tsx` | PASS，退出码 0。 |
| `npm run build` | FAIL（已知基线）：两个 `src/features/build/**` import 目标不存在，见 concerns。 |
| `mvn spotless:check -DskipTests` | 首次发现本轮 DataPlane 三元表达式格式问题；用 `apply_patch` 修正后重跑 PASS，91 个 reactor 模块 SUCCESS。 |
| `git diff --check` | PASS，无空白错误。 |

## Concerns / 环境限制

### 本轮新复现

- 默认 `JAVA_HOME` 指向 JDK 21，项目 Maven Enforcer 要求 JDK 17。本轮所有有效 Maven 验证均显式使用已安装的 `C:\Program Files\Java\jdk-17`。
- `npm run build` 在 2026-09-04 新复现以下基线错误：
  - `src/main.tsx(54,29): TS2307`，缺少 `./features/build/deployments/DeploymentsPage`；
  - `src/pages/AgentsHubPage.tsx(17,25): TS2307`，缺少 `../features/build/agents/AgentsHubPage`。
- 两个目标文件均实际不存在，且被根 `.gitignore:27` 的 `build/` 规则匹配。按接管要求未创建占位页面；目标文件独立 TypeScript 检查已通过。

### 仅引用 2026-09-03 既有验证，未在本轮冒充重跑

同目录 `task-9-report.md` 已记录完整模块回归的 Windows 环境阻断：

- `DangerousPathBypassTest.symlinkToDotEnvIsDetected`
- `DangerousPathBypassTest.symlinkToSshIsDetected`

两例因当前 Windows 账号缺少创建 symlink 的特权报错。

Harness 另有三个依赖 Unix `sh` 的环境用例：

- `DockerSandboxCommandTest.execProcessDestroysTheHostProcessOnTimeout`
- `DockerSandboxCommandTest.execProcessDestroysTheHostProcessWhenWritingFails`
- `DockerSandboxCommandTest.execProcessDestroysTheHostProcessWhenInterrupted`

既有报告中，排除上述环境用例后 Core、Harness、Agent Protocol、AG-UI、DataPlane 全部通过。本轮按最终修复要求重新执行了受影响目标测试，但没有把 2026-09-03 的全量结果表述为本轮新跑。

### 其他基线提示

- Maven 持续提示 `agentscope-extensions-aistio` 未声明 `maven-resources-plugin` 版本。
- 测试日志持续包含既有 deprecated API、SLF4J 无 provider 和 CDS 提示；没有观察到本轮新增警告或断言失败。

## 最终判断

- 五项最终评审 finding 均已落实且有本轮新跑的目标验证。
- `SessionEventMapperTest` 必要、行为断言正确，并已通过对应生产修复完成 RED -> GREEN。
- 默认未 opt-in 的 `ReActAgent#streamEvents()` 事件序列仍由 `ReActAgentNewLoopReplyTest.unwrappedTextOnlyStreamPreservesLegacySequenceWithoutDisposition` 保护。
- 未 push、merge、publish；未创建无关基线文件。
- 提交 SHA 范围在提交完成后的最终交接消息中给出。
