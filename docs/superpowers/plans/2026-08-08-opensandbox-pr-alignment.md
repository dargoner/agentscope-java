# OpenSandbox Java 对齐与沙箱 PR 改进实施规划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不改变当前“不处理 MCP”边界的前提下，将 Java Harness 沙箱的关键并发、恢复、文件编辑和路径安全能力与 Python OpenSandbox 实现及相关 Java PR 对齐。

**Architecture:** OpenSandbox 继续复用 `AbstractBaseSandbox` 的 workspace projection/snapshot 生命周期和官方 Java SDK 的 native Filesystem API。先补跨 provider 的 Harness 回归修复，再为 OpenSandbox 增加服务端超时后的恢复能力；`volumes`、metadata discovery、pause/resume 等 Python 专属能力作为独立增强，不和本次修复混在一起。

**Tech Stack:** Java 17、Maven、JUnit、Harness Sandbox SPI、OpenSandbox Java SDK 1.0.18、Jackson。

---

## 评审结论

### 直接值得纳入 Java 的改进

1. **Java PR #2586：高优先级，移植到 OpenSandbox。**
   当前 `OpenSandbox.ensureSandbox()` 在 SDK 404 后只调用 `state.setWorkspaceRootReady(false)`，没有清空 `workspaceProjectionHash`。重建出来的是空沙箱，旧 hash 会让 `AbstractBaseSandbox.applyWorkspaceProjectionIfChanged()` 跳过重新投影，导致工作区文件丢失。E2B PR 的修复逻辑适用于 OpenSandbox，但应增加 OpenSandbox 自己的回归测试，不应直接假设 E2B 测试覆盖它。

2. **Java PR #2573：高优先级，直接适用于 OpenSandbox。**
   当前 `BaseSandboxFilesystem.edit()` 仍把 `\\n` 拼进 `python3 -c` 字符串；POSIX shell 会把它传成字面量反斜杠-n，编辑命令可能 SyntaxError。PR 还补了非零退出码和空退出码处理。OpenSandbox 走该通用 filesystem，因此应移植完整实现和测试。

3. **Java PR #2491：高优先级，直接适用于所有 sandbox-backed filesystem。**
   当前 `SandboxLifecycleMiddleware` 仍用 agent 级 `ConcurrentHashMap<sessionKey, SandboxAcquireResult>`，filesystem 仍按 session key 的共享 map 取 sandbox。不同 session 可以并发，但同一 agent 的跨调用绑定/释放仍可能覆盖或清理错误对象。PR 的 per-call `RuntimeContext` 绑定和 CAS 清理是正确方向；PR 已关闭未合并，应该移植其设计和测试，而不是机械 cherry-pick。

4. **Java PR #2567：中优先级，直接适用于 OpenSandbox。**
   `MemoryFlushManager.resolveOffloadPath()` 当前无 sandbox 判断，会把宿主侧 session archive 路径写进 agent 可见的摘要。OpenSandbox agent 无法访问宿主路径，应按 PR 返回空路径，同时保留本地 filesystem 的原有提示，并补 sandbox-backed regression test。

### 只需确认已同步、无需再次合并

- **#2557 已合并且在当前 `HEAD` 的祖先链中。** Docker fallback 的 tar 流上传和路径边界校验已经是现状；OpenSandbox 的 native file transfer 不依赖该 fallback。
- **#2530 已合并到当前主线祖先链。** 它只优化 Docker 大文件流式传输，不直接改 OpenSandbox。

### 仅供参考或暂不纳入本次修复

- **Python #2215：值得设计，但不是直接 cherry-pick。** Python cache 命中时用 `is_healthy()` 驱逐被服务端 timeout 杀掉的 workspace；Java 当前没有 OpenSandbox manager cache，只有持久化 sandbox ID。先实现 404 恢复，再单独评估 health probe/cache。
- **Python #2198/#2194：最值得作为后续增强。** `use_server_proxy` Java 已有；`volumes`、env、metadata、network policy Java 尚未暴露。优先补 `volumes`，前提是确认 SDK 1.0.18 的 Java model/API。
- **Java #2196、#2555：不适用于 OpenSandbox。** 分别是 E2B native binary I/O 和 E2B snapshot retention；OpenSandbox 已使用 native read/write，且当前没有 E2B snapshot API。

## 文件范围

**本次修复候选文件：**

- Modify: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/src/main/java/io/agentscope/extensions/sandbox/opensandbox/OpenSandbox.java`
- Test: `agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox/src/test/java/io/agentscope/extensions/sandbox/opensandbox/OpenSandboxTest.java`
- Modify: `agentscope-harness/src/main/java/io/agentscope/harness/agent/filesystem/sandbox/BaseSandboxFilesystem.java`
- Test: `agentscope-harness/src/test/java/io/agentscope/harness/agent/filesystem/sandbox/BaseSandboxFilesystemTest.java`
- Modify: `agentscope-harness/src/main/java/io/agentscope/harness/agent/filesystem/sandbox/SandboxBackedFilesystem.java`
- Modify: `agentscope-harness/src/main/java/io/agentscope/harness/agent/middleware/SandboxLifecycleMiddleware.java`
- Test: `agentscope-harness/src/test/java/io/agentscope/harness/agent/filesystem/sandbox/SandboxBackedFilesystemTest.java`
- Test: `agentscope-harness/src/test/java/io/agentscope/harness/agent/middleware/SandboxLifecycleMiddlewarePerCallBindingTest.java`
- Modify: `agentscope-harness/src/main/java/io/agentscope/harness/agent/memory/MemoryFlushManager.java`
- Test: `agentscope-harness/src/test/java/io/agentscope/harness/agent/memory/MemoryFlushManagerOffloadTest.java`

**后续增强候选文件（本次不改）：**

- `...opensandbox/OpenSandboxClientOptions.java`
- `...opensandbox/OfficialOpenSandboxSdk.java`
- `...opensandbox/OpenSandboxState.java`
- `...opensandbox/OpenSandboxClient.java`
- `...opensandbox/OpenSandboxFilesystemSpec.java`

## 实施顺序

### Task 1: 固化 OpenSandbox 404 重建的 projection 回归

- [ ] 在 `OpenSandboxTest` 增加测试：已有 sandbox ID，`connect` 抛 404，`start()` 触发 create，并断言 `state.getWorkspaceProjectionHash()` 为 `null`、`workspaceRootReady` 为 `false`。
- [ ] 增加对非 404 connect 异常不重建的测试，保持当前错误分类契约。
- [ ] 实现重建分支：在 `state.setWorkspaceRootReady(false)` 后立即调用 `state.setWorkspaceProjectionHash(null)`，再 create/assign。
- [ ] 运行：

```powershell
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox -am -Dtest=OpenSandboxTest -Dsurefire.failIfNoSpecifiedTests=false test
```

### Task 2: 移植 #2573 的通用 edit 修复

- [ ] 在 `BaseSandboxFilesystemTest` 先添加失败用例：fake sandbox 记录 edit command，断言 command 中包含实际换行；再添加非零退出码返回明确 `EditResult.fail` 的用例。
- [ ] 将 `BaseSandboxFilesystem.edit()` 的 Python 程序改成 Java 字符串中的真实 `\n`，保留 heredoc/base64 payload；命令退出码非零时直接返回截断后的错误信息。
- [ ] 验证 replace-all、未找到字符串、多次匹配和 `exitCode == null` 的既有语义未变。
- [ ] 运行：

```powershell
mvn -pl agentscope-harness -Dtest=BaseSandboxFilesystemTest -Dsurefire.failIfNoSpecifiedTests=false test
```

### Task 3: 移植 #2491 的 per-call RuntimeContext 绑定

- [ ] 先运行现有 sandbox middleware 测试，确认当前共享 map 的基线。
- [ ] 将 `SandboxLifecycleMiddleware` 的 acquire result 从 agent 级 map 移入 `RuntimeContext`，并把 `Sandbox` 同步写入 context；release 只读取并清理当前 context。
- [ ] 将 `SandboxBackedFilesystem` 的解析顺序改为“当前 RuntimeContext sandbox 优先，context-less maintenance path 使用 fallback slot”；fallback 清理使用 `AtomicReference.compareAndSet(expected, null)`。
- [ ] 保留同一 session 的串行化前置条件，不在本任务中新增分布式锁；补并发测试，证明两个 RuntimeContext 的 execute、release 不会互相覆盖或释放。
- [ ] 运行：

```powershell
mvn -pl agentscope-harness -Dtest=SandboxBackedFilesystemTest,SandboxLifecycleMiddlewareTest,SandboxLifecycleMiddlewarePerCallBindingTest -Dsurefire.failIfNoSpecifiedTests=false test
```

### Task 4: 隐藏 sandbox agent 不可访问的宿主 offload 路径

- [ ] 在 `MemoryFlushManagerOffloadTest` 增加 sandbox-backed filesystem 用例，断言 `resolveOffloadPath()` 返回空字符串；增加 local filesystem 用例，断言原路径仍返回。
- [ ] 在 `MemoryFlushManager.resolveOffloadPath()` 对 `workspaceManager.getFilesystem() instanceof AbstractSandboxFilesystem` 返回 `""`，其余路径继续委托 `SessionTranscriptWriter`。
- [ ] 运行：

```powershell
mvn -pl agentscope-harness -Dtest=MemoryFlushManagerOffloadTest -Dsurefire.failIfNoSpecifiedTests=false test
```

### Task 5: 评估 Python #2215/#2198 的独立增强（不与修复混合）

- [ ] 先读取 OpenSandbox SDK 1.0.18 的 Java API，确认是否存在 `isHealthy`、`SandboxManager.list`/metadata filter、`pause`/`resume`、volumes、env、network policy 对应 model。
- [ ] 若 SDK 支持且部署场景需要，新增 `OpenSandboxClientOptions` 的 volumes/env/metadata/network policy 字段，并让 `OfficialOpenSandboxSdk.create()` 透传；状态序列化不得包含 API key。
- [ ] 设计跨 JVM 的 metadata discovery 和幂等创建策略后，再新增 manager/cache；健康检查应放在锁外，采用“对象身份检查后再 eviction”的并发模式。
- [ ] pause/resume 不能直接替换当前 `stop()`/`shutdown()`：先明确 Harness 的 self-managed 生命周期契约和 TTL/资源回收策略，再单独立项。

### Task 6: 全量验证与真实服务 smoke test

- [ ] 只通过进程环境变量注入 endpoint/API key，运行 OpenSandbox 模块单测和全仓编译安装；不得把 token 写入源码、文档或测试资源。
- [ ] 真实服务 smoke test 覆盖 create、exec、native binary upload/download、404 recreate；若服务端默认网络 `app-net` 不存在，应通过测试配置选择服务端已存在的 network，不修改生产默认值。
- [ ] 运行：

```powershell
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox -am test
mvn install -DskipTests "-Dmaven.javadoc.skip=true"
git diff --check
git status --short
```

## 退出标准

- [ ] Task 1-4 的回归测试通过，且没有真实凭据进入 git diff。
- [ ] OpenSandbox 404 重建后 workspace projection 一定重新执行。
- [ ] 同一 agent 的并发调用只能使用各自 RuntimeContext 绑定的 sandbox。
- [ ] sandbox agent 不再收到宿主 session archive 路径。
- [ ] #2557/#2530 不重复合并；#2215/#2198 只有在 SDK 能力和部署需求确认后另行实施。
