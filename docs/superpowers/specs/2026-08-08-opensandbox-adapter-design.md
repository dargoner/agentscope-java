# AgentScope Java OpenSandbox 适配器设计

> 状态：设计已确认，待书面评审
>
> 日期：2026-08-08
>
> 范围：新增 OpenSandbox Sandbox Provider；不修改 Harness 公共 SPI，不扩展 MCP。

## 1. 背景与目标

AgentScope Java 已通过 `agentscope-harness` 定义统一的 Sandbox 生命周期、工作区、
状态持久化和文件系统绑定，并提供 E2B、Daytona、AgentRun、Kubernetes 四种
Provider。本设计新增 OpenSandbox Provider，使应用能够连接自建或远程
OpenSandbox 服务，同时保持与现有 Provider 一致的使用方式和生命周期语义。

目标包括：

- 创建 OpenSandbox 实例并初始化 AgentScope 工作区。
- 执行 Shell 命令；成功时返回输出，非零退出码按现有 Provider 语义抛出
  `SandboxException.ExecException` 并保留退出码、stdout 和 stderr。
- 支持文件上传、下载以及工作区 tar 快照持久化和恢复。
- 将 sandbox ID 和工作区状态序列化，供后续调用或其他实例恢复。
- 区分停止和销毁：`stop()` 只持久化并释放本地资源，`shutdown()` 销毁自有实例。
- 支持 endpoint、API Key、镜像、资源和超时配置。
- 使用真实 OpenSandbox 服务完成端到端验证。

## 2. 非目标

本次不处理：

- 让 MCP stdio 进程运行在 OpenSandbox 内。
- 新增 sandbox-aware MCP transport。
- 修改 `Sandbox`、`SandboxClient` 或 `SandboxManager` 公共接口。
- 暴露 OpenSandbox 的代码解释器、浏览器、VNC、网络策略、Credential Vault、
  Sandbox Pool 等平台特有能力。
- 将 API Key 写入可持久化状态或日志。

## 3. 方案选择

采用 OpenSandbox 官方 Java SDK：

```xml
<dependency>
    <groupId>com.alibaba.opensandbox</groupId>
    <artifactId>sandbox</artifactId>
    <version>1.0.18</version>
</dependency>
```

选择理由：

- SDK 原生支持创建、连接已有实例、命令执行和文件流操作。
- SDK 负责 lifecycle endpoint、execd endpoint、认证、SSE 和重试细节。
- SDK class file 兼容 Java 8，满足 AgentScope Java 17 构建要求。
- 相比自行实现 OpenAPI 客户端，减少协议重复和后续升级成本。

不采用以下方案：

- 自行实现 OpenAPI/execd 客户端：需要重复实现 endpoint 解析、SSE、认证和重试。
- OpenSandbox MCP：无法对接 AgentScope 的 Sandbox 生命周期、状态和快照契约。

## 4. 模块与组件

在现有 Sandbox 聚合模块下新增：

```text
agentscope-extensions/agentscope-extensions-sandbox/
  agentscope-extensions-sandbox-opensandbox/
    pom.xml
    src/main/java/io/agentscope/extensions/sandbox/opensandbox/
      OpenSandboxClient.java
      OpenSandboxClientOptions.java
      OpenSandbox.java
      OpenSandboxState.java
      OpenSandboxFilesystemSpec.java
      OpenSandboxHarnessSandboxJacksonModule.java
```

同时把新模块加入 `agentscope-extensions-sandbox/pom.xml`。实现遵循 Daytona、E2B
和 AgentRun Provider 的命名、配置、状态序列化与测试结构。

### 4.1 `OpenSandboxClientOptions`

实现 `SandboxClientOptions`，保存非持久化客户端配置：

- `endpoint`：支持 `host:port` 或完整 HTTP(S) URL；无 scheme 时默认 `http`。
- `apiKey`：仅保存在客户端配置中。
- `image`：默认 `ubuntu:22.04`。
- `entrypoint`：默认 `tail -f /dev/null`。
- `sandboxTimeoutSeconds`、`readyTimeoutSeconds`、`requestTimeoutSeconds`。
- `cpu`、`memory`：按 OpenSandbox 字符串资源格式传入。
- `useServerProxy`：用于客户端不能直接访问 sandbox execd endpoint 的部署。

配置支持默认值与单次调用参数合并，保持其他 Provider 的优先级规则。

### 4.2 `OpenSandboxClient`

实现 `SandboxClient<OpenSandboxClientOptions>`：

- `create(...)` 创建 session ID 和 `OpenSandboxState`，但把远端创建延迟到
  `OpenSandbox.start()`，与其他 Provider 一致。
- `resume(...)` 校验状态类型并重建本地 handle。
- `serializeState(...)` 和 `deserializeState(...)` 使用 Jackson，并注册 Harness 与
  OpenSandbox 状态 subtype module。
- `delete(...)` 遵循现有 Provider 的 best-effort 清理语义。

### 4.3 `OpenSandboxState`

继承 `SandboxState`，持久化：

- `sandboxId`
- `sandboxOwned`
- `image`
- `entrypoint`
- `resourceLimits`
- `sandboxTimeoutSeconds`

基类继续持久化 session ID、`WorkspaceSpec`、快照和工作区就绪状态。API Key、
endpoint 和 SDK 对象不进入状态 JSON。

### 4.4 `OpenSandbox`

继承 `AbstractBaseSandbox`，内部持有按需创建的官方 SDK `Sandbox` handle：

- `start()`：没有 sandbox ID 时创建；已有 ID 时连接。
- 恢复明确返回不存在时创建新实例，清除工作区就绪标记，由 AgentScope 快照恢复。
- 认证失败、超时和服务端异常不自动创建，防止暂时不可达时产生重复实例。
- `doExec(...)` 使用 `commands().run(...)`，映射退出码和输出；非零退出码抛出
  `SandboxException.ExecException`。
- `doSetupWorkspace()` 创建工作区目录。
- `doDestroyWorkspace()` best-effort 删除工作区。
- `stop()` 先执行基类快照逻辑，再关闭本地 SDK HTTP 资源，不销毁远端实例。
- `shutdown()` 只对 `sandboxOwned=true` 的实例通过官方 `SandboxManager.killSandbox(id)`
  按 ID 销毁。该操作不依赖可能已由 `stop()` 关闭的 SDK handle。

### 4.5 工作区与文件传输

工作区默认根目录为 `/workspace`。Provider 实现 `SandboxFileTransfer`，使用 SDK
文件 API 直接上传和下载字节，供 `SandboxBackedFilesystem` 优先使用。

快照继续遵循 Harness 的 tar 流契约：

1. 在沙箱中把工作区打包到临时 tar 文件。
2. 通过 SDK `files().readStream(...)` 返回归档流。
3. 恢复时上传临时 tar 文件并在工作区解包。
4. 无论成功失败都 best-effort 删除临时文件。

这避免通过命令 stdout 传输二进制数据，也不要求镜像包含 Python 或 base64 工具。

### 4.6 `OpenSandboxFilesystemSpec`

继承 `SandboxFilesystemSpec`，提供与现有 Provider 一致的 fluent 配置：

- `client(...)`
- `endpoint(...)`
- `apiKey(...)`
- `image(...)`
- `entrypoint(...)`
- `cpu(...)`、`memory(...)`
- `sandboxTimeoutSeconds(...)`
- `readyTimeoutSeconds(...)`
- `requestTimeoutSeconds(...)`
- `useServerProxy(...)`
- `workspaceRoot(...)`、`workspaceSpec(...)`、`snapshotSpec(...)`

## 5. 生命周期与数据流

一次调用的数据流为：

```text
SandboxLifecycleMiddleware
  -> SandboxManager.acquire
  -> OpenSandboxClient.create/resume
  -> OpenSandbox.start
       -> SDK Sandbox.builder().build() 或 Sandbox.connector().connect()
       -> AbstractBaseSandbox 初始化/恢复工作区
  -> Agent 使用 SandboxBackedFilesystem 和 shell 工具
  -> 保存 OpenSandboxState
  -> SandboxManager.release
       -> OpenSandbox.stop
            -> 持久化工作区快照
            -> 关闭本地 SDK handle
       -> OpenSandbox.shutdown
            -> SandboxManager.killSandbox(sandboxId)
```

Harness 对 self-managed sandbox 的每轮 release 都按现有契约执行 `stop()` 和
`shutdown()`，因此远端实例会在调用结束时销毁。下一轮会读取持久状态，发现原
sandbox ID 不存在后，使用状态中保存的镜像、entrypoint、资源和 TTL 重建实例，
再从 AgentScope 快照恢复工作区。用户通过 external sandbox 注入的 user-managed
实例不由 Harness 停止或销毁。

显式清理同样执行：

```text
SandboxManager delete/close
  -> OpenSandbox.shutdown
  -> SDK Sandbox.kill()（仅自有实例）
```

集群下的重复创建控制继续由现有 `SandboxExecutionGuard`、lease 和
`SessionSandboxStateStore` 负责，Provider 不增加进程级全局缓存或独立分布式锁。

## 6. 异常处理

- 参数缺失或格式错误转换为 `SandboxConfigurationException`。
- 创建、连接和 SDK 调用错误转换为带 cause 的 AgentScope `SandboxException`。
- 命令超时映射为执行超时错误；非零退出码转换为
  `SandboxException.ExecException`，与 Daytona、E2B、Kubernetes 保持一致。
- 只有 SDK 明确报告 sandbox 不存在时才执行重建。
- 工作区初始化、归档和恢复错误沿用 `AbstractBaseSandbox` 的错误分类。
- 清理操作采用 best-effort，并保留主异常，避免清理失败覆盖真实错误。
- 日志可以记录 endpoint、session ID 和 sandbox ID，但不得记录 API Key 或完整认证头。

## 7. 测试设计

测试结构参考现有 Provider，不引入独立测试框架。

### 7.1 单元测试

- `OpenSandboxFilesystemSpecTest`：默认工作区和 fluent 配置。
- `OpenSandboxClientOptionsTest`：endpoint 解析、默认值、资源和超时校验。
- `OpenSandboxStateSerializationTest`：Jackson subtype 和字段往返，确认无 API Key。
- 生命周期测试：创建、连接、停止、销毁以及仅在明确不存在时重建。
- 命令映射测试：成功输出、非零退出码的 `ExecException`、超时和 SDK 异常。
- 文件传输测试：上传、下载、tar 快照和临时文件清理。

如官方 SDK 的 final 类型不便直接替换，在模块内部增加最小 package-private SDK
边界用于测试；该边界不得进入公共 API，也不复制 SDK 业务逻辑。

### 7.2 真实服务测试

集成测试通过环境变量读取：

```text
OPEN_SANDBOX_ENDPOINT
OPEN_SANDBOX_API_KEY
OPEN_SANDBOX_TEST_IMAGE
```

凭据不进入源码、测试资源或提交历史。端到端测试依次验证：

1. 创建沙箱。
2. 执行命令并检查 stdout/stderr/退出码。
3. 写入并读取二进制文件。
4. 初始化并持久化工作区。
5. 关闭本地连接后按 sandbox ID 恢复。
6. 恢复工作区内容。
7. 销毁沙箱并确认远端不存在。

集成测试必须在 `finally` 中清理其创建的实例。默认 Maven 单元测试不依赖外部服务；
真实测试由显式环境开关启用。

### 7.3 构建验证

完成实现后依次执行：

```text
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox -am test
mvn -pl agentscope-extensions/agentscope-extensions-sandbox/agentscope-extensions-sandbox-opensandbox -am package
mvn install
```

## 8. 验收标准

1. 使用 OpenSandbox endpoint、API Key 和镜像即可创建并使用沙箱。
2. Shell 成功执行返回正确输出；非零退出码抛出的 `ExecException` 包含正确退出码、
   stdout 和 stderr。
3. `SandboxBackedFilesystem` 能上传和下载文本及二进制文件。
4. 同一持久化状态能够连接已有实例；实例不存在时能够重建并恢复工作区。
5. 暂时网络错误或认证错误不会触发新建第二个实例。
6. `stop()` 不销毁远端实例，`shutdown()` 按 ID 销毁 AgentScope 自有实例；
   Harness 每轮 release 的 `stop() + shutdown()` 行为与其他 Provider 保持一致。
7. 状态 JSON、日志和测试文件中不包含 API Key。
8. 单元测试、模块聚合构建和全仓 `mvn install` 通过。
9. 指定真实服务上的创建、命令、文件、恢复和销毁流程通过。
