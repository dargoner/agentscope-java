---
title: "沙箱（Sandbox）"
description: "隔离执行 + 跨调用恢复 + 多副本部署"
---

> 三种文件系统模式的对比见 [文件系统](./filesystem.md)。本文专门讲沙箱模式怎么用。

## 沙箱解决什么

把 agent 的**文件操作和命令执行**收到一个隔离环境里，宿主完全不参与。同时给你三个额外好处：

1. **执行边界** —— 不可信用户输入、奇怪的脚本、可能 `rm -rf` 的命令都关进沙箱，宿主无感。
2. **跨调用恢复** —— 不止恢复对话状态：连同 `pip install`、`npm install`、生成的临时文件这些可执行环境也会被快照保存，下次 `call()` 在同一沙箱里继续，不需要重装。
3. **多副本可用** —— 跨副本/跨进程对同一逻辑用户提供服务时，可以让沙箱状态共享同一个 slot，任意节点都能 resume 出同一份工作区。

## 一个最小例子

最简：本地 Docker，按用户隔离。

```java
HarnessAgent agent = HarnessAgent.builder()
    .name("code-agent")
    .model(model)
    .workspace(workspace)
    .filesystem(new DockerFilesystemSpec()
        .image("ubuntu:24.04"))
    .build();

agent.call(msg, RuntimeContext.builder()
    .userId("alice")
    .sessionId("conv-1")
    .build()).block();
```

同一 `userId` 的多次 `call()` → 自动复用同一沙箱（或从快照恢复）；不同 `userId` → 各自独立。如果 `userId` 缺失，自动降级为按 `sessionId` 隔离。

## IsolationScope —— 谁和谁共享同一沙箱

所有沙箱配置都集中在 `SandboxFilesystemSpec`（如 `DockerFilesystemSpec`）上。核心参数是 `isolationScope`：

| Scope | 谁共享 | 典型场景 |
|-------|--------|---------|
| `USER`（默认） | 同 `userId` 的多个 session 共享；userId 缺失时自动降级为 `SESSION` | 多用户 SaaS，同一用户跨会话保持工作区 |
| `SESSION` | 每个 sessionId 独立 | 严格按对话隔离 |
| `AGENT` | 这个 agent 的所有用户 / 会话共享 | 公共工具型 agent、共享知识库 |
| `GLOBAL` | 一个 store 内全局共享 | 谨慎使用 |

```java
// 显式指定 SESSION（覆盖默认的 USER）
.filesystem(new DockerFilesystemSpec()
    .image("ubuntu:24.04")
    .isolationScope(IsolationScope.SESSION))
```

`SESSION` 是天然并发安全（每个 session 自己一份）；`USER` / `AGENT` / `GLOBAL` 多副本部署时建议配并发互斥（见下面的"并发控制"）。

**USER 降级逻辑：** 当 `IsolationScope.USER` 生效（不管是默认还是显式设置），但 `RuntimeContext.userId` 缺失时，框架自动降级为按 `sessionId` 隔离。不需要额外处理 userId 为空的情况——沙箱会优雅降级。

## 跨调用恢复 = 快照

沙箱在每次 `call()` 结束时把工作区状态打包成快照存起来；下次 `call()` 开始时按情况恢复：

- 容器还在 + 工作区还在 → 直接接着用（最快）
- 容器没了 → 拿快照重新起一个，恢复工作区
- 没快照 → 按 `WorkspaceSpec` 全量初始化（冷启动）

快照存到哪里取决于你配的 `snapshotSpec`：

| 选项 | 适合 |
|------|------|
| `NoopSnapshotSpec`（默认） | 不持久化；容器没了就走冷启动 |
| `LocalSnapshotSpec` | 宿主本地文件（单机长期运行） |
| `OssSnapshotSpec` | OSS / S3 兼容存储（多副本） |
| `RedisSnapshotSpec` | Redis（低延迟、小工作区） |
| `JdbcSnapshotSpec` | MySQL / JDBC BLOB（已有关系型数据库） |

```java
.filesystem(new DockerFilesystemSpec()
    .image("ubuntu:24.04")
    .snapshotSpec(new OssSnapshotSpec(ossClient, "my-bucket", "agentscope/")))
```

`AGENTS.md` / `skills/` / `subagents/` / `knowledge/` 等宿主侧的工作区文件会在每次沙箱启动时同步进沙箱（按内容哈希增量）。你改了 `skills/` 里的脚本，下次 `call()` 沙箱里就是新版。

## 分布式部署

多副本部署同一个 agent，要让任意副本都能接住同一用户的对话，需要：

1. 一个分布式 `AgentStateStore`（例如基于 Redis 的实现）—— 通过 builder 的 `.stateStore(...)` 传入
2. 一个非 `NoopSnapshotSpec` 的快照（OSS / Redis 等远端存储）—— 直接配在 filesystem spec 上的 `.snapshotSpec(...)`
3. `IsolationScope` 选合适的（默认 `USER` 通常就够用）

所有配置集中在一处：

```java
HarnessAgent.builder()
    .name("assistant")
    .model(model)
    .workspace(workspace)
    .stateStore(redisStateStore)                    // 分布式状态
    .filesystem(new DockerFilesystemSpec()
        .image("ubuntu:24.04")
        .snapshotSpec(ossSnapshotSpec)              // 跨副本快照
        .isolationScope(IsolationScope.USER))       // 默认值，可省略
    .build();
```

框架把沙箱元数据（容器 ID、快照指针、workspace-ready 标记）和 agent 的运行时状态存在同一个 `AgentStateStore` 里。只要你配了分布式 store，沙箱跨副本 resume 就自动可用——不需要额外声明。

如果你使用的是本地 `AgentStateStore`（默认的 `JsonFileAgentStateStore`），开启沙箱模式时框架会在构建阶段打一条 warn 日志提醒你：沙箱状态不能跨 JVM 恢复、也不能跨实例共享。

## 并发控制（多副本场景）

`USER` / `AGENT` / `GLOBAL` 模式在多副本下，两个副本同时处理同一个用户的请求会都把状态写到同一个 slot，最后写入的为准。如果你不想这样，需要一把分布式锁。

**推荐方式**：使用 `distributedStore(...)`，快照和执行锁都会自动注入：

```java
DistributedStore store = RedisDistributedStore.fromJedis(jedis);

HarnessAgent.builder()
    .distributedStore(store)    // 自动注入 stateStore + snapshotSpec + executionGuard
    .filesystem(new DockerFilesystemSpec()
        .image("ubuntu:24.04")
        .isolationScope(IsolationScope.USER))
    .build();
```

如需自定义锁参数，可在 `SandboxFilesystemSpec` 上显式设置来覆盖 store 的默认值：

```java
.filesystem(new DockerFilesystemSpec()
    .image("ubuntu:24.04")
    .isolationScope(IsolationScope.USER)
    .executionGuard(RedisSandboxExecutionGuard.builder(jedis)
        .leaseTtl(Duration.ofMinutes(30)).build()))
```

内置实现：`RedisSandboxExecutionGuard`（Redis `SET NX PX`）、`JdbcSandboxExecutionGuard`（MySQL `GET_LOCK()`）。也可以实现 `SandboxExecutionGuard` 接口接其他锁后端（Zookeeper / etcd 等）。

## 自管沙箱实例（高级）

默认沙箱的整个生命周期由框架托管。三种"我自己管"的场景：

**1. 我已经启动好一个容器，想让 agent 用它**

```java
Sandbox mySandbox = dockerClient.create(workspaceSpec, snapshotSpec, options);
mySandbox.start();

SandboxContext callCtx = SandboxContext.builder()
    .client(dockerClient)
    .externalSandbox(mySandbox)       // 框架在 call 结束时只 stop()，不 shutdown()
    .build();

agent.call(msgs, RuntimeContext.builder()
    .sessionId("my-session")
    .put(SandboxContext.class, callCtx)
    .build()).block();

mySandbox.shutdown();
```

**2. 我有一个具体的快照串，想恢复到那个时刻**

```java
SandboxState savedState = dockerClient.deserializeState(savedStateJson);
SandboxContext callCtx = SandboxContext.builder()
    .client(dockerClient)
    .externalSandboxState(savedState)  // 框架按这个 state 恢复，但生命周期仍由框架管
    .build();
```

**3. 多个 agent 共享同一个沙箱**

把同一个 `externalSandbox` 透传给多个 agent 的 `call()`，最后由你自己 `shutdown()`。

## 沙箱后端怎么选

| 后端 | 适合 |
|------|------|
| **Docker** | 本地开发 / 单机 / 信任 shell |
| **Kubernetes** | 自建 K8s 集群；完全基于 [agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox)（SandboxClaim / WarmPool），工作区持久化靠 PVC（见下文） |
| **Daytona** | 通用托管沙箱 HTTP API |
| **E2B** | 通用托管沙箱 + 平台原生快照 |
| **OpenSandbox** | 自建或托管的 OpenSandbox 服务；支持生命周期、命令执行、原生文件传输和 Harness 快照 |
| **AgentRun** | 阿里云托管沙箱（函数计算 FC 3.0），实例级 NAS / OSS 动态挂载，中国大陆区域低延迟。在 Harness 里和 Docker / K8s / Daytona / E2B 等后端等价对待，模板配置 / RAM 权限 / NAS-first 等接入细节归在 integration 文档下 |

所有后端实现同一组接口，agent 代码、工具集、`AGENTS.md` 都不用变。

### OpenSandbox

引入独立扩展模块：

```xml
<dependency>
    <groupId>io.agentscope</groupId>
    <artifactId>agentscope-extensions-sandbox-opensandbox</artifactId>
    <version>${agentscope.version}</version>
</dependency>
```

```java
new OpenSandboxFilesystemSpec()
        .endpoint("http://localhost:8080")
        .apiKey(System.getenv("OPEN_SANDBOX_API_KEY"))
        .image("ubuntu:22.04")
        .workspaceRoot("/workspace");
```

API Key 应从环境变量或外部配置注入，不会写入可序列化的沙箱状态。OpenSandbox 适配器只接管沙箱生命周期、Shell 和文件传输；MCP 进程与连接仍运行在宿主侧。

#### 使用 Redis 协调 OpenSandbox 集群生命周期

需要跨应用副本稳定复用工作区时，引入 Redis 协调模块。它要求应用传入已经配置好的 `RedissonClient`；模块不会创建 Redis 连接，也不会读取 Redis 密码。

```xml
<dependency>
    <groupId>io.agentscope</groupId>
    <artifactId>agentscope-extensions-sandbox-opensandbox-redis</artifactId>
    <version>${agentscope.version}</version>
</dependency>
```

```java
// 默认：http://localhost:8080、无 API key、USER 隔离、
// idle TTL 60 分钟、sweep interval 5 分钟。
RedisOpenSandboxFilesystemSpec filesystemSpec =
        new RedisOpenSandboxFilesystemSpec(redissonClient);
```

control-plane 凭据是可选项，仅在环境变量非空时配置：

```java
OpenSandboxClientOptions openSandboxOptions = new OpenSandboxClientOptions();
String endpoint = System.getenv("OPEN_SANDBOX_ENDPOINT");
if (endpoint != null && !endpoint.isBlank()) {
    openSandboxOptions.setEndpoint(endpoint);
}
String apiKey = System.getenv("OPEN_SANDBOX_API_KEY");
if (apiKey != null && !apiKey.isBlank()) {
    openSandboxOptions.setApiKey(apiKey);
}

OpenSandboxRedisLifecycleOptions lifecycleOptions =
        new OpenSandboxRedisLifecycleOptions();
WorkspaceSpec workspaceSpec = new WorkspaceSpec();
workspaceSpec.setRoot("/workspace");

filesystemSpec
        .clientOptions(openSandboxOptions)
        .lifecycleOptions(lifecycleOptions)
        .workspaceSpec(workspaceSpec)
        .isolationScope(IsolationScope.USER);
```

上面的 `redissonClient` 由应用创建和配置。`USER` 是默认隔离范围，使用 `userId + agentId` 得到稳定的工作区身份；如果其他共享边界更符合部署需求，也可选择 `SESSION`、`AGENT` 或 `GLOBAL`。OpenSandbox endpoint 和 API key 是 client 级凭据，在一个 `RedisOpenSandboxClient` 的整个生命周期内必须稳定；逐调用切换任一凭据都会 fail closed。

image 与 entrypoint 构成工作区的 runtime profile，在该工作区存在期间也必须保持稳定。不同 profile 会 fail closed：应先显式 delete 工作区，再用新 profile 重新创建。delete 会清理历史原生快照并推进 tombstone，防止旧 generation 被重新发现。只在远端创建时生效的参数（例如 resource limits 和初始 metadata）不会在复用已有 sandbox 时重新应用。

Redis lifecycle lock 只保护短暂的创建、恢复、暂停、快照、删除与元数据迁移，不覆盖整个 Turn，因此多个 Turn 和 Subagent 可以持有独立 lease 并行工作。spec 会预置官方 no-op `SandboxExecutionGuard` 与 `NoopSnapshotSpec` 标记，防止 `distributedStore(...)` 将其 full-Turn guard 或 tar snapshot 后端自动注入该原生快照模式；distributed store 仍可提供 agent state、task、messaging 等其他能力。此模式不要配置其他 `SandboxExecutionGuard`。并发写同一共享文件仍可能冲突；写入顺序有要求时应由应用协调。detached process 不会自行续租：需要它继续运行时，应保持活跃 Turn，或调整 lifecycle 参数。

默认 idle TTL 为 60 分钟、每 5 分钟 sweep 一次、pause retention 为 5 分钟。eviction grace 与等待快照就绪的时长由 `OpenSandboxRedisLifecycleOptions` 控制（两者默认均为 5 分钟）。原生快照与 pause 完成后，远端沙箱按 `pauseRetention` 续期（默认 5 分钟），最终依据 OpenSandbox 的 `expiresAt` 回收。

此模式使用 OpenSandbox **原生快照**，它会保存远端文件系统，因此可能包含源码、生成物、凭据或其他敏感文件。应为 OpenSandbox 服务配置相应的访问控制、加密、保留与删除策略。Redis 只保存工作区和快照 ID、生命周期元数据、删除 tombstone 与活跃 lease，不保存文件内容，也不保存 OpenSandbox API key。不要同时配置启用持久化的 tar `SandboxSnapshotSpec`；这里只接受 `null` 或 `NoopSnapshotSpec`。

恢复时先尝试 current 原生快照，再尝试 previous 快照；两个已知快照都失败时会 fail closed，不会静默启动空工作区。Redis 元数据丢失时，client 会确定性发现匹配的 OpenSandbox 沙箱与原生快照并重建元数据；之前的显式删除由 tombstone 隔离，已删除 generation 不会复活。显式 delete 会先推进 tombstone，再清理远端资源。

应用负责持有并关闭注入的 Redisson client。通过 `.client(...)` 注入的预构建 sandbox client、绕过 `HarnessAgent.builder().filesystem(...)` 直接使用的 client，以及通过逐调用 `SandboxContext` 传入的 client 均由调用方持有，agent 绝不会关闭它们。spec 构建默认 `RedisOpenSandboxClient` 时，该 client 由 agent 持有，并由 `HarnessAgent.close()` 只关闭一次，从而停止其 daemon sweeper scheduler。内部构建的 Redis client 会根据 `clientOptions` 创建 OpenSandbox SDK/control-plane 对象；当前没有单独关闭该对象的公开操作。关闭借用的 sandbox handle 会释放该 handle 的 lease。关闭 Redis client 或借用的 handle 都不会 shutdown Redisson。本模块不提供 MCP 集成，MCP 连接生命周期不在其范围内。

## 运行时镜像约束

沙箱镜像（Docker 的 `image`、Kubernetes agent-sandbox 的运行时镜像等）由你指定，但**不是任意镜像都能用**。Harness 的文件工具（`read_file` / `write_file` / `edit_file` / `grep_files` / `glob_files` / `list_files`）和快照机制全部通过在沙箱内执行 POSIX shell 命令实现，镜像必须满足下面的契约，否则工具会以难以排查的方式失败。

### 基线约束（所有后端通用）

镜像内必须可用：

| 类别 | 要求 | 谁在用 |
|------|------|--------|
| Shell | POSIX 兼容 `sh`（支持 `[ ]`、`&&`、管道、重定向、heredoc） | 所有文件工具、`execute` 工具 |
| 核心工具 | `mkdir` `dirname` `rm` `mv` `test` `printf` `sort` | 各文件操作 |
| 文本/查找 | `sed` `grep`（支持 `-rHnF` `--include`）`find` | `read_file` 分页、`grep_files`、`glob_files` |
| 元数据 | GNU 风格 `stat -c`（非 BSD `stat -f`） | `list_files`、`glob_files` |
| 归档/编码 | `tar`、`base64`（编码 + `-d` 解码） | 快照持久化/恢复、文件上传下载 |
| 解释器 | `python3` | `edit_file`（精确字符串替换） |
| 文件系统 | 工作区根目录（默认 `/workspace`）可写 | 全部 |

以 `ubuntu:24.04`、`debian` 为基础的镜像天然满足（`python3` 可能需额外安装）；`alpine`（BusyBox `stat` / `grep` 行为不同）和 distroless 镜像**不满足**。

快速自检（在镜像内执行，全部成功即基本达标）：

```bash
sh -c 'echo ok' && python3 --version && tar --version \
  && printf x | base64 | base64 -d && stat -c %Y /tmp && grep -rHnF --include='*.txt' x /tmp; true
```

### Kubernetes（agent-sandbox）附加约束

agent-sandbox 后端不通过 `kubectl exec` 进容器，而是访问运行时容器暴露的 HTTP API（默认端口 8888）。镜像里的运行时服务必须实现：

| 端点 | 语义 |
|------|------|
| `POST /execute`（body `{"command": "..."}`） | **必须以 POSIX shell 语义解释 command**（等价 `sh -c`），返回 `{"stdout", "stderr", "exit_code"}`。Harness 发出的命令包含 `cd ... && (...)`、管道等 shell 结构 |
| `POST /upload`（multipart，`filename` 为相对路径） | 写文件到文件 API 根目录下 |
| `GET /download/{path}` | 按相对路径下载文件字节 |
| `GET /list/{path}`、`GET /exists/{path}` | 目录列表 / 存在性检查 |

**文件 API 根目录必须与工作区根一致**（推荐都用 `/workspace`）。Harness 通过 `/upload` / `/download` 传输两类内容：工作区快照 tar 包（临时文件放在根目录下的 `.agentscope-tmp/`），以及 `write_file` / 文件下载涉及的单文件字节（路径在根目录之下时；Linux 对单个命令行参数有约 128 KiB 上限，走文件 API 的原生传输不受此限制）。根目录通过 `KubernetesSandboxClientOptions.fileApiBaseDir` 配置（默认 `/workspace`）；置空则退回 base64-over-exec 传输。

> 注意：agent-sandbox 上游仓库的示例运行时（`examples/python-runtime-sandbox`）用 `shlex.split` 直接 `subprocess.run`、不经过 shell，且文件 API 根目录是 `/app`——**不满足本契约**，只能作为端点形状的参考。上游 KEP-539.2 正在推进运行时接口的正式标准化（REST/gRPC 规范 + 一致性测试），未来可对齐官方规范。

### 为什么这样设计

`Sandbox` 抽象的主数据面入口是 `exec(command)`。这是刻意的——`edit_file` / `grep_files` 这类工具的语义（正则、字符串替换、glob）不可能靠有限的文件 API 端点表达，靠镜像内的标准工具链执行 shell 脚本是唯一通用解。文件 API（upload/download）只承担"纯字节搬运"：工作区快照和单文件上传下载走它（后端通过实现 `SandboxFileTransfer` 可选接口声明该能力），其余一切走 `execute`。这也意味着：**镜像契约本身就是沙箱接口的一部分**，换镜像前先跑上面的自检。

## Kubernetes 后端的状态保存：PVC 是第一层

Kubernetes 后端完全基于 agent-sandbox：沙箱 pod 由 agent-sandbox 控制器管理，镜像、资源、存储都声明在集群侧的 `SandboxTemplate` / `SandboxWarmPool` 里，Java 侧只负责领取（`SandboxClaim`）和连接。这带来一个和其他后端不同的点——**工作区数据的持久化主要靠 PVC，而不是 Harness 快照**，两层机制各管一事：

| 层 | 保存什么 | 恢复场景 |
|----|---------|---------|
| **PVC**（`SandboxTemplate.volumeClaimTemplates`） | 工作区文件本身 | pod 重启 / 驱逐 / 休眠唤醒——claim 还活着，文件随 PVC 原样回来，零传输 |
| **SandboxState + snapshotSpec**（Harness 层） | 身份指针（claimName / namespace）+ 工作区 tar 快照 | resume 时定位沙箱（始终需要）；claim 已删除 / PVC 丢失 / 跨集群时的冷恢复 |

框架不需要为 PVC 做任何特殊适配：resume 后启动时会探测 `test -d /workspace`，PVC 在的话直接命中"工作区仍存活"分支，快照恢复整个跳过。

**必须配置对的三件事：**

1. **PVC 必须挂载在 `workspaceRoot` 上**（默认 `/workspace`）。挂错位置或用 `emptyDir`，pod 一重启工作区就丢，每次 call 都退化成快照恢复甚至冷启动。参考模板（来自 agent-sandbox 官方示例）：

```yaml
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxTemplate
spec:
  podTemplate:
    spec:
      containers:
      - name: runtime
        image: your-conformant-runtime:latest   # 须满足上面的运行时镜像约束
        ports:
        - containerPort: 8888
        volumeMounts:
        - name: workspace
          mountPath: /workspace                 # = workspaceRoot = fileApiBaseDir
  volumeClaimTemplates:
  - metadata:
      name: workspace
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 1Gi
```

2. **注意 claim 的生命周期边界**。`shutdownTime` / `shutdownPolicy: Delete` 到期删除 Sandbox 后，PVC 模式的热恢复就失效了——下次 resume 找不到 claim，框架会新建沙箱（新 PVC 是空的）。能不能找回工作区，取决于第 3 条。

3. **按需选 snapshotSpec**。PVC + `NoopSnapshotSpec`（默认）：省掉每次 call 结束的 tar 打包传输，代价是 claim 没了就冷启动；PVC + OSS / Redis 快照：双保险，claim 过期、PVC 丢失、跨集群迁移都能从快照冷恢复，代价是每次 call 结束多一次全量打包。

另外提醒：`SandboxState`（身份指针）这层永远绕不开——多副本部署时要配分布式 `AgentStateStore`，否则别的副本拿不到 claimName，PVC 里的数据再完整也 resume 不回来。

## 工作区怎么映射进沙箱

宿主侧 `workspace/` 下的关键文件（`AGENTS.md`、`skills/`、`subagents/`、`knowledge/`）在每次沙箱启动时同步进去；按内容哈希增量，不变就跳过传输。

需要把宿主的某个目录 bind 进沙箱（例如代码仓库），用 `BindMountEntry`（仅 Docker 支持；Kubernetes 后端的挂载在集群侧 `SandboxTemplate` 的 podTemplate 里声明，Daytona / E2B 等托管沙箱在云上跑，自然不能挂宿主目录）。

Sandbox 内对文件的修改不会反向同步回宿主——你想取沙箱里的产物，让 agent 自己 `read_file`。

## 实现自己的沙箱后端

需要接入 Docker 以外的隔离环境（自建远端执行器、商用沙箱 API、本地 mock 等），不需要改 Harness 源码——实现几个契约接口然后传给 `filesystem(...)` 就行。参考 `agentscope-harness` 测试里的 `InMemorySandbox` 系列，是最小可改造骨架。

## 相关文档

- [文件系统](./filesystem.md) — 三种声明式模式对比
- [工作区](./workspace.md) — `workspace/` 下哪些文件会同步进沙箱
- [架构](./architecture.md) — 沙箱 acquire / release 在 call() 时序中的位置
