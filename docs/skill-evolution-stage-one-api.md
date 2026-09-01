# 技能进化阶段一公共契约

阶段一只在 `agentscope-core` 暴露九个与平台无关的类型，平台的工作空间、审批、灰度、发布、预算和监控语义不得进入这些契约。

1. `SkillEvolutionType`
2. `SkillRevisionRef`
3. `SkillCandidateArtifact`
4. `SkillCandidateGenerationRequest`
5. `SkillCandidateGenerator`
6. `SkillValidationStage`
7. `SkillValidationRequest`
8. `SkillValidationReport`
9. `SkillCandidateValidator`

`agentscope-harness` 提供模型驱动的默认候选生成器和沙箱验证器，但它们不是核心公共契约的一部分。候选是不可变制品；`REFINE` 必须携带且只能携带一个来源；开发验证可返回经过净化的反馈；最终门禁无条件移除反馈。沙箱验证只执行服务端固定命令，不解析或调用技能中声明的 MCP 接口。

兼容性约束：新增字段优先使用带默认值的版本化映射；不得在阶段一改变九个类型的包名、公开方法或构造参数。确需破坏性变更时，必须先提供迁移期和兼容适配器。
