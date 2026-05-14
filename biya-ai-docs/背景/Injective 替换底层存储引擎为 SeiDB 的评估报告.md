# Injective 替换底层存储引擎为 SeiDB 的评估报告

## 结论

如果目标是“**只替换 Injective 的数据库存储引擎**，其余业务逻辑、模块、RPC、共识流程尽量保持不变”，那么 **SeiDB 是可评估的候选方案，但它不是一次低风险的纯存储替换**。它更像是把 Injective 从传统 Cosmos/IAVL 存储模型，迁移到一套新的 **双层状态存储架构**。

从 Sei 官方文档看，SeiDB 的设计目标是替换 Cosmos 链上传统的 IAVL 存储，拆成：

- **SC Store**：负责当前状态提交、Merkle 哈希、状态读写
- **SS Store**：负责历史版本、查询、pruning、state sync 辅助

这意味着你不是在换一个普通 KV 引擎，而是在换一整套 **状态管理与版本化模型**。

## 适用前提

这个方案只在以下前提下成立：

- Injective 的目标问题主要是 **存储性能、状态膨胀、state sync、pruning、commit 延迟**
- 你接受对历史证明、迁移流程、运维方式做显著调整
- 你可以维护一条长期 fork 分支

如果 Injective 的主要瓶颈其实在执行层、模块逻辑、EVM/Wasm、消息路由或共识参数，那么 **只换存储不会根治性能问题**。

## 优点

### 1. 更适合高吞吐和大状态

SeiDB 的主要收益是：

- 降低状态膨胀
- 降低历史数据增长速度
- 缩短 commit 路径
- 提升 state sync / block sync 性能
- 提升长期运行下的稳定性

这对 Injective 这种长期高频交易链有现实价值。

### 2. 存储职责拆分更清晰

把 active state 和 historical state 拆开后：

- 验证人节点可以更专注于当前状态提交
- 全节点/归档节点可以更专注于历史查询
- pruning 和历史数据管理更容易单独优化

### 3. 更容易做后续性能优化

如果未来你们还想做：

- 更快的 state sync
- 更激进的 pruning
- 更强的并发写入
- 更灵活的存储 backend 切换

那 SeiDB 的架构比原始 Cosmos/IAVL 更有扩展性。

## 缺点

### 1. 不是“无感替换”

SeiDB 不是单纯换掉 LevelDB/RocksDB/PebbleDB 里的一个实现，而是替换状态模型本身。你需要处理：

- 版本化读写
- 历史查询路由
- snapshot/export/import
- migration
- pruning
- store 一致性

### 2. 历史证明能力会受影响

Sei 文档明确提到，SeiDB 不再支持所有历史块的 historical proof。  
如果 Injective 的某些外部依赖、审计需求、桥接逻辑或证明逻辑依赖这类能力，这会是硬约束。

### 3. 迁移成本高

从 Sei 的迁移文档看，迁移通常需要：

- 配置切换
- state sync
- 甚至 archive node 的额外迁移流程

对 Injective 来说，这意味着上线过程会比“换 DB 参数”复杂很多。

### 4. 维护复杂度上升

一旦接入 SeiDB，你们要长期维护：

- fork 的 store 代码
- 迁移工具
- 监控指标
- 故障恢复流程
- 回滚方案

这会显著增加工程负担。

## 改动难度评估

### 总体难度：高

如果限定为“只换底层存储，不动 Injective 业务模块”，难度仍然很高，因为存储层和上层逻辑并不是完全解耦的。

### 难度分级

- **原型验证**：中
- **测试网可用**：高
- **主网可运维**：很高

### 主要难点

- 现有代码是否强依赖 IAVL 的历史版本语义
- 现有模块是否依赖 historical proof
- 现有 state sync / snapshot / pruning 流程是否可直接复用
- 是否存在自定义 store wrapper 或特殊 multi-store 假设
- 迁移后数据一致性如何验证

## 需要改动的模块

下面按“只换存储引擎”的目标拆解。

### 1. `baseapp` / `app` 初始化

需要改：

- CommitMultiStore 的创建
- store mount 逻辑
- 启动参数解析
- 初始化时的 state store / state commit store 绑定
- genesis / upgrade 的 store 加载逻辑

核心目标是让 Injective 的 app 在启动时不再依赖原有 IAVL 体系，而是接入 SeiDB 的 SC/SS 结构。

### 2. `store` 相关实现

这是最核心的改造面：

- KV store wrapper
- multi-store
- cache store
- iterator 语义
- proof 相关实现
- commit / prune / version 访问

如果 Injective 现有代码里有自定义 store wrapper、cache multi-store、或历史版本访问逻辑，都要逐一确认兼容性。

### 3. `snapshot` / `state sync` / `export-import`

SeiDB 文档里明确提到它对 state sync 和 snapshot 的设计与传统 IAVL 不同，所以需要检查：

- 快照格式是否兼容
- export/import 是否需要重写
- state sync 恢复流程是否需要调整
- archive node 是否需要单独迁移逻辑

### 4. `pruning`

SeiDB 之后 pruning 语义会变化。要确认：

- 原有 pruning 配置是否仍然生效
- 旧的 keep-recent / keep-every / interval 是否被忽略
- 新的 prune job 是否能满足 Injective 的历史保留要求

### 5. `metrics` / `monitoring`

必须补：

- commit 延迟
- state store 写入延迟
- state sync 进度
- migration 进度
- 磁盘占用增长

否则上线后很难判断性能收益是否真实成立。

### 6. `upgrade` / `migration`

这是上线风险最大的部分：

- 需要从旧存储迁移到 SeiDB
- 需要定义切换窗口
- 需要可回滚方案
- 需要验证历史状态与当前状态一致

## 风险点

### 1. 兼容性风险

Injective 上可能存在依赖旧存储语义的模块，例如：

- 历史状态查询
- proof 验证
- 特定 iterator 顺序
- 自定义 snapshot 假设

这些地方最容易出 bug。

### 2. 运维风险

Sei 文档里已经说明迁移和 archive node 场景复杂。  
如果 Injective 是生产链，这会直接变成：

- 更复杂的部署
- 更长的恢复时间
- 更难的回滚

### 3. 性能收益不一定线性

SeiDB 主要改善的是存储瓶颈。  
如果 Injective 的瓶颈还在：

- 交易执行
- 签名验证
- 模块内部锁竞争
- ABCI 消息调度

那只换存储后，整体提升会有限。

## 建议路线

### 第一阶段：兼容性审计

先盘点 Injective 当前是否依赖：

- IAVL proof
- 历史版本查询
- 特殊 iterator 语义
- pruning 旧参数
- snapshot/export/import 细节

### 第二阶段：最小接入原型

先只做：

- 启动接入
- 基础读写
- commit
- state sync

不要一开始就追求全量迁移。

### 第三阶段：验证回归

重点测：

- 区块提交延迟
- mem/disk 增长
- state sync 时间
- node restart 时间
- 历史查询正确性

### 第四阶段：迁移策略

如果原型通过，再考虑：

- 灰度节点
- 测试网切换
- 主网分阶段迁移

## 最终判断

**作为“只替换 Injective 底层存储引擎”的方案，SeiDB 值得做 PoC，但不建议直接当作轻量改造。**

它的价值主要在：

- 存储性能
- 状态膨胀控制
- state sync / commit 优化

它的代价主要在：

- 迁移复杂
- 历史证明能力下降
- 运维和回滚复杂
- 长期 fork 维护成本高

如果你要把这件事做成工程项目，建议把它定义为：

**“Injective 存储层重构项目”**

而不是：

**“换一个数据库后端”**
