# Injective 接入 SeiDB 的分析报告

## 一、问题背景

当前公司直接沿用 Injective 区块链。  
而 Injective 当前使用的仍然是传统 Cosmos-SDK / IAVL 存储模式，没有采用 SeiDB 这种 SC / SS 分层的状态存储架构。

现在的目标是：

- 将 SeiDB 接入 Injective
- 提升存储性能
- 降低状态膨胀
- 优化 commit、state sync、pruning、block sync 等表现

但当前最大的困惑是：

- 这件事到底是在改 Injective，还是在改 Cosmos-SDK
- 应该从哪里开始
- 工程上该怎么推进

## 二、核心结论

### 结论 1：这件事本质上主要不是改 Injective 业务层

如果只是“把 SeiDB 接进 Injective”，那么主要改动对象通常不是 Injective 的业务模块，而是：

- Injective 所依赖的 Cosmos-SDK / store 层
- Injective app 的启动和接入层
- 少量依赖旧存储语义的兼容代码

原因很简单：

- Injective 的业务模块通常并不直接操作底层数据库文件
- 它们依赖的是 Cosmos-SDK 暴露的 store 抽象接口
- 真正决定底层是不是 IAVL、是不是 SC / SS、是不是 MemIAVL 的，是 SDK 的存储层实现

所以这件事的本质不是：

- “修改 Injective 业务逻辑去支持新数据库”

而是：

- **“修改 Injective 依赖的 Cosmos-SDK / store 实现，使其底层从传统 IAVL 模式切换到 SeiDB 模式”**

### 结论 2：SeiDB 不是普通数据库替换，而是状态模型替换

SeiDB 不是简单地把：

- LevelDB 换成 PebbleDB

它做的是更深层的替换：

- 传统 IAVL 单层状态模型
- 切换成 SC（State Commitment）+ SS（State Store）双层结构

这意味着要改的不只是某个 DB backend，而是：

- 当前状态提交路径
- 历史状态存储路径
- snapshot / state sync / pruning / migration 路径

因此，这个项目的正确理解应该是：

**Injective 存储层架构迁移项目**

而不是：

**给 Injective 换一个底层数据库**

## 三、为什么主要要改 Cosmos-SDK / store 层

### 1. Injective 应用层通常只依赖抽象接口

Injective 的模块通常通过这些抽象访问状态：

- `KVStore`
- `MultiStore`
- `CommitMultiStore`
- cache store
- iterator

这些接口由 Cosmos-SDK 的 store 体系提供。

所以只要上层业务模块不直接耦合到底层 IAVL 实现细节，它们理论上不需要知道：

- 数据是存在 IAVL
- 还是存在 MemIAVL
- 还是存在 SC / SS 双层系统

### 2. SeiDB 替换的是状态管理实现，不是业务 API

SeiDB 的作用是替换：

- 状态提交
- 历史状态存储
- 快照与回放
- 历史查询与 pruning

它不是在修改业务模块的 keeper API 语义。

因此主战场在：

- `baseapp`
- `store`
- `rootmulti`
- `snapshot`
- `state sync`
- `migration`

### 3. Injective 侧主要做接入和兼容

当 SDK / store 层支持 SeiDB 后，Injective 侧通常要做的是：

- 启动时接入新的 commit store
- 配置新的 app.toml / 节点参数
- 补迁移逻辑
- 修补依赖旧 IAVL 语义的边界代码

所以，Injective 侧不是“不用动”，而是：

- **不是主改动层**
- **而是接入适配层**

## 四、SeiDB 接入 Injective，技术上会碰到什么

### 1. 需要替换的不是单个 DB backend

要改的不只是：

- `dbm.NewDB(...)`

而是更高层的状态组织方式，包括：

- app hash 生成路径
- 当前状态读写路径
- 历史状态查询路径
- pruning 路径
- snapshot 导入导出
- state sync 恢复
- archive node 策略

### 2. SeiDB 自身依赖 Sei 改造后的 SDK 结构

SeiDB 并不是完全独立于 Sei 链存在。

它和下面这些层有耦合：

- `sei-cosmos/store`
- `sei-cosmos/baseapp`
- 启动时的 store wiring
- migration 路由

所以如果要接入 Injective，现实路径通常是：

- 参考 Sei 的 store / baseapp 改法
- 把最小必要部分移植到 Injective 的 SDK fork

而不是直接把 `sei-db/` 目录扔进去就结束

### 3. 兼容性是最大风险

最大的风险不是编译不过，而是这些语义差异：

- 历史 proof
- 历史版本读取
- iterator 顺序
- snapshot 格式
- pruning 语义
- state sync 行为

如果 Injective 当前在某些地方依赖了传统 IAVL 语义，那么这些地方要么适配，要么重做。

## 五、项目应该如何理解：改 Injective 还是改 Cosmos-SDK

更准确的说法是：

### 第一层：改 Cosmos-SDK / store 层

这是主改动层，负责：

- 把传统 IAVL 路径替换为 SeiDB 路径
- 接入 SC / SS
- 支持新 snapshot / pruning / state sync / migration 行为

### 第二层：改 Injective 接入层

这是次改动层，负责：

- 应用启动接入
- 配置接入
- 少量兼容修补
- 节点迁移与运维支持

### 第三层：尽量不改 Injective 业务模块

这应该是目标状态。

只有当某些模块明确依赖旧 IAVL 行为时，才去改业务层。

## 六、你现在最应该先做什么

你现在不应该直接开始“把 SeiDB 塞进去”，因为这会导致范围不清、边界不清、耦合点不清。

你现在最应该做的是 **可行性审计**。

### 第一步：确认 Injective 当前技术底座

先确认：

- Injective 用的 Cosmos-SDK 版本
- Injective 是否维护自己的 SDK fork
- Injective 用的 CometBFT / Tendermint 版本
- Injective 当前 store 路径是否有自定义改造

这是第一优先级。

### 第二步：确认 Injective 是否依赖 IAVL 特性

重点排查：

- 是否依赖 historical proof
- 是否依赖历史版本查询
- 是否依赖特殊 iterator 顺序
- 是否依赖 IAVL snapshot / exporter / importer 语义
- 是否有模块显式假设底层是 IAVL

### 第三步：梳理 Sei 里哪些代码才是真正相关的

如果只是为了接 SeiDB，不需要把 Sei 全部学完。

应该重点看：

- `sei-db/`
- `sei-cosmos/store/`
- `sei-cosmos/baseapp/`
- `app/seidb.go`
- `docs/migration/seidb_migration.md`
- `docs/migration/seidb_archive_migration.md`

不需要先优先看：

- `x/evm`
- `oracle`
- `tokenfactory`
- `evmrpc`

## 七、改造范围怎么划分

建议把改造范围拆成四个阶段。

## 阶段 1：分析阶段

目标：

- 搞清楚 Injective 当前的存储依赖
- 搞清楚 SeiDB 依赖的 SDK 层改造
- 明确最小必要改动集合

交付物：

- Injective store 依赖清单
- IAVL 语义依赖清单
- SeiDB 接入点映射
- 风险清单

## 阶段 2：最小 PoC

目标：

- 先让 Injective 在本地最小环境下跑在 SeiDB 上

成功标准：

- 节点能启动
- genesis 能初始化
- 能出块
- 状态能提交
- app hash 正常

这一阶段不要先追求：

- archive node
- 主网迁移
- 回滚完整性
- 极限性能

## 阶段 3：兼容性阶段

目标：

- 验证历史查询、snapshot、state sync、pruning

要重点确认：

- 老的查询语义是否还成立
- 历史状态是否可用
- state sync 是否能恢复
- pruning 是否满足节点需求

## 阶段 4：迁移阶段

目标：

- 制定从旧 IAVL 节点迁移到 SeiDB 的路径

包括：

- validator 节点
- full node
- archive node
- rollback 方案

## 八、工程上最现实的路线

从现实工程角度，通常有两条路线。

### 路线 A：把 Sei 的 store / baseapp / db 相关改造移植到 Injective 的 SDK fork

这是最现实、最可控的路线。

优点：

- 改动面集中
- 不会把 Sei 其他无关模块一并带进来
- 更容易控制行为差异

缺点：

- 移植工作量大
- 需要自己维护 fork

### 路线 B：直接让 Injective 切到 `sei-cosmos`

这通常不建议作为第一方案。

原因：

- 会把 Sei 对 SDK 的其他改动一并带进来
- 行为差异范围变大
- Debug 成本更高

除非你们想深度向 Sei 靠拢，否则不建议一开始走这条路。

## 九、需要重点关注的代码层

如果后续真要动手，通常应该先盯这些层：

### 1. SDK / store 层

- `MultiStore`
- `CommitMultiStore`
- `rootmulti`
- cache store
- iterator
- proof
- snapshot importer / exporter
- pruning

### 2. BaseApp 层

- app 初始化 store 的方式
- commit 路径
- load version 路径
- startup wiring

### 3. Injective app 接入层

- app 创建逻辑
- 节点配置
- 启动参数
- migration 入口

## 十、这个项目最可能失败的地方

### 1. 把问题想简单了

如果把它理解成“换个数据库”，项目很容易在中后期失控。

### 2. 没先做依赖审计

如果不先确认 Injective 是否依赖：

- 历史 proof
- iterator 顺序
- 历史版本读取

后面会反复返工。

### 3. 一开始就试图一次迁完整条链

更好的做法是：

- 先做最小 PoC
- 再做兼容性
- 最后做迁移

## 十一、当前最合理的行动建议

如果你现在还没有头绪，建议按这个顺序做。

### 第 1 步

先确认 Injective 当前：

- SDK 版本
- 是否有 SDK fork
- store 初始化代码位置
- snapshot / pruning / state sync 入口

### 第 2 步

建立一个映射表：

- Injective 当前 store 路径
- Sei 的对应路径

### 第 3 步

只做一个最小目标：

- 本地启动 Injective
- 用接入后的 SeiDB 完成最小出块和状态提交

### 第 4 步

再逐步补：

- 历史查询
- state sync
- pruning
- migration

## 十二、最终判断

### 能不能做

能做。

### 主要改哪里

主要改：

- Injective 所依赖的 Cosmos-SDK / store 层

其次改：

- Injective app 接入层

尽量不改：

- Injective 业务模块

### 这件事该怎么定义

这件事应该定义为：

**“Injective 存储层架构迁移到 SeiDB”**

而不是：

**“给 Injective 插一个新数据库”**

## 十三、一句话结论

**如果要把 SeiDB 接入 Injective，主战场是 Cosmos-SDK/store 层而不是 Injective 业务层；正确路径是先审计 Injective 对 IAVL 的依赖，再把 Sei 的最小存储改造移植到 Injective 的 SDK 基座上，最后再做应用接入和迁移。**
