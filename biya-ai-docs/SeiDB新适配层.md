# SeiDB 新适配层

## 一、这次改动的目标

这次改动的目标不是“已经把 SeiDB 完整接入 `cosmos-sdk-inj`”，而是：

- 在 `cosmos-sdk-inj` 中先建立一套 **SeiDB 新适配层骨架**
- 固定后续接入所需的核心配置、接口入口和 app wiring
- 保持当前默认 IAVL 路径不被破坏
- 让开发者 A 和开发者 B 可以围绕同一条新路径并行工作

因此，这次改动的工程定位是：

**先增加一条新的 SeiDB 路径，而不是立即替换或删除原有 IAVL 路径。**

---

## 二、这次改动的总体策略

这次采用的是：

- **新增代码为主**
- **尽量不删原代码**
- **尽量不破坏默认行为**

也就是说：

- 原有 IAVL 路径保留
- 默认仍走原有 IAVL 路径
- 新增一组 `SeiDB` 配置和新 store 骨架入口
- 后续由开发者 A 和 B 在这条新路径上分别补核心实现与外围兼容

这比直接在旧路径上“边改边替换”更适合当前阶段，因为它有几个明显优势：

### 1. 保证链当前仍可运行

当前 `biyachain-core` 仍然可以沿用旧路径编译和启动。  
这样不会因为 SeiDB 尚未完成而阻塞现有工作。

### 2. 降低大范围回归风险

如果一开始就替换旧路径，任何 `rootmulti`、`baseapp`、proof、snapshot、rollback 的问题都会直接影响默认运行路径。  
新适配层把风险隔离到了 feature flag 和新配置路径下。

### 3. 便于双人并行

开发者 A 可以专注实现新的底层语义。  
开发者 B 可以在不等待最终实现完成的情况下，先做 app 层和消费侧兼容。

---

## 三、这次新增了什么

本次新增内容可以分成四层。

## 1. store 配置骨架

新增文件：

- `store/config.go`
- `store/seidb_skeleton.go`

新增概念：

- `StoreBackendIAVL`
- `StoreBackendSeiDB`
- `StoreConfig`
- `StoreBackendType`
- `SeiDBConfig`

当前 `StoreConfig` 负责描述：

- 当前选择哪种 backend
- 是否启用 `SeiDB`
- `SeiDB` 的 home
- SC backend
- SS backend
- keep recent
- async commit

这套配置现在已经是 SDK 层的正式入口。

### 当前行为

虽然支持选择 `StoreBackendSeiDB`，但当前 skeleton 阶段下：

- 仍然委托给现有 `rootmulti` 实现
- 目的是固定接口与路径，而不是立即完成底层语义切换

所以现在的 `seidb` 路径是：

**配置与入口已建立，底层真实实现待后续填充。**

---

## 2. CommitMultiStore 构造路径骨架

修改文件：

- `store/store.go`

当前形成了三层构造关系：

- `NewCommitMultiStore(...)`
- `NewIAVLCommitMultiStore(...)`
- `NewCommitMultiStoreWithConfig(..., cfg)`

含义如下：

### `NewCommitMultiStore(...)`

保留原有默认构造入口，默认仍使用 `DefaultStoreConfig()`。

### `NewIAVLCommitMultiStore(...)`

明确保留旧 IAVL 路径构造函数。  
后续即使 `seidb` 路径逐步完善，也仍有清晰的旧路径锚点。

### `NewCommitMultiStoreWithConfig(..., cfg)`

这是后续新路径的主入口。

后续真正的 `SeiDB` 实现，应主要在这一层扩展，而不是继续散落在 `BaseApp` 或 app 层做硬编码判断。

---

## 3. BaseApp 注入入口

修改文件：

- `baseapp/options.go`
- `baseapp/baseapp.go`

新增内容：

- `BaseApp.storeConfig`
- `BaseApp.StoreConfig()`
- `baseapp.SetStoreConfig(cfg store.StoreConfig)`

这意味着 `BaseApp` 现在正式支持：

- 持有一份当前 store 配置
- 在构造阶段显式接收 store config
- 基于该配置选择 multistore 构造路径

这一步非常关键，因为后续所有 app 层接入都将依赖这个入口，而不是继续直接操作 `rootmulti.NewStore(...)`。

---

## 4. server / flags / app wiring 骨架

修改文件：

- `server/start.go`
- `server/util.go`

新增 flags：

- `store.backend`
- `seidb.enabled`
- `seidb.home`
- `seidb.sc-backend`
- `seidb.ss-backend`
- `seidb.keep-recent`
- `seidb.async-commit`

新增逻辑：

- `server.GetStoreConfig(appOpts)`
- `DefaultBaseappOptions(...)` 中注入 `baseapp.SetStoreConfig(...)`

这意味着从节点启动参数到 `BaseApp` 的 store config 注入路径已经贯通。

当前流程是：

1. 命令行 / app options 读取 `store.backend` 和 `seidb.*`
2. `server.GetStoreConfig(appOpts)` 解析为 `StoreConfig`
3. `baseapp.SetStoreConfig(cfg)` 注入 `BaseApp`
4. `BaseApp` 基于配置选择 multistore 构造路径

---

## 四、为什么说这是“新适配层”

这里的“新适配层”指的不是完整的 SeiDB 实现，而是：

- 在现有 SDK/store/baseapp 之上
- 新增一层正式的 `SeiDB` 接入契约
- 让未来的 SC/SS 实现能被干净地挂接进来

所以它的作用不是立刻替代旧路径，而是先充当：

- 配置层适配器
- 构造层适配器
- app wiring 适配器
- 多路径切换适配器

一句话说：

**这层先解决“从哪里接入、怎么切换、上层如何调用”的问题。**

真正的 SC/SS 存储语义，则是后续要继续填充的下一层。

---

## 五、当前还没有完成的内容

当前这版并不等于完整 SeiDB 接入。

以下内容还没有完成：

- 真正的 SC `Committer` 接入
- 真正的 SS `StateStore` 接入
- `rootmulti` 基于 SC/SS 的 commit/load/version 行为
- snapshot 的新语义
- pruning 的新语义
- rollback 的新语义
- proof runtime 的新语义
- 历史查询行为适配

换句话说，当前已经完成的是：

- **新路径骨架**
- **配置入口**
- **app 接线**
- **默认不破坏旧路径**

还没有完成的是：

- **新路径的真实底层实现**

---

## 六、当前运行状态

当前运行状态可以概括为：

### 1. 默认路径仍然是原 IAVL

如果不显式启用 `SeiDB` 配置，当前行为仍然保持在原有 IAVL 路径。

### 2. 新路径入口已经存在

现在已经可以通过：

- `store.backend=seidb`
- 或 `seidb.enabled=true`

进入新路径的骨架模式。

### 3. skeleton 路径当前仍委托给现有 rootmulti

也就是说：

- 新路径的上层调用和配置入口已经固定
- 但底层目前还是旧实现

这样做的目的是：

- 先让 SDK 和 app 层稳定下来
- 再逐步填真正的 `SeiDB` 底层实现

---

## 七、对 `biyachain-core` 的影响

为了让 `biyachain-core` 能真正消费这次本地改动，本次还做了两件事：

### 1. app 层接上新 store config

`biyachain-core` 已经在以下路径注入了 `SetStoreConfig(...)`：

- 命令启动主路径
- devnet app 路径

这意味着：

- 后续 B 可以围绕这条新路径做外围兼容
- 不需要再等待 app 层接线变化

### 2. `go.mod` 切换到本地 `cosmos-sdk-inj`

`biyachain-core` 当前已经使用本地工作区里的：

- `../cosmos-sdk-inj`
- `../cosmos-sdk-inj/store`
- `../cosmos-sdk-inj/api`
- `../cosmos-sdk-inj/core`
- `../cosmos-sdk-inj/errors`
- `../cosmos-sdk-inj/x/*`

这样 `biyachain-core` 不会再混用远端 SDK 版本和本地 SDK 改动。

---

## 八、这层适合如何协作

当前这版新适配层已经足够支撑开发者 A / B 并行工作。

## 开发者 A 的工作重点

A 应该继续围绕这层骨架，完成：

- `StoreConfig -> 真正的 SeiDB backend` 映射
- `CommitMultiStoreWithConfig(..., cfg)` 下的真实 `SeiDB` 构造逻辑
- `rootmulti` 与 SC/SS 的真实对接
- snapshot / pruning / rollback / proof 的底层语义实现

### A 主要负责的层

- `store/types`
- `store/rootmulti`
- `store/snapshots`
- `store/pruning`
- `store/internal/proofs`
- `baseapp`

## 开发者 B 的工作重点

B 应该围绕这层骨架，完成消费侧兼容：

- `biyachain-core` 的 app 接入与配置
- proof / RPC / query 兼容
- upgrade / export / snapshot 外围入口
- `cosmos-sdk-inj` 外围模块与测试消费者兼容

### B 主要负责的层

- `biyachain-core` app / cmd / config
- `biyachain-core` proof / RPC / testutil
- `cosmos-sdk-inj/x/*` 的消费侧兼容
- `cosmos-sdk-inj/tests/integration`
- `cosmos-sdk-inj/testutil`
- `cosmos-sdk-inj/simapp`

---

## 九、A/B 共同需要遵循的原则

为了让并行开发真正有效，当前阶段建议严格遵循以下原则。

### 原则 1：不随意改入口契约

当前已经固定的这些入口，应尽量保持稳定：

- `StoreConfig`
- `SeiDBConfig`
- `NewCommitMultiStoreWithConfig(...)`
- `baseapp.SetStoreConfig(...)`
- `server.GetStoreConfig(...)`
- `store.backend`
- `seidb.*`

这些是 A/B 并行的基础。

### 原则 2：优先新增，不急于删除旧路径

当前阶段应继续保持：

- 旧 IAVL 路径保留
- 新 `SeiDB` 路径逐步补完
- 默认行为不强切

这样便于回归和对照。

### 原则 3：核心语义由 A 控制

如果 B 在外围兼容中发现问题，不应直接改：

- `rootmulti`
- `baseapp`
- `store/types`
- proof runtime

而应：

- 先补失败测试
- 明确暴露缺口
- 交给 A 在核心层补能力

### 原则 4：消费层适配由 B 控制

同理，A 不应把时间分散到：

- `biyachain-core` app wiring
- EVM RPC proof 返回格式
- 外围 testutil / integration test 修补

这些由 B 负责更高效。

### 原则 5：默认路径必须持续可运行

在完整 `SeiDB` 真正跑通前：

- 不应让默认 IAVL 路径失效
- 不应让 feature flag 之外的路径被破坏

---

## 十、当前阶段的一句话结论

这次改动建立的是：

**`cosmos-sdk-inj` 中用于接入 SeiDB 的新适配层骨架。**

它的本质是：

- 新增一条 `SeiDB` 配置与构造路径
- 保留原有 IAVL 路径
- 固定 SDK/store/baseapp/app 的入口契约
- 让 A/B 能围绕这条新路径并行推进

所以当前阶段应理解为：

**已经有了“SeiDB 新路径”，但它还是 skeleton；下一步的工作，是由 A 填充核心实现，由 B 补齐消费侧兼容。**

