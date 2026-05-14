# 双人协作分工方案：SeiDB 接入 `cosmos-sdk-inj` / `biyachain-core`

## 结论先说

这个项目按两个人拆分是合理的，但前提是：

- 分工必须按“核心基座”和“外围兼容”切
- 不能按功能模块随意切
- 必须明确文件 ownership
- 必须定义串并行边界

如果拆得好，这样的分工是有效的：

- 开发者 A：负责 `cosmos-sdk-inj` 对齐 SeiDB 的核心改造
- 开发者 B：负责外围兼容适配，包括 `biyachain-core` 接入，以及 `cosmos-sdk-inj` 中消费新 store 语义的外围模块、测试和工具链修补

这样拆的好处是：

- A 专注 SDK/store/baseapp 的系统级改造
- B 专注 app 层和模块边界的落地与回归
- 避免两个人同时改同一批核心文件

不建议的拆法是：

- A 改一半 `rootmulti`
- B 改另一半 `rootmulti`

或者：

- A 负责 `sei-db`
- B 负责 `baseapp`

这种拆法边界太差，冲突会很多。

---

## 一、为什么这样拆是合理的

这个项目天然分成两层：

### 1. 核心基座层

这一层决定：

- store 抽象是否能承载 SeiDB
- `rootmulti` 是否能接 SC / SS
- `baseapp` 是否能正常 load / commit / query / snapshot / rollback

这部分的特点是：

- 改动深
- 风险高
- 强耦合
- 需要统一设计

适合由一个人集中负责，不适合多人同时并改。

### 2. 外围兼容层

这一层负责：

- `biyachain-core` app 接入
- 启动参数与配置适配
- upgrade / export / snapshot / state sync 接口联调
- proof / RPC / query 路径修补
- `cosmos-sdk-inj` 中消费新 store 语义的外围模块、集成测试、testutil 和 mock 修补
- 模块和测试代码的兼容性调整

这部分的特点是：

- 接口消费方多
- 改动分散
- 很适合在核心接口初步稳定后并行推进

所以“两人拆成核心 + 外围”是合理的。

---

## 二、建议角色定义

## 开发者 A：核心基座负责人

职责：

- 负责 `cosmos-sdk-inj` 对齐 Sei 的 store/baseapp 体系
- 负责 SeiDB 接入骨架
- 负责核心接口设计与语义兼容策略
- 负责 `rootmulti` / snapshot / pruning / rollback / proof 的主改造

目标：

- 让 `cosmos-sdk-inj` 成为一个可以消费 SeiDB 语义的 SDK/store 基座

### A 的主要写入范围

建议 A 独占以下目录或文件：

- `cosmos-sdk-inj/store/types/`
- `cosmos-sdk-inj/store/rootmulti/`
- `cosmos-sdk-inj/store/snapshots/`
- `cosmos-sdk-inj/store/pruning/`
- `cosmos-sdk-inj/store/internal/proofs/`
- `cosmos-sdk-inj/baseapp/`
- `cosmos-sdk-inj/server/rollback.go`
- `cosmos-sdk-inj/server/pruning.go`
- `cosmos-sdk-inj/server/util.go`
- `cosmos-sdk-inj/x/upgrade/types/storeloader.go`

必要时可新增：

- `cosmos-sdk-inj/store/seidb/` 或类似接入目录
- `cosmos-sdk-inj/store/rootmulti` 下的新适配代码

### A 的明确不负责项

A 不应主导这些工作：

- `biyachain-core` 的具体 app wiring 落地
- EVM RPC proof 结果适配
- 业务模块测试修补
- CLI / 配置模板 / 节点参数文案整理

这些交给 B 更合适。

---

## 开发者 B：外围兼容负责人

职责：

- 负责 `biyachain-core` 对新的 `cosmos-sdk-inj` 基座做接入
- 负责 `cosmos-sdk-inj` 中外围消费者对新 store 语义的兼容修补
- 负责 app 启动参数、配置、升级、导出、proof/RPC、测试修补
- 负责业务模块和工具链的兼容性验证
- 负责将核心改动真正落到链应用层和消费侧

目标：

- 让 `biyachain-core` 在尽量少改业务模块的前提下跑起来
- 让 `cosmos-sdk-inj` 的外围模块、测试和工具链能够消费新的存储语义

### B 的主要写入范围

建议 B 独占以下目录或文件：

- `biyachain-core/biya-chain/app/app.go`
- `biyachain-core/biya-chain/app/app_devnet.go`
- `biyachain-core/biya-chain/app/upgrade.go`
- `biyachain-core/biya-chain/app/export.go`
- `biyachain-core/biya-chain/app/upgrades/`
- `biyachain-core/cmd/biyachaind/root.go`
- `biyachain-core/cmd/biyachaind/start.go`
- `biyachain-core/cmd/biyachaind/config/`
- `biyachain-core/biya-chain/modules/evm/rpc/backend/account_info.go`
- `biyachain-core/biya-chain/modules/evm/rpc/types/query_client.go`
- `biyachain-core/biya-chain/modules/peggy/testpeggy/common.go`
- `biyachain-core/biya-chain/modules/evm/testutil/`
- 其他依赖 proof / snapshot / rollback / store loader 的测试辅助代码
- `cosmos-sdk-inj/x/bank/` 中因新存储语义导致的消费侧兼容修补
- `cosmos-sdk-inj/x/auth/`
- `cosmos-sdk-inj/x/staking/`
- `cosmos-sdk-inj/x/distribution/`
- `cosmos-sdk-inj/tests/integration/`
- `cosmos-sdk-inj/testutil/`
- `cosmos-sdk-inj/simapp/`
- `cosmos-sdk-inj/server/mock/`
- 其他直接消费 `CommitMultiStore` / proof / query / version 语义的模块侧与测试侧代码

### B 的次要负责项

B 还应承担：

- 梳理哪些 IAVL 参数要保留、废弃或重命名
- app 层 feature flag
- migration / rollout 文档初稿
- 与 A 联合做端到端 smoke test

### B 的明确不负责项

B 不应直接主导这些改动：

- `cosmos-sdk-inj/store/rootmulti` 核心实现逻辑
- `baseapp` 的 commit / load / query 主路径设计
- snapshotter / pruning manager 内部改造
- proof runtime 的底层实现
- `store/types` 的核心接口设计
- `sei-db` adapter 的底层实现

否则会和 A 发生高冲突。

---

## 三、建议的协作边界

为了避免冲突，建议直接按“核心接口层”和“消费层”切。

### A 提供给 B 的稳定边界

A 需要尽快给出一版“对外可消费接口契约”，至少包括：

- `CommitMultiStore` 是否保留现有对外形状
- `BaseApp` 外部调用方式是否保持兼容
- snapshot 初始化方式是否变化
- rollback 行为是否变化
- proof 查询是否保留，哪些高度可用
- pruning 参数如何映射
- store upgrade / `SetStoreLoader` 是否保持原用法

B 的所有外围兼容工作，都依赖这个边界。

### B 反馈给 A 的风险清单

B 需要持续把这些问题反馈给 A：

- 哪些 app 层代码因为接口变化无法编译
- 哪些 RPC / proof 行为变了
- 哪些测试依赖旧 IAVL 语义
- 哪些 CLI / 配置项需要新语义

这样 A 才能判断：

- 是继续兼容旧接口
- 还是明确宣告行为变化

---

## 四、建议的阶段顺序

两个人不是完全并行，而是“先串后并”。

## 阶段 1：A 先打出核心骨架，B 做审计和准备

### A 负责

- 确定 `cosmos-sdk-inj` 最终对齐的接口面
- 设计 `rootmulti` 如何接 SC / SS
- 做最小 `CommitMultiStore` / `BaseApp` 接入骨架
- 形成初版 feature flag 或切换路径

### B 负责

- 清点 `biyachain-core` 和 `cosmos-sdk-inj` 外围消费层受影响文件
- 建立外围兼容清单
- 定位 proof / snapshot / upgrade / export / CLI 参数依赖点
- 定位 `cosmos-sdk-inj` 外围模块和测试中对旧 IAVL 语义的依赖点
- 准备消费侧 patch 列表，但先不大改核心调用

这一阶段 B 主要是做准备，不要抢改核心接口依赖。

## 阶段 2：A 稳定核心接口，B 开始并行接入

### A 负责

- 完成 `rootmulti` / `baseapp` / snapshot / pruning / rollback 主路径
- 跑通最小本地 PoC
- 提供“可编译、可启动”的 SDK 基座分支

### B 负责

- 修改 `biyachain-core` app wiring
- 接入新的配置参数和 feature flag
- 修补 `SetStoreLoader` / `LoadLatestVersion` / snapshot / export 相关调用
- 修补 `cosmos-sdk-inj` 外围模块和测试消费者
- 修补测试辅助代码

这一阶段开始真正并行。

## 阶段 3：A/B 联调 proof、query、迁移、测试

### A 负责

- proof runtime
- 历史查询语义
- rollback / pruning 行为
- snapshot/state sync 底层问题

### B 负责

- EVM RPC `GetProof`
- app export / zero-height
- CLI / node config
- `cosmos-sdk-inj` 外围模块兼容，例如 `x/bank` 等消费者修补
- 回归测试、集成测试、业务模块测试

## 阶段 4：一起收敛上线前能力

A 和 B 共同负责：

- migration/runbook
- rollback 方案
- archive/full node 差异
- 可观测性
- 压测和恢复演练

---

## 五、具体 ownership 建议

下面给出一个更具体的 ownership 表。

### A 独占

- `cosmos-sdk-inj/store/types/store.go`
- `cosmos-sdk-inj/store/rootmulti/store.go`
- `cosmos-sdk-inj/store/rootmulti/proof.go`
- `cosmos-sdk-inj/store/snapshots/manager.go`
- `cosmos-sdk-inj/store/snapshots/store.go`
- `cosmos-sdk-inj/store/pruning/manager.go`
- `cosmos-sdk-inj/baseapp/baseapp.go`
- `cosmos-sdk-inj/baseapp/options.go`
- `cosmos-sdk-inj/baseapp/abci.go`
- `cosmos-sdk-inj/server/rollback.go`
- `cosmos-sdk-inj/server/util.go`

### B 独占

- `biyachain-core/biya-chain/app/app.go`
- `biyachain-core/biya-chain/app/app_devnet.go`
- `biyachain-core/biya-chain/app/upgrade.go`
- `biyachain-core/biya-chain/app/export.go`
- `biyachain-core/cmd/biyachaind/root.go`
- `biyachain-core/cmd/biyachaind/start.go`
- `biyachain-core/cmd/biyachaind/config/*`
- `biyachain-core/biya-chain/modules/evm/rpc/backend/account_info.go`
- `biyachain-core/biya-chain/modules/evm/rpc/types/query_client.go`
- `biyachain-core/biya-chain/modules/peggy/testpeggy/common.go`
- `cosmos-sdk-inj/x/bank/`
- `cosmos-sdk-inj/x/auth/`
- `cosmos-sdk-inj/x/staking/`
- `cosmos-sdk-inj/x/distribution/`
- `cosmos-sdk-inj/tests/integration/`
- `cosmos-sdk-inj/testutil/`
- `cosmos-sdk-inj/simapp/`
- `cosmos-sdk-inj/server/mock/`

### 共同 review，但不要同时写

- `cosmos-sdk-inj/x/upgrade/types/storeloader.go`
- `biyachain-core/biya-chain/app/upgrades/*`
- 任何 proof / snapshot / export / rollback 的联动测试

这些文件容易跨边界，不建议两个人同时改。

---

## 六、两个人各自的交付物

## A 的交付物

- `cosmos-sdk-inj` 对齐 SeiDB 的接口设计说明
- 最小可运行的 `CommitMultiStore` / `rootmulti` / `baseapp` 骨架
- snapshot / pruning / rollback / proof 的行为说明
- SDK 层 PoC 分支

## B 的交付物

- `biyachain-core` app 接入补丁
- `cosmos-sdk-inj` 外围消费者兼容补丁
- 配置参数迁移方案
- proof/RPC 兼容清单
- 测试辅助代码修补
- app 层回归验证记录

## A+B 共同交付物

- 兼容性矩阵
- 风险清单
- migration/runbook
- 回滚方案
- 联调测试结果

---

## 七、需要提前约定的技术规则

为了让双人协作真的有效，建议提前约定：

### 1. 外部接口优先兼容

A 修改 `cosmos-sdk-inj` 时，优先保持这些调用尽量不变：

- `baseapp.NewBaseApp(...)`
- `app.MountKVStores(...)`
- `app.LoadLatestVersion()`
- `app.SetStoreLoader(...)`
- `storetypes.KVStore`
- `runtime.NewKVStoreService(...)`

这样 B 的改动会小很多。

### 2. 新能力优先用 feature flag 引入

不要一次性删除旧路径。

建议：

- 保留旧 IAVL 路径
- 新增 SeiDB 路径
- app 层通过配置切换

这样 A 和 B 都更容易联调。

### 3. proof / rollback / pruning 要单独列风险

这三块不要混在“普通兼容性”里处理。

建议单独维护：

- proof 行为变化清单
- rollback 方案说明
- pruning 参数映射表

### 4. 不并改 `rootmulti`

这是最重要的协作规则。

`rootmulti`、`baseapp`、snapshot 主路径由 A 独占。

B 遇到问题，只提 issue 和使用反馈，不直接抢改核心实现。

---

## 八、最推荐的执行方式

最推荐的方式不是“两人同时从第一天就各改各的”，而是：

1. A 先用 2-4 天打出 `cosmos-sdk-inj` 最小骨架和接口边界
2. B 同期只做外围依赖审计和 patch 准备
3. A 输出第一版可接入接口后，B 再开始 app 层和测试层并行开发
4. 双方每天同步一次接口变化和阻塞点

这样比完全并行更稳。

---

## 九、一句话结论

双人拆分是合理的，最优拆法是：

- **A 负责 `cosmos-sdk-inj` 的核心 SeiDB 对齐改造**
- **B 负责消费侧外围兼容，包括 `biyachain-core` 接入，以及 `cosmos-sdk-inj` 中模块、测试、工具链的兼容修补**

核心原则是：

**按“基座实现”和“消费适配”拆，不按单个模块或单个目录机械拆。**
