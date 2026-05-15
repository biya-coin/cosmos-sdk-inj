# SeiDB staged 修改说明（cosmos-sdk-inj）

本文档汇总 `cosmos-sdk-inj` 当前暂存区（staged）改动，用于评审、联调和回归测试准备。

## 1. 改动概览

- 影响文件数：`20`
- 新增文件：`14`
- 修改文件：`6`
- 代码量变化：`4497 insertions`, `10 deletions`
- 主要方向：将 SeiDB 路径从 skeleton 过渡到可运行的 `seidb/rootmulti` 方案，并补齐 SC/SS 分层、历史查询、回滚、一致性检查与测试。

## 2. 改动文件清单

### 2.1 修改文件（M）

- `server/start.go`
- `server/util.go`
- `server/util_store_config_test.go`
- `store/config.go`
- `store/config_test.go`
- `store/metrics/telemetry.go`
- `store/seidb_skeleton.go`

### 2.2 新增文件（A）

- `store/seidb/commitment/store.go`
- `store/seidb/commitment/store_test.go`
- `store/seidb/rootmulti/sc_committer.go`
- `store/seidb/rootmulti/sc_committer_db.go`
- `store/seidb/rootmulti/sc_committer_db_test.go`
- `store/seidb/rootmulti/ss_store.go`
- `store/seidb/rootmulti/ss_store_pebble.go`
- `store/seidb/rootmulti/ss_store_pebble_test.go`
- `store/seidb/rootmulti/store.go`
- `store/seidb/rootmulti/store_test.go`
- `store/seidb/rootmulti/storev2_runtime.go`
- `store/seidb/state/store.go`
- `store/seidb/state/store_test.go`

## 3. 关键功能变化

### 3.1 配置与启动参数调整

- 移除/替换了 `seidb.async-commit` 相关配置入口，新增：
  - `seidb.historical-proof-max-concurrency`
- `SeiDBConfig` 字段由：
  - `AsyncCommit bool`
  - 调整为 `HistoricalProofQueryMaxConcurrency uint32`
- 当 `seidb.home` 未显式配置时，新增回退逻辑：使用节点 `home`（`flags.FlagHome`）。

影响文件：
- `server/start.go`
- `server/util.go`
- `store/config.go`
- `server/util_store_config_test.go`

### 3.2 skeleton 接入方式变化

`store/seidb_skeleton.go` 不再创建 legacy `rootmulti.NewStore`，改为实例化 `seidb/rootmulti.NewStore` 并传入 `SeiDB Config`。  
同时 skeleton 实现了 `Query`，在底层支持时透传查询能力。

直接影响：
- SeiDB backend 不再只是“占位路径”，而是接入新 rootmulti 运行时。
- `NewCommitMultiStoreWithConfig_SeiDBSkeleton` 测试新增了 `Queryable` 断言。

### 3.3 新增 SeiDB rootmulti 运行路径（核心）

新增 `store/seidb/rootmulti/store.go` 与 `storev2_runtime.go`：

- `Store` 结构新增 `runtime + sc + ss` 组合：
  - `runtime`: storev2 兼容运行时（负责 mount/load/commit/query 等基础流程）
  - `sc`: state commitment 端（变更集提交）
  - `ss`: state store 端（历史版本读取/快照）
- 在 `Commit()` 中明确了顺序与一致性约束：
  1) runtime 提交
  2) SS 同步版本/changeset
  3) SC 应用并提交 changeset
  4) 清理 pending changeset
- 增加历史查询分流策略：
  - 非证明查询（`req.Prove=false`）优先走 SS 历史快照读取
  - 历史证明查询走 runtime 路径，并支持并发限流
- 增加历史证明查询并发门控：
  - `historicalProofQueryGate` + 拒绝计数与时延埋点
- 支持 rollback 到指定版本，并联动 runtime/SC/SS 一致性检查。
- restore 流程支持将 snapshot 节点同步导入 SS（后端支持时）。

### 3.4 SC（State Commitment）新增实现与可扩展注册

新增：
- `sc_committer.go`
- `sc_committer_db.go`

能力点：
- 引入 `RegisterSCCommitterBuilder`，可按 backend 名字注册构建器。
- 默认 `"memiavl"` 对应 `dbSCCommitter`。
- `dbSCCommitter` 支持：
  - `ApplyChangeSets`
  - `Commit(version)`
  - `Snapshot(store, version)`
  - `HasVersion/EarliestVersion/CurrentVersion`
  - `RollbackToVersion`
  - `keepRecent` 历史保留窗口与裁剪。

### 3.5 SS（State Store）新增抽象与 Pebble 后端

新增：
- `ss_store.go`
- `ss_store_pebble.go`
- `state/store.go`（历史快照只读查询视图）

能力点：
- `StateStore` 接口覆盖快照读取、版本可用性、变更应用、回滚、全量同步、关闭等能力。
- 支持按 backend 注册构建器 `RegisterStateStoreBuilder`。
- 默认 `"pebbledb"` 后端实现：
  - 独立持久化目录（`<seidb_home>/data`）
  - 版本元数据维护（latest/earliest/version_base）
  - 版本快照存取、历史保留、回滚
  - 快照导入与全量 `SyncFromStores`
- `state.Store` 提供历史只读 KV 语义，支持：
  - `Get/Iterator/ReverseIterator`
  - `/key` 与 `/subspace` 查询路径
  - 对证明查询显式报错（不支持）。

### 3.6 度量能力扩展

`store/metrics/telemetry.go` 扩展 `StoreMetrics` 接口：
- `MeasureSinceFrom`
- `SetGauge`
- `IncrCounter`

并提供了 `NoOpMetrics` 对应实现，供新路径埋点（例如历史证明限流）使用。

## 4. 测试补充情况

本次 staged 变更新增或更新了覆盖以下方向的测试：

- 配置解析与 home 回退
  - `server/util_store_config_test.go`
- SeiDB skeleton 初始化与 `Queryable` 能力
  - `store/config_test.go`
- commitment wrapper 行为
  - `store/seidb/commitment/store_test.go`
- rootmulti 主流程（commit/query/cache/rollback/upgrade/consistency 等）
  - `store/seidb/rootmulti/store_test.go`
- SC DB 提交器
  - `store/seidb/rootmulti/sc_committer_db_test.go`
- SS Pebble 后端
  - `store/seidb/rootmulti/ss_store_pebble_test.go`
- state 历史只读 store
  - `store/seidb/state/store_test.go`

## 5. 兼容性与风险关注点（评审建议）

- 旧配置项 `seidb.async-commit` 已不再生效，部署脚本/配置模板需同步更新。
- `seidb.home` 为空时将落到节点 `home`，需确认多环境（本地/容器/CI）目录权限与路径预期一致。
- 历史证明查询新增并发上限，若设置过小可能导致客户端收到并发拒绝错误。
- SS 后端默认依赖 `pebbledb` 并在 `<home>/data` 管理状态，升级时建议重点验证磁盘与恢复流程。
- rollback/restore 现已联动 SC/SS 一致性检查，建议在真实链状态数据上做长链路回归（含 prune、snapshot restore、历史查询）。

## 6. 建议的验证步骤（最小集合）

- 执行本次新增测试：
  - `store/seidb/...` 相关单测
  - `server/util_store_config_test.go`
  - `store/config_test.go`
- 启动链路验证：
  - 开启 SeiDB，确认启动参数生效
  - 验证历史高度 `/key` 与 `/subspace` 查询行为
  - 验证历史 proof 限流阈值生效与指标上报
- 数据一致性验证：
  - commit 连续写入
  - rollback 到历史高度
  - snapshot restore 后历史查询可用性

