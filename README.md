# onchain-reorg-indexer-go

可本地复现的链上数据索引流程：以本地模拟链为数据源、内存/文件快照为存储驱动，
对区块与交易做增量索引，保证链重组、重复消费与重启续跑下的数据状态可预期。

## 架构与索引模型

```
cmd/indexer        演示程序：增量同步 → 重组 → 重启续跑
internal/types     核心数据模型与可区分的拒绝原因（RejectError）
internal/chain     本地模拟链数据源（确定性区块生成、分叉注入）
internal/store     索引存储驱动（内存 + 可选文件快照，全部操作原子生效）
internal/indexer   摄取管线：归一化校验、重组检测与回滚重放、位点管理
```

原始 `Block/Tx` 经 `NormalizeBlock` 归一化为可查询的 `IndexedBlock/IndexedTx`
（交易携带 `BlockHash/BlockHeight/TxIndex`）。非法记录不会被静默丢弃，
而是返回 `*types.RejectError`，其 `Reason` 可区分失败类别：

| Reason            | 含义                                             |
|-------------------|--------------------------------------------------|
| `missing_field`   | 必填字段缺失（hash/from/to/value 为空）          |
| `bad_encoding`    | 编码不兼容（哈希非 0x+64hex、地址非 0x+40hex、值非十进制） |
| `validation`      | 校验失败（块内交易哈希重复、块不在规范链上、创世冲突）   |
| `out_of_order`    | 乱序无法衔接（首块非创世、高度跳跃）              |
| `reorg_too_deep`  | 重组超出保留范围                                 |

重复消费（相同高度相同哈希，含已裁剪的历史高度）是幂等无操作，返回 `nil`。

## 重组与断点规则

- **位点（Checkpoint）**：每应用一个区块，位点 `(height, hash)` 与索引数据在同一
  存储操作中原子推进；文件快照模式下先写临时文件再 rename 提交。
- **重启续跑**：`store.OpenFile` 打开已有快照即恢复位点、索引数据与保留下界，
  新索引器从位点之后继续同步，不丢不重。
- **重组检测**：摄取区块与已索引数据冲突（同高度不同哈希，或父哈希不衔接位点）
  时触发重组处理。
- **回滚与重放**：从分叉点向下找到共同祖先（规范链与已索引数据哈希一致的最高
  高度），`RollbackTo(ancestor)` 删除其上方全部数据，再按规范链重放
  `[ancestor+1, forkTip]`。重放后存储与规范链逐块一致，旧分支无残留。
- **保留边界**：链头之下保留 `retention` 个区块（`floor = head - retention`）。
  共同祖先低于 `floor` 的重组被 `reorg_too_deep` 明确拒绝，且拒绝前不改动任何
  数据（无回滚残留、位点不动）。
- **并发摄取**：所有摄取经互斥锁串行化；重复块幂等跳过、乱序块以
  `out_of_order` 拒绝并由后续重放补齐，多流程并行不丢不重。

## 性能边界

- 单块摄取为 O(1)（哈希表读写 + 一次快照落盘）。
- 重放开销只与**分叉深度**相关，与已索引总量无关：回滚删除 d 个区块、
  重放 d 个区块；`TestReplayCostIndependentOfChainLength` 在链长
  50/500/2000 下验证固定深度 5 的重组恰好触及相同数量的区块。
- 内存占用以 `retention` 为界：超出保留范围的历史被 `Prune` 裁剪。
- 文件快照模式每次状态变更全量落盘，适合本地复现与中小数据量；
  生产场景可替换 `store.Store` 接口为增量写入的存储驱动。

## 验证方法

```bash
go build ./...                 # 编译
go test ./...                  # 全部单元测试
go test -race -count=2 ./...   # 竞态检测 + 重复运行（可复现性）
go test -v ./internal/indexer/ # 查看高度/区块标识/位点/判定依据日志
go run ./cmd/indexer           # 端到端演示：同步 → 重组 → 重启续跑
```

测试覆盖：

- `TestNormalizeRejects`：字段缺失/编码不兼容/校验失败，逐类断言拒绝原因；
- `TestIncrementalSync`：按高度增量摄取与保留边界；
- `TestReorgRollbackReplay`：分叉后回滚到共同祖先并按新分支重放，结果与链一致；
- `TestReorgTooDeepRejected`：超保留范围重组被拒绝且状态零变更；
- `TestDuplicateAndOutOfOrder`：重复幂等、乱序拒绝、非创世起步拒绝；
- `TestRestartResume`：文件快照位点持久化与重启续跑；
- `TestConcurrentIngest`：8 并发交错重放，幂等不丢不重（`-race` 通过）；
- `TestReplayCostIndependentOfChainLength`：重放开销与链长无关。
