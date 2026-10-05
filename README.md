# onchain-reorg-indexer-go（本地可复现的链上数据索引器）

用**本地模拟链**与**本地存储驱动**实现区块/交易的增量索引，使链重组（reorg）、
重复/乱序消费与重启续跑下的数据状态**可预期、可复现**。无任何外部依赖（标准库），
`go run ./cmd/demo` 即可一键复现完整场景。

## 目录结构

```
internal/model/   归一化数据模型 + 可区分的失败原因码目录
internal/chain/   本地模拟链（可延伸/分叉/注入坏块）+ v1 线上信封编码与校验和
internal/decode/  严格解码：版本/JSON/必填字段/地址/金额/两级校验和
internal/store/   存储驱动：原子事务内存存储 + AVL 有序索引 + 增量日志/周期快照
internal/index/   增量摄取、重组回滚重放、位点续跑、乱序缓冲、并发幂等
internal/query/   面向使用方：按地址查交易（keyset 翻页）+ 地址汇总
cmd/demo/         一键复现：坏块 → 重启续跑 → 分叉重组 → 并发摄取 → 地址查询
```

## 一、索引模型

| 记录 | 字段 |
| --- | --- |
| `IndexedBlock` | `Height, Hash, ParentHash, TxCount, IncludedAt` |
| `IndexedTx` | `TxHash, BlockHeight, BlockHash, Index, From, To, Amount` |
| `AddressSummary`（派生） | `Address, TxCount, Inflow, Outflow` |
| `FailureRecord` | `Height, Hash, Stage, Reason, Detail, Raw, CreatedAt` |
| `Checkpoint`（派生） | `NextHeight = maxIndexedHeight + 1`，`TipHash = 顶块哈希` |

- 存储以 **高度→区块**、**哈希→高度**、**高度→交易列表** 三张主索引组织，区块与其交易在
  **同一个事务**内写入，要么全部可见，要么全部不可见。
- **地址派生索引不是异步构建的**：每笔交易在写入/删除主索引的同一个事务里同步维护
  “**地址 → 有序交易树**”（按 `(高度, 区块内序号)` 排序的平衡 AVL 树）和
  “**地址 → 汇总计数**”。因此查询结果在结构上不可能与主索引出现“一边有一边没有”：
  同一事务同时可见/同时回滚，不存在最终一致窗口。
- **位点不是独立存储的**，而是从已提交区块派生，因此“位点”永远不可能与区块数据不一致。
- 每笔失败以稳定指纹 `height|hash|stage|reason` 去重持久化：坏记录**不静默丢弃**，
  重复投递同一条坏记录也不会产生重复失败行。

### 失败原因码（稳定公开契约，互不相同）

| Reason | 触发条件 |
| --- | --- |
| `UNSUPPORTED_VERSION` | 信封版本号非 v1（编码不兼容） |
| `MALFORMED_JSON` | 载荷不是合法 JSON，或载荷高度与信封高度不符 |
| `MISSING_BLOCK_HASH` / `MISSING_PARENT_HASH` / `MISSING_TX_HASH` | 必填标识字段缺失或为零 |
| `BAD_ADDRESS` | from/to 不是 20 字节 0x 十六进制 |
| `BAD_AMOUNT` | 金额为负等越界值 |
| `CHECKSUM_MISMATCH` | 区块头或交易校验和不通过（篡改/编码不兼容） |
| `BAD_PARENT_LINK`（保留） | 链接阶段父哈希不匹配（实际由重组路径处理） |
| `BEFORE_START_HEIGHT` | 投递高度低于配置起始高度 |
| `REORG_TOO_DEEP` | 分叉分歧深度超出最终性保留窗口 |

解码按固定顺序校验，首个失败原因**确定性**产生，测试日志逐条打印高度、区块标识、
位点与判定依据。

## 二、增量摄取与乱序/重复规则

对每条已通过解码的区块，按高度分类（`internal/index`）：

1. **重复（同高同哈希）** → `DUPLICATE`，空操作，位点不动，任何时刻可安全重放。
2. **位点回退（投递已提交高度的同哈希块）** → 同上，幂等。
3. **超前（高度 > 期望高度，存在缺口）** → `BUFFERED`，进入内存乱序缓冲；每次提交后
   尝试从当前期望高度起级联排空（drain），缺口一旦闭合自动按序落库，**不丢块**。
4. **期望高度且父哈希匹配** → `APPLIED`。
5. **同高不同哈希（迟到分叉）或期望高度但父哈希不匹配** → 进入重组流程。
6. **低于起始高度** → `REJECTED / BEFORE_START_HEIGHT`。

解码在互斥锁之外进行（纯函数，可并发）；状态应用在进程内互斥锁 + 存储原子事务内串行，
因此多个摄取流程并行 Submit 是**不丢不重**的（已由 `-race` 测试覆盖）。

## 三、重组（reorg）规则

收到分叉块时：

1. 从该块的 `ParentHash` 沿来源逐块**向历史回溯**，与已索引链逐高度比较，定位
   **共同祖先（common ancestor）**。
2. 回溯中一旦超过 `MaxReorgDepth`（最终性窗口）仍未找到共同祖先 →
   `REJECTED / REORG_TOO_DEEP`，并写入失败记录；**不删除、不覆盖任何已提交数据**，
   状态逐字节不变（有测试断言）。
3. 在窗口内找到祖先：在**单个存储事务**内
   - 删除 `(ancestor, oldTip]` 的全部区块及其交易（回滚）；
   - 从来源拉取并重放 `(ancestor, forkTip]` 的新分支区块（重放）。
4. 回滚/重放后逐高度与新链哈希比对，保证**最终结果与链上状态完全一致**；
   被分叉失效的缓冲块被清理。

## 四、面向使用方的查询模型（internal/query）

在“能存”的基础上新增“能按业务问”的只读服务 `query.Service`，只依赖 `store.Store`：

| 方法 | 语义 | 成本 |
| --- | --- | --- |
| `Transactions(ctx, addr, Page)` | 返回该地址作为**发送方或接收方**参与的交易，按 `(块高度, 区块内序号)` 升序 | O(log n + 页大小) |
| `Summary(ctx, addr)` | 返回 `TxCount / Inflow / Outflow` 汇总；从未参与的地址返回 `ok=false` | O(1) |
| `TransactionsHex / SummaryHex` | 上述接口的 0x 十六进制地址便捷封装，地址非法直接报错 | 同上 |

- **翻页采用 keyset（游标）而非 offset**：`Page{AfterHeight, AfterIndex, HasCursor, Limit}`
  表示“严格取该位置之后”的一页；返回的 `TxPage{NextHeight, NextIndex, HasNext}` 即下一页游标。
  深翻页不扫前缀，成本与“翻到第几页”无关（有测试断言第 10、5000、10000、19970 个位置之后
  取页代价一致）。`Limit=0` 用默认页大小（100），上限 1000，结果集和工作量都有界。
- **排序与主索引一致**：区块内序号就是交易在区块中的位置；同一笔自转交易（from==to）
  在地址列表中只出现一次，但金额同时计入流入和流出；`TxCount` 统计该地址参与的**去重交易笔数**。
- 地址派生结构只保存在内存索引里、**不重复序列化进快照**：重启时由主索引一次性重建
  （O(交易数)，仅重启一次），因此快照里永远不会出现主数据与派生数据两份可能漂移的副本。

### 与重组、断点规则的兼容范围

- **重组回滚重放**：回滚删除 `(ancestor, tip]` 时，在同一事务里对每笔被删交易反冲
  发送方/接收方的树与汇总；重放再按新分支重新累加。失效分支的交易立即从地址列表和汇总中
  消失，新分支交易补进来——没有“删除后又被查回”的窗口。
- **重复投递 / 乱序 / 重启续跑 / 并发摄取**：所有地址索引更新都走幂等的事务操作
  （同 `(高度,序号)` 同内容为 no-op，异内容先反冲旧值再写新值），重复与并发不会多一条；
  乱序缓冲块在真正上链的事务里才进入地址索引，缓冲未上链时查不到。
- **重启**：增量日志 + 快照恢复（见下节）后由主索引重建地址索引，已回滚的旧分支交易
  不会随重启复活。
- 地址查询是**只读视图**：只反映已提交的规范链状态，不包含缓冲区中尚未上链的超前块。

## 五、位点持久化与重启续跑（有界增量日志）

- 每次事务提交只把**本事务自身的操作**（put/delete 区块、交易、失败记录）追加到一个
  append-only 的**操作日志**（`.journal`，JSON Lines），按行追加并 `fsync`，
  常规提交写盘量 **O(本次增量)**，与历史总量无关。
- 日志操作数达到压实间隔时，在同一提交内把全量状态写成快照（临时文件 + 原子 rename）并删除日志。
  压实间隔从 64 起**倍增、封顶 4096**，于是：
  - 重启时最多在快照之上重放 **4096** 条日志操作——这是与链长无关的**固定上界**；
  - 全量快照改写次数随历史对数级变稀疏，封顶后每个固定窗口一次。
- 重启恢复 = 读全量快照 + 顺序幂等重放日志 + 由主索引重建地址派生结构；
  崩溃在追加中途留下的**残缺末行被丢弃**（在最后一个完整边界恢复），已回滚状态同样持久化，
  重启后已删除区块/交易不复活，失败记录按设计保留。
- 快照文件格式版本为 `version: 2`（在原 blocks/txs/failures 基础上记录压实间隔），
  地址派生结构不序列化，避免双份数据漂移。

## 六、性能边界（随历史增长保持稳定）

| 操作 | 上界 | 说明 |
| --- | --- | --- |
| 常规摄取一块 | O(log N) 内存操作 + O(本块增量) 追加写 | 高度树/地址树均为平衡树；提交只追加本事务日志 |
| 重组（深度 d） | O(d · log N)，与链总长无关 | 只触碰 `(ancestor, tip]` 区间及其地址树条目 |
| 重启续跑 | 快照 O(状态) 读一次 + 至多 4096 条日志重放 + 一次派生重建 | 重建仅在重启时发生，续跑后逐块恢复 O(log N) |
| 地址交易翻页 | O(log N + 页大小)，与页码/历史无关 | keyset 区间遍历，剪掉游标左侧整棵子树 |
| 地址汇总 | O(1) | 计数随主索引事务化维护，读取不做任何累加扫描 |
| 位点/失败计数 | O(1)/O(log N) | `MaxHeight` 走高度树，失败计数直接取 map 大小 |

说明：当前内存驱动是面向本地复现的 JSON 快照/日志，全量压实的代价在压实时刻为 O(状态)，
但通过倍增间隔保证其对数级稀疏、且每块摊薄成本随历史增长很缓慢；接口 `store.Store`
可整体替换为真正的增量 DB 驱动而不改动 index/query 逻辑。

- **并发**：解码并行、提交串行化，吞吐受单写者约束（正确性优先）；无忙等、无后台 goroutine。
- 无界增长的只有“随链上真实数据量必须保存的数据本身”；所有每操作/每查询的工作量、
  内存中为服务单次请求保留的结构、以及重启重放长度都有与链长无关的固定/对数上界。

## 七、验证方法（全部可复现）

```bash
go test ./...                 # 全量单测
go test -race ./...           # 竞争检测
go test -run xxx -bench . ./internal/index/   # 重放成本基准
go test -run xxx -bench 'BenchmarkIngestAtScale|BenchmarkAddressQueryAtScale' ./internal/store/  # 摄取/查询随链长基准
go run ./cmd/demo             # 端到端场景演示（含地址查询与重组对比）
```

测试日志逐场景打印覆盖了哪些用例、输入与结论（高度、区块标识、位点 next_height、判定依据、
每个地址的笔数/流入/流出）。测试矩阵：

| 场景 | 测试 |
| --- | --- |
| 9 类坏输入各自被拒、原因可区分、重复拒绝去重、位点不前进 | `TestMalformedInputsAreRejected` |
| 回滚到共同祖先 + 重放后逐高度等于新链，无残留 | `TestReorgRollbackAndReplay` |
| 超深重组被明确拒绝且状态逐字节不变 | `TestReorgTooDeepRejected` |
| 在顶部分叉（祖先为 tip-1） | `TestForwardReorgAtTip` |
| 重复/位点回退幂等 | `TestDuplicateSubmissionIsIdempotent` |
| 乱序缓冲 + 级联排空，无丢失 | `TestOutOfOrderIngestionDrainsInOrder` |
| 已提交高度出现异哈希块 → 触发重组而非覆盖 | `TestCheckpointRegressionAndResubmit` |
| 低于起始高度拒绝 | `TestBeforeStartHeightRejected` |
| 快照关闭/重建后续跑，回滚状态也持久化 | `TestSnapshotRestartContinuation` / `TestEndToEndRestartContinuation` |
| 12 worker 并发 + 链动态增长，无重无轻、链接完整 | `TestConcurrentIngestionIsLossless` |
| **地址分页/汇总与主索引逐笔一致（含自转）** | `query.TestAddressQueriesAndSummary` |
| **重组后旧分支交易从地址列表/汇总消失、新分支补入，无重复** | `query.TestAddressQueriesSurviveReorg` |
| **重组后落盘→重启→续跑，旧分支不复活，地址视图依旧一致** | `query.TestAddressQueriesRestart` |
| **重复投递 + 乱序缓冲，地址派生行不多不少** | `query.TestAddressQueriesDuplicateOutOfOrder` |
| **8 worker 并发重复投递，地址视图 == 主索引 == 独立期望模型** | `query.TestAddressQueriesConcurrent` |
| **增量日志压实有界（journal ≤ 4096）** | `store.TestJournalBoundedCompaction` |
| **快照+日志恢复精确、回滚记录不复活、派生计数正确** | `store.TestJournalRestartReplaysAndRollsBack` |
| **日志残缺末行（崩溃）容忍** | `store.TestJournalTornTail` |
| **深游标翻页与页码无关、全量 keyset 遍历无遗漏** | `store.TestPagingIndependentOfOffset` |
| **AVL 有序集插入/删除/区间删除/翻页** | `store.TestOrderedSet*` |

地址查询测试使用 5 个固定账户构成的独立“期望模型”（逐块手工维护每个地址应有的有序交易与
流入/流出），在每个阶段同时用**小页翻页**、**汇总**、**主索引全量遍历**三条路径交叉对账，
确保派生视图与主索引严格相等；重组与重启阶段额外断言具体失效/新增交易哈希和金额合计。

模拟链通过 `ForkAt(branch, height)` 在任意高度制造确定性分叉，
通过 `ExtendMalformed(n, kind)` 注入 9 类确定性坏块，因此每次运行结果完全一致。
