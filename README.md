# onchain-reorg-indexer-go（本地可复现的链上数据索引器）

用**本地模拟链**与**本地存储驱动**实现区块/交易的增量索引，使链重组（reorg）、
重复/乱序消费与重启续跑下的数据状态**可预期、可复现**。无任何外部依赖（标准库），
`go run ./cmd/demo` 即可一键复现完整场景。

## 目录结构

```
internal/model/   归一化数据模型 + 地址查询派生模型 + 可区分的失败原因码目录
internal/chain/   本地模拟链（可延伸/分叉/注入坏块）+ v1 线上信封编码与校验和
internal/decode/  严格解码：版本/JSON/必填字段/地址/金额/两级校验和
internal/store/   存储驱动：原子事务内存存储 + JSON 快照（原子 rename 落盘）+ 派生地址索引
internal/index/   增量摄取、重组回滚重放、位点续跑、乱序缓冲、并发幂等、地址查询 API
cmd/demo/         一键复现：坏块 → 重启续跑 → 分叉重组 → 并发摄取 → 地址查询
```

## 一、索引模型

| 记录 | 字段 |
| --- | --- |
| `IndexedBlock` | `Height, Hash, ParentHash, TxCount, IncludedAt` |
| `IndexedTx` | `TxHash, BlockHeight, BlockHash, Index, From, To, Amount` |
| `FailureRecord` | `Height, Hash, Stage, Reason, Detail, Raw, CreatedAt` |
| `Checkpoint`（派生） | `NextHeight = maxIndexedHeight + 1`，`TipHash = 顶块哈希` |

- 存储以 **高度→区块**、**哈希→高度**、**高度→交易列表** 三张索引组织，区块与其交易在
  **同一个事务**内写入，要么全部可见，要么全部不可见。
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

## 二、地址查询模型（派生索引）

在主索引之上派生出一层按地址的查询能力，全部通过 `Indexer` 的只读 API 暴露：

| 记录/API | 内容 |
| --- | --- |
| `AddressTxRef` | `Height, TxIndex, TxHash, From, To, Amount`：某地址参与了某笔交易 |
| `AddressCursor` | 翻页位点 `(Height, TxIndex)`，零值表示从头开始 |
| `AddressSummary` | `TxCount, TotalIn, TotalOut`：参与笔数、流入合计、流出合计 |
| `QueryAddressTxs(addr, cursor, limit)` | 按（高度, 块内序号）排序的分页列表，返回 `NextCursor/HasMore` |
| `AddressSummary(addr)` | O(1) 汇总；地址无活动时 `ok=false` |

语义约定：

- 地址作为**发送方或接收方**出现即计入；`TotalOut` 累加 `From==addr` 的金额，
  `TotalIn` 累加 `To==addr` 的金额。
- **自转账**（From==To==addr）只计 1 笔，金额同时计入流入与流出。
- 派生索引与主索引**永远一致**：地址条目/汇总与对应的区块、交易在**同一个存储事务**
  内写入或删除，不存在"一边有一边没有"的中间态；重复投递通过 `(addr, height, txIndex)`
  幂等去重，重放不会双计。

### 与重组/断点规则的兼容范围

- **重组回滚重放**：回滚时在同事务内按高度删除失效分支的地址条目并反向扣减汇总，
  重放时逐笔补入新分支条目。重组结束后失效分支的交易**不再出现**在任何地址的结果与
  汇总里，新分支交易立即可查（`TestAddressQueryReorgConsistency` 逐地址断言）。
- **重启续跑**：快照只持久化主索引（区块/交易/失败记录），派生地址索引在加载时从
  主索引**确定性重建**，因此重启后旧分支不会"复活"，查询结果与重启前逐字节一致
  （`TestAddressQueryRestartConsistency`）。
- **重复/乱序/并发**：与主索引共用同一条幂等、串行提交的摄取路径，不多一条不少一条
  （`TestAddressQueryConcurrentDuplicates`，6 worker × 全量重复投递后逐地址对账）。
- **超深重组拒绝**：`REORG_TOO_DEEP` 路径不写任何数据，派生索引同样逐字节不变。

## 三、增量摄取与乱序/重复规则

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

## 四、重组（reorg）规则

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

## 五、位点持久化与重启续跑

- 每次事务提交后，存储驱动把全部区块/交易/失败记录序列化为 JSON，写到临时文件再
  **原子 rename** 覆盖快照（崩溃不会留下半截文件）。
- 重启时从快照重建三张索引，`NextHeight` 自动指向下一未索引高度，`RunTo` 无缝续跑。
- 回滚后的状态同样落盘：重启后已删除的区块/交易不复活（无残留），失败记录按设计保留。

## 六、性能边界

数据持续增长时，各操作的单次要价有稳定上界，与历史总量无关：

- **常规摄取**：每块一次原子事务，内含 O(块内交易数) 的 map 写入与地址条目插入；
  链尖高度（`MaxHeight`）由提交时增量维护，**O(1)**，不再随链长全扫。
  基准 `BenchmarkIngestAtScale`（链长 1024/8192/32768，单块摄取约 16.5µs，三条曲线持平）。
- **地址查询**：每地址条目按 `(高度, 块内序号)` 有序，翻页定位为二分查找，
  单次查询 **O(log n + 页大小)**；汇总为预聚合记录，**O(1)**。
  基准 `BenchmarkAddressQueryAtScale`（历史 256/2048/8192，首页查询+汇总约 1µs，不随历史增长）。
- **重组**：只触碰 `(ancestor, tip]` 区间；地址派生索引的删除按高度定位到受影响地址集合，
  成本同样为 **O(重组深度)**，与链总长度无关。
  基准 `BenchmarkReorgFixedDepth`（固定 3 块重组、链长 128/1024/8192）与
  `BenchmarkReorgFirstTimeAtScale`（16384 高度上 4 块真实重组）验证这一点。
- **重启**：快照加载与派生索引重建均为一次性的 O(已索引数据量)，与既有全量加载同阶；
  重建只读主索引，不引入额外持久化格式（快照版本不变，旧快照可直接加载）。
- **快照落盘**：当前文件驱动每次提交全量序列化 JSON，适合本地复现与中小数据量；
  接口 `store.Store` 可替换为增量/DB 驱动而不改索引与查询逻辑。
- **乱序缓冲**：每个超前块一次 map 插入；缺口闭合时级联排空，每块仍只落库一次。
- **并发**：解码并行、提交串行化，吞吐受单写者约束（正确性优先）；无忙等、无后台 goroutine。

内存占用与已索引数据量成正比（索引的本质），但**任何单次操作**的耗时与临时内存
都有上述上界；派生索引不丢数据、不与主索引脱节。

## 七、验证方法（全部可复现）

```bash
go test ./...                 # 全量单测
go test -race ./...           # 竞争检测
go test -run xxx -bench . ./internal/index/   # 重放成本基准
go run ./cmd/demo             # 端到端场景演示
```

测试矩阵（均打印高度、区块标识、位点 next_height 与判定依据）：

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
| 地址索引：排序/自转账/幂等/有界删除/快照重建 | `TestAddressIndexBasics` / `TestAddressIndexBoundedDelete` / `TestAddressIndexSnapshotRebuild`（store 层） |
| 地址分页与汇总，逐地址与主索引对账 | `TestAddressQueryPaginationAndSummary` |
| 重组后失效分支交易与汇总消失、新分支补入 | `TestAddressQueryReorgConsistency` |
| 重启后旧分支不复活、派生索引与主索引一致 | `TestAddressQueryRestartConsistency` |
| 并发重复投递后地址索引不多一条不少一条 | `TestAddressQueryConcurrentDuplicates` |

模拟链通过 `ForkAt(branch, height)` 在任意高度制造确定性分叉，
通过 `ExtendMalformed(n, kind)` 注入 9 类确定性坏块，因此每次运行结果完全一致。
地址查询测试另用手工构造的 v1 信封（`chain.EncodeEnvelope`）制造"热点地址"，
以验证深历史下的分页与汇总。

> 注：`TestConcurrentIngestionIsLossless` 的 worker 循环本次修复了一个既有缺陷：
> 空链时 `SimChain.Head()` 返回 `start-1`，在 `start=0` 下溢为 `MaxUint64`，
> 抢先于链增长的 worker 会沿该上界空扫直至 OOM；现在扫描上限被钳制到本测试
> 的高度范围，空轮次让出调度而非忙等。
