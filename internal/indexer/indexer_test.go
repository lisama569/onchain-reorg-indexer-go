package indexer_test

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/chain"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/indexer"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/store"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/types"
)

const testRetention = 16

// logState 打印高度、区块标识、位点与判定依据，便于失败定位。
func logState(t *testing.T, ix *indexer.Indexer, st store.Store, note string) {
	t.Helper()
	cp, hasCP := ix.Checkpoint()
	t.Logf("state note=%s checkpoint=%v(has=%v) floor=%d heights=%v",
		note, cp, hasCP, st.LowestRetained(), st.Heights())
}

// assertChainConsistent 校验索引存储与规范链 [from, to] 完全一致。
func assertChainConsistent(t *testing.T, src chain.Source, st store.Store, from, to uint64) {
	t.Helper()
	for h := from; h <= to; h++ {
		want, ok := src.BlockAt(h)
		if !ok {
			t.Fatalf("source missing height=%d", h)
		}
		got, ok := st.BlockAt(h)
		if !ok {
			t.Fatalf("store missing height=%d", h)
		}
		if got.Hash != want.Hash || got.ParentHash != want.ParentHash {
			t.Fatalf("mismatch height=%d store.hash=%s chain.hash=%s", h, got.Hash, want.Hash)
		}
		if len(got.Txs) != len(want.Txs) {
			t.Fatalf("tx count mismatch height=%d store=%d chain=%d", h, len(got.Txs), len(want.Txs))
		}
		for i, tx := range got.Txs {
			if tx.Hash != want.Txs[i].Hash || tx.BlockHeight != h || tx.BlockHash != want.Hash || tx.TxIndex != i {
				t.Fatalf("tx mismatch height=%d index=%d tx=%+v", h, i, tx)
			}
		}
	}
}

func newIndexer(src chain.Source, st store.Store) *indexer.Indexer {
	return indexer.New(src, st, testRetention, nil)
}

// --- 归一化拒绝：原因可区分 ---

func TestNormalizeRejects(t *testing.T) {
	src := chain.NewSimChain()
	blocks := src.Extend(3)
	base := blocks[0]

	cases := []struct {
		name   string
		mutate func(b *types.Block)
		reason types.RejectReason
	}{
		{"missing block hash", func(b *types.Block) { b.Hash = "" }, types.ReasonMissingField},
		{"missing tx from", func(b *types.Block) { b.Txs[0].From = "" }, types.ReasonMissingField},
		{"missing tx value", func(b *types.Block) { b.Txs[1].Value = "" }, types.ReasonMissingField},
		{"bad block hash encoding", func(b *types.Block) { b.Hash = "0xzz" + b.Hash[4:] }, types.ReasonBadEncoding},
		{"bad tx hash length", func(b *types.Block) { b.Txs[0].Hash = "0x1234" }, types.ReasonBadEncoding},
		{"bad address encoding", func(b *types.Block) { b.Txs[0].To = "not-an-address" }, types.ReasonBadEncoding},
		{"bad value encoding", func(b *types.Block) { b.Txs[0].Value = "12a3" }, types.ReasonBadEncoding},
		{"dup tx hash in block", func(b *types.Block) { b.Txs[1].Hash = b.Txs[0].Hash }, types.ReasonValidation},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewMemory()
			ix := newIndexer(src, st)
			b := base
			b.Txs = append([]types.Tx(nil), base.Txs...) // 深拷贝，避免子用例间共享底层数组
			tc.mutate(&b)
			err := ix.IngestBlock(b)
			if err == nil {
				t.Fatalf("expected rejection, got nil")
			}
			if !types.IsReason(err, tc.reason) {
				t.Fatalf("reason mismatch: got %v, want %s", err, tc.reason)
			}
			t.Logf("rejected height=%d hash=%s reason=%s err=%v", b.Height, b.Hash, tc.reason, err)
			if _, hasCP := ix.Checkpoint(); hasCP {
				t.Fatalf("rejected record must not advance checkpoint")
			}
		})
	}
}

// --- 增量摄取 ---

func TestIncrementalSync(t *testing.T) {
	src := chain.NewSimChain()
	src.Extend(20)
	st := store.NewMemory()
	ix := newIndexer(src, st)

	if err := ix.SyncTo(20); err != nil {
		t.Fatalf("sync: %v", err)
	}
	logState(t, ix, st, "after sync to 20")
	cp, _ := ix.Checkpoint()
	if cp.Height != 20 {
		t.Fatalf("checkpoint=%d want 20", cp.Height)
	}
	assertChainConsistent(t, src, st, st.LowestRetained(), 20)
	// 保留边界：floor = 20 - retention。
	if st.LowestRetained() != 20-testRetention {
		t.Fatalf("floor=%d want %d", st.LowestRetained(), 20-testRetention)
	}
}

// --- 重组：回滚到共同祖先并按新分支重放 ---

func TestReorgRollbackReplay(t *testing.T) {
	src := chain.NewSimChain()
	src.Extend(10)
	st := store.NewMemory()
	ix := newIndexer(src, st)
	if err := ix.SyncTo(10); err != nil {
		t.Fatalf("sync: %v", err)
	}
	oldTip, _ := st.BlockAt(10)
	logState(t, ix, st, "before reorg")

	// 从高度 6 分叉，新分支延伸到 12。
	fork := src.Fork(6, 7)
	if err := ix.SyncTo(12); err != nil {
		t.Fatalf("sync after fork: %v", err)
	}
	logState(t, ix, st, "after reorg replay")

	cp, _ := ix.Checkpoint()
	if cp.Height != 12 || cp.Hash != fork[len(fork)-1].Hash {
		t.Fatalf("checkpoint=%+v want height=12 hash=%s", cp, fork[len(fork)-1].Hash)
	}
	assertChainConsistent(t, src, st, st.LowestRetained(), 12)
	// 旧分支顶块不得残留。
	if b, ok := st.BlockAt(10); ok && b.Hash == oldTip.Hash {
		t.Fatalf("stale block from old branch retained at height=10 hash=%s", oldTip.Hash)
	}
	t.Logf("reorg verified: ancestor=5 oldTip=%s newTip=%s", oldTip.Hash, cp.Hash)
}

// --- 超出保留范围的重组必须被拒绝且不留残留 ---

func TestReorgTooDeepRejected(t *testing.T) {
	src := chain.NewSimChain()
	src.Extend(40)
	st := store.NewMemory()
	ix := newIndexer(src, st)
	if err := ix.SyncTo(40); err != nil {
		t.Fatalf("sync: %v", err)
	}
	before := st.Heights()
	cpBefore, _ := ix.Checkpoint()
	hashBefore := snapshotHashes(st, before)
	logState(t, ix, st, "before deep reorg")

	// 分叉点远低于保留下界（floor = 40-16 = 24）。
	src.Fork(2, 45)
	err := ix.SyncTo(46)
	if err == nil {
		t.Fatalf("expected reorg_too_deep rejection, got nil")
	}
	if !types.IsReason(err, types.ReasonReorgTooDeep) {
		t.Fatalf("reason mismatch: got %v want %s", err, types.ReasonReorgTooDeep)
	}
	t.Logf("deep reorg rejected: height=%d reason=%s err=%v", cpBefore.Height+1, types.ReasonReorgTooDeep, err)

	// 状态必须保持不变：无回滚残留、位点未动。
	cpAfter, _ := ix.Checkpoint()
	if cpAfter != cpBefore {
		t.Fatalf("checkpoint moved after rejection: before=%+v after=%+v", cpBefore, cpAfter)
	}
	after := st.Heights()
	if fmt.Sprint(after) != fmt.Sprint(before) {
		t.Fatalf("heights changed after rejection: before=%v after=%v", before, after)
	}
	hashAfter := snapshotHashes(st, after)
	if fmt.Sprint(hashAfter) != fmt.Sprint(hashBefore) {
		t.Fatalf("block hashes changed after rejection:\nbefore=%v\nafter=%v", hashBefore, hashAfter)
	}
	t.Logf("state unchanged after rejection: heights=%v checkpoint=%+v", after, cpAfter)
}

// snapshotHashes 记录各高度区块哈希，用于拒绝前后的数据一致性比对。
func snapshotHashes(st store.Store, heights []uint64) map[uint64]types.Hash {
	m := make(map[uint64]types.Hash, len(heights))
	for _, h := range heights {
		if b, ok := st.BlockAt(h); ok {
			m[h] = b.Hash
		}
	}
	return m
}

// --- 重复与乱序消费：幂等且不丢不重 ---

func TestDuplicateAndOutOfOrder(t *testing.T) {
	src := chain.NewSimChain()
	blocks := src.Extend(10)
	st := store.NewMemory()
	ix := newIndexer(src, st)
	if err := ix.SyncTo(10); err != nil {
		t.Fatalf("sync: %v", err)
	}
	cpBefore, _ := ix.Checkpoint()

	// 重复消费已索引区块：幂等跳过，不报错、位点不动。
	for _, b := range blocks {
		if err := ix.IngestBlock(b); err != nil {
			t.Fatalf("duplicate ingest height=%d should be idempotent, got %v", b.Height, err)
		}
	}
	cp, _ := ix.Checkpoint()
	if cp != cpBefore {
		t.Fatalf("checkpoint moved on duplicates: before=%+v after=%+v", cpBefore, cp)
	}
	t.Logf("duplicates skipped idempotently: checkpoint=%+v", cp)

	// 乱序（未来高度）：拒绝并给出 out_of_order。
	future := blocks[9]
	future.Height = 15
	err := ix.IngestBlock(future)
	if !types.IsReason(err, types.ReasonOutOfOrder) {
		t.Fatalf("expected out_of_order, got %v", err)
	}
	t.Logf("out-of-order rejected: height=%d hash=%s err=%v", future.Height, future.Hash, err)

	// 未从创世开始：拒绝。
	st2 := store.NewMemory()
	ix2 := newIndexer(src, st2)
	if err := ix2.IngestBlock(blocks[3]); !types.IsReason(err, types.ReasonOutOfOrder) {
		t.Fatalf("expected out_of_order for non-genesis start, got %v", err)
	}

	// 最终数据仍与规范链一致。
	assertChainConsistent(t, src, st, st.LowestRetained(), 10)
}

// --- 重启续跑：位点持久化 ---

func TestRestartResume(t *testing.T) {
	src := chain.NewSimChain()
	src.Extend(30)
	path := filepath.Join(t.TempDir(), "snapshot.json")

	st1, err := store.OpenFile(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	ix1 := newIndexer(src, st1)
	if err := ix1.SyncTo(12); err != nil {
		t.Fatalf("sync phase1: %v", err)
	}
	cp1, _ := ix1.Checkpoint()
	logState(t, ix1, st1, "before restart")

	// 模拟重启：重新打开同一快照文件。
	st2, err := store.OpenFile(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	ix2 := newIndexer(src, st2)
	cp2, hasCP := ix2.Checkpoint()
	if !hasCP || cp2 != cp1 {
		t.Fatalf("checkpoint not persisted: before=%+v after=%+v(has=%v)", cp1, cp2, hasCP)
	}
	t.Logf("restart resume: checkpoint=%+v floor=%d", cp2, st2.LowestRetained())

	if err := ix2.SyncTo(30); err != nil {
		t.Fatalf("sync phase2: %v", err)
	}
	assertChainConsistent(t, src, st2, st2.LowestRetained(), 30)
	cp, _ := ix2.Checkpoint()
	if cp.Height != 30 {
		t.Fatalf("checkpoint=%d want 30", cp.Height)
	}
}

// --- 并发摄取：幂等、不丢不重 ---

func TestConcurrentIngest(t *testing.T) {
	src := chain.NewSimChain()
	blocks := src.Extend(60)
	st := store.NewMemory()
	ix := newIndexer(src, st)

	// 先摄取创世块，避免并发下多个"首块"竞争。
	if err := ix.IngestBlock(mustBlockAt(t, src, 0)); err != nil {
		t.Fatalf("ingest genesis: %v", err)
	}

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers*len(blocks))
	// 每个 worker 以不同起点正序重放全部区块，制造大量重复与并发。
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for i := 0; i < len(blocks); i++ {
				b := blocks[(i+offset)%len(blocks)]
				if b.Height == 0 {
					continue
				}
				err := ix.IngestBlock(b)
				// 乱序到达的块可能被拒（gap），由后续重放补齐；只容忍 out_of_order。
				if err != nil && !types.IsReason(err, types.ReasonOutOfOrder) {
					errs <- fmt.Errorf("worker=%d height=%d: %w", offset, b.Height, err)
				}
			}
		}(w * 3)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// 并发重放后补齐到链头。
	if err := ix.SyncTo(60); err != nil {
		t.Fatalf("final sync: %v", err)
	}
	logState(t, ix, st, "after concurrent ingest")
	cp, _ := ix.Checkpoint()
	if cp.Height != 60 {
		t.Fatalf("checkpoint=%d want 60", cp.Height)
	}
	assertChainConsistent(t, src, st, st.LowestRetained(), 60)
}

func mustBlockAt(t *testing.T, src chain.Source, h uint64) types.Block {
	t.Helper()
	b, ok := src.BlockAt(h)
	if !ok {
		t.Fatalf("source missing height=%d", h)
	}
	return b
}

// --- 重放开销与数据量无关：回滚+重放只触及分叉深度 ---

// countingStore 统计 Apply 调用次数。
type countingStore struct {
	store.Store
	applies int
}

func (c *countingStore) Apply(b types.IndexedBlock, cp types.Checkpoint) error {
	c.applies++
	return c.Store.Apply(b, cp)
}

func TestReplayCostIndependentOfChainLength(t *testing.T) {
	const depth = 5
	for _, total := range []int{50, 500, 2000} {
		t.Run(fmt.Sprintf("total=%d", total), func(t *testing.T) {
			src := chain.NewSimChain()
			src.Extend(total)
			cs := &countingStore{Store: store.NewMemory()}
			ix := newIndexer(src, cs)
			if err := ix.SyncTo(uint64(total)); err != nil {
				t.Fatalf("sync: %v", err)
			}
			cs.applies = 0

			// 固定深度分叉：无论链多长，重放只应触及 depth 个区块。
			src.Fork(uint64(total-depth+1), depth+2)
			if err := ix.SyncTo(uint64(total + 2)); err != nil {
				t.Fatalf("sync after fork: %v", err)
			}
			t.Logf("replay cost: total=%d depth=%d applies=%d checkpoint=%d",
				total, depth, cs.applies, total+2)
			if cs.applies != depth+2 {
				t.Fatalf("replay touched %d blocks, want %d (depth+new)", cs.applies, depth+2)
			}
			assertChainConsistent(t, src, cs, cs.LowestRetained(), uint64(total+2))
		})
	}
}
