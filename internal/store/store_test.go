package store_test

import (
	"path/filepath"
	"testing"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/store"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/types"
)

func blk(h uint64) types.IndexedBlock {
	return types.IndexedBlock{
		Height: h,
		Hash:   hashOf(h),
		Txs: []types.IndexedTx{
			{Hash: hashOf(1000 + h), BlockHash: hashOf(h), BlockHeight: h},
		},
	}
}

func hashOf(v uint64) types.Hash {
	s := "0x"
	for i := 0; i < 64; i++ {
		s += string(rune('a' + (v+uint64(i))%6))
	}
	return s
}

func TestApplyRollbackPrune(t *testing.T) {
	m := store.NewMemory()
	for h := uint64(0); h <= 10; h++ {
		b := blk(h)
		if h > 0 {
			b.ParentHash = hashOf(h - 1)
		}
		if err := m.Apply(b, types.Checkpoint{Height: h, Hash: b.Hash}); err != nil {
			t.Fatalf("apply height=%d: %v", h, err)
		}
	}
	cp, ok := m.Checkpoint()
	if !ok || cp.Height != 10 {
		t.Fatalf("checkpoint=%+v ok=%v want height=10", cp, ok)
	}

	// 幂等：重复应用相同区块不改变状态。
	if err := m.Apply(blk(10), types.Checkpoint{Height: 10, Hash: hashOf(10)}); err != nil {
		t.Fatalf("idempotent re-apply: %v", err)
	}
	if got := len(m.Heights()); got != 11 {
		t.Fatalf("heights=%d want 11 after idempotent re-apply", got)
	}

	// 回滚到 7：上方数据删除，位点回退。
	if err := m.RollbackTo(7); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	cp, _ = m.Checkpoint()
	if cp.Height != 7 || cp.Hash != hashOf(7) {
		t.Fatalf("checkpoint after rollback=%+v", cp)
	}
	if _, ok := m.BlockAt(8); ok {
		t.Fatalf("height 8 should be rolled back")
	}

	// 裁剪到 5：低于 5 的不可再回滚。
	m.Prune(5)
	if m.LowestRetained() != 5 {
		t.Fatalf("floor=%d want 5", m.LowestRetained())
	}
	if err := m.RollbackTo(4); err == nil {
		t.Fatalf("rollback below floor must fail")
	}
	t.Logf("rollback floor enforced: floor=%d heights=%v", m.LowestRetained(), m.Heights())
}

func TestFileSnapshotRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap.json")
	m1, err := store.OpenFile(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for h := uint64(0); h <= 5; h++ {
		b := blk(h)
		if err := m1.Apply(b, types.Checkpoint{Height: h, Hash: b.Hash}); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	m1.Prune(2)

	// 重新打开：位点、数据、下界全部恢复。
	m2, err := store.OpenFile(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	cp, ok := m2.Checkpoint()
	if !ok || cp.Height != 5 {
		t.Fatalf("restored checkpoint=%+v ok=%v", cp, ok)
	}
	if m2.LowestRetained() != 2 {
		t.Fatalf("restored floor=%d want 2", m2.LowestRetained())
	}
	for h := uint64(2); h <= 5; h++ {
		b, ok := m2.BlockAt(h)
		if !ok || b.Hash != hashOf(h) || len(b.Txs) != 1 {
			t.Fatalf("restored block height=%d mismatch: %+v ok=%v", h, b, ok)
		}
	}
	t.Logf("snapshot restored: checkpoint=%+v floor=%d heights=%v", cp, m2.LowestRetained(), m2.Heights())
}
