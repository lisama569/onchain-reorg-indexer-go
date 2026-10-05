package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/model"
)

func testHashes(n byte) (model.Hash, model.Hash) {
	var bh, th model.Hash
	bh[31] = n
	th[31] = n + 100
	return bh, th
}

func putNBlocks(t testing.TB, st *MemoryStore, from, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		h := uint64(from + i)
		bh, th := testHashes(byte(h))
		err := st.Update(ctx, func(tx Tx) error {
			if err := tx.PutBlock(model.IndexedBlock{Height: h, Hash: bh, TxCount: 1}); err != nil {
				return err
			}
			return tx.PutTx(model.IndexedTx{
				TxHash: th, BlockHeight: h, BlockHash: bh, Index: 0,
				From: testAddr(1), To: testAddr(2), Amount: int64(10 + h),
			})
		})
		if err != nil {
			t.Fatalf("put h=%d: %v", h, err)
		}
	}
}

// TestJournalBoundedCompaction asserts per-commit persistence is incremental
// (journal exists with a bounded op count) and that periodic compaction
// rewrites a snapshot and empties the journal, independently of total history.
func TestJournalBoundedCompaction(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	st, err := NewFileBackedMemoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	const total = 400 // far beyond the 64-op compaction threshold
	putNBlocks(t, st, 1, total)

	info, jerr := os.Stat(path + ".journal")
	if jerr != nil {
		t.Fatalf("journal missing: %v (compaction state=%d)", jerr, st.journalCount)
	}
	// The restart-replay journal never exceeds the fixed cap, no matter how
	// much history is indexed. This is the chain-length-independent bound.
	if st.journalCount > journalCompactMax {
		t.Fatalf("journal op count %d exceeds fixed cap %d with %d blocks indexed",
			st.journalCount, journalCompactMax, total)
	}
	t.Logf("after %d blocks (800 ops): journal ops=%d (<=fixed cap %d), current interval=%d, journal bytes=%d",
		total, st.journalCount, journalCompactMax, st.compactThreshold, info.Size())

	snapInfo, serr := os.Stat(path)
	if serr != nil {
		t.Fatalf("compacted snapshot missing: %v", serr)
	}
	t.Logf("snapshot present after periodic compaction: %d bytes", snapInfo.Size())
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".journal"); !os.IsNotExist(err) {
		t.Fatalf("Close must compact journal away, stat err=%v", err)
	}
}

// TestJournalRestartReplaysAndRollsBack asserts snapshot+journal recovery
// reproduces exact primary and derived state, and that journaled
// deletions (reorg rollback) survive restart without residue.
func TestJournalRestartReplaysAndRollsBack(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")

	st, err := NewFileBackedMemoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	putNBlocks(t, st, 1, 50)

	// Roll back the last 10 heights (the exact shape of a reorg delete phase).
	ctx := context.Background()
	err = st.Update(ctx, func(tx Tx) error {
		for h := uint64(50); h > 40; h-- {
			if err := tx.DeleteTxsByBlock(h); err != nil {
				return err
			}
			if err := tx.DeleteBlock(h); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := NewFileBackedMemoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	err = st2.View(ctx, func(tx Tx) error {
		max, ok, err := tx.MaxHeight()
		if err != nil || !ok || max != 40 {
			t.Fatalf("after restart max=%d ok=%v err=%v, want 40", max, ok, err)
		}
		if _, ok, _ := tx.GetBlock(41); ok {
			t.Fatalf("rolled-back block 41 revived after restart")
		}
		if txs, _ := tx.ListTxs(41); len(txs) != 0 {
			t.Fatalf("rolled-back txs 41 revived after restart")
		}
		// Derived state: only heights 1..40 remain, so address counts reflect
		// exactly the retained txs.
		sum, ok, err := tx.GetAddressSummary(testAddr(1))
		if err != nil || !ok {
			t.Fatalf("sender summary after restart: %+v ok=%v err=%v", sum, ok, err)
		}
		if sum.TxCount != 40 || sum.Outflow != sum40Outflow() {
			t.Fatalf("sender summary after restart = %+v, want tx=40 outflow=%d", sum, sum40Outflow())
		}
		rows, err := tx.ListAddressTxs(testAddr(1), 0, 0, false, 1000)
		if err != nil || len(rows) != 40 {
			t.Fatalf("sender rows after restart=%d err=%v, want 40", len(rows), err)
		}
		rows, err = tx.ListAddressTxs(testAddr(2), 0, 0, false, 1000)
		if err != nil || len(rows) != 40 {
			t.Fatalf("recipient rows after restart=%d err=%v, want 40", len(rows), err)
		}
		t.Logf("restart recovery exact: tip=40, sender{tx=%d out=%d} recipient{tx=%d in=%d}",
			sum.TxCount, sum.Outflow, len(rows), testInflow40(tx))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestJournalTornTail tolerates a crash mid-append: the partial final line is
// ignored and the durable prefix plus snapshot is recovered exactly.
func TestJournalTornTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	st, err := NewFileBackedMemoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	putNBlocks(t, st, 1, 20)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen, append a few ops without compaction, then append garbage bytes.
	st2, err := NewFileBackedMemoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	putNBlocks(t, st2, 21, 5) // 10 more ops, under threshold
	f, err := os.OpenFile(path+".journal", os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("{\"op\":\"put_block\",\"h\":99,\"b\":{this is torn"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	st3, err := NewFileBackedMemoryStore(path)
	if err != nil {
		t.Fatalf("torn journal must recover, got %v", err)
	}
	var max uint64
	var has bool
	if err := st3.View(context.Background(), func(tx Tx) error {
		max, has, err = tx.MaxHeight()
		return err
	}); err != nil || !has || max != 25 {
		t.Fatalf("after torn tail max=%d ok=%v err=%v, want 25", max, has, err)
	}
	if _, ok := st3.blocks[99]; ok {
		t.Fatalf("torn block must not be recovered")
	}
	t.Log("torn journal tail tolerated: recovered through height 25, partial op discarded")
}

func testAddr(n byte) model.Address {
	var a model.Address
	a[19] = n
	return a
}

func sum40Outflow() int64 {
	var sum int64
	for h := uint64(1); h <= 40; h++ {
		sum += int64(10 + h)
	}
	return sum
}

func testInflow40(tx Tx) int64 {
	sum, ok, _ := tx.GetAddressSummary(testAddr(2))
	if !ok {
		return -1
	}
	return sum.Inflow
}
