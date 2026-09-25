package store

import (
	"context"
	"testing"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/model"
)

// TestSnapshotRestartContinuation persists blocks, txs and failures, then
// rebuilds a store from the same file and verifies exact state recovery.
func TestSnapshotRestartContinuation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := dir + "/snapshot.json"

	st, err := NewFileBackedMemoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	var h1 model.Hash
	h1[31] = 1
	var h2 model.Hash
	h2[31] = 2
	var ph model.Hash
	ph[31] = 9
	if err := st.Update(ctx, func(tx Tx) error {
		if err := tx.PutBlock(model.IndexedBlock{Height: 1, Hash: h1, ParentHash: ph, TxCount: 1}); err != nil {
			return err
		}
		if err := tx.PutTx(model.IndexedTx{BlockHeight: 1, BlockHash: h1, Index: 0, TxHash: h2}); err != nil {
			return err
		}
		_, err := tx.PutFailure(model.FailureRecord{Height: 1, Hash: h1.Hex(), Stage: "decode", Reason: model.ReasonBadAmount, Detail: "x"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: new driver instance backed by the same snapshot file.
	st2, err := NewFileBackedMemoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	err = st2.View(ctx, func(tx Tx) error {
		max, ok, err := tx.MaxHeight()
		if err != nil || !ok || max != 1 {
			t.Fatalf("checkpoint max height: max=%d ok=%v err=%v", max, ok, err)
		}
		b, ok, err := tx.GetBlock(1)
		if err != nil || !ok || b.Hash != h1 {
			t.Fatalf("block not restored: %+v ok=%v err=%v", b, ok, err)
		}
		byHash, ok, err := tx.GetBlockByHash(h1)
		if err != nil || !ok || byHash.Height != 1 {
			t.Fatalf("hash index not restored")
		}
		txs, err := tx.ListTxs(1)
		if err != nil || len(txs) != 1 || txs[0].TxHash != h2 {
			t.Fatalf("txs not restored: %v err=%v", txs, err)
		}
		fails, err := tx.ListFailures()
		if err != nil || len(fails) != 1 || fails[0].Reason != model.ReasonBadAmount {
			t.Fatalf("failures not restored: %v err=%v", fails, err)
		}
		t.Logf("restored checkpoint max_height=%d block=%s txs=%d failures=%d",
			max, b.Hash.Short(), len(txs), len(fails))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Idempotent re-save of the same failure must not duplicate after restart.
	if err := st2.Update(ctx, func(tx Tx) error {
		added, err := tx.PutFailure(model.FailureRecord{Height: 1, Hash: h1.Hex(), Stage: "decode", Reason: model.ReasonBadAmount, Detail: "x"})
		if err != nil {
			return err
		}
		if added {
			t.Fatalf("identical failure must be deduplicated across restart")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Rolled-back state survives restart too: delete block+txs, snapshot,
	// reopen and confirm absence (no residue after reorg persistence).
	if err := st2.Update(ctx, func(tx Tx) error {
		if err := tx.DeleteTxsByBlock(1); err != nil {
			return err
		}
		return tx.DeleteBlock(1)
	}); err != nil {
		t.Fatal(err)
	}
	if err := st2.Close(); err != nil {
		t.Fatal(err)
	}
	st3, err := NewFileBackedMemoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = st3.View(ctx, func(tx Tx) error {
		if _, ok, _ := tx.GetBlock(1); ok {
			t.Fatalf("rolled-back block survived restart")
		}
		if txs, _ := tx.ListTxs(1); len(txs) != 0 {
			t.Fatalf("rolled-back txs survived restart")
		}
		if max, ok, _ := tx.MaxHeight(); ok {
			t.Fatalf("expected empty store after restart, max=%d", max)
		}
		t.Logf("post-rollback restart verified: no block, no tx, no checkpoint; failures retained=%v", mustFailCount(t, tx))
		return nil
	})
}

func mustFailCount(t *testing.T, tx Tx) int {
	t.Helper()
	fs, err := tx.ListFailures()
	if err != nil {
		t.Fatal(err)
	}
	return len(fs)
}
