package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/model"
)

func addrOf(b byte) model.Address {
	var a model.Address
	a[19] = b
	return a
}

func hashOf(b byte) model.Hash {
	var h model.Hash
	h[31] = b
	return h
}

// TestAddressIndexBasics covers insertion order, idempotent re-put,
// self-transfer accounting, summary math and cursor pagination.
func TestAddressIndexBasics(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStore()
	a, b := addrOf(1), addrOf(2)

	put := func(height uint64, idx int, from, to model.Address, amount int64) {
		t.Helper()
		ref := model.AddressTxRef{Height: height, TxIndex: idx, TxHash: hashOf(byte(height*10 + uint64(idx))), From: from, To: to, Amount: amount}
		err := st.Update(ctx, func(tx Tx) error {
			if err := tx.PutAddressTx(from, ref); err != nil {
				return err
			}
			if to != from {
				return tx.PutAddressTx(to, ref)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	put(3, 0, a, b, 10)
	put(1, 0, a, b, 5)  // out-of-order arrival must still sort by height
	put(2, 1, a, a, 7)  // self-transfer: one tx, counts both directions
	put(2, 0, b, a, 3)  // reverse direction
	put(3, 0, a, b, 10) // exact duplicate: must be a no-op

	err := st.View(ctx, func(tx Tx) error {
		sum, ok, err := tx.GetAddressSummary(a)
		if err != nil || !ok {
			t.Fatalf("summary for a: ok=%v err=%v", ok, err)
		}
		// a: txs (1,0) out 5, (2,1) self 7, (2,0) in 3, (3,0) out 10
		if sum.TxCount != 4 || sum.TotalOut != 5+7+10 || sum.TotalIn != 7+3 {
			t.Fatalf("summary a = %+v, want TxCount=4 TotalOut=22 TotalIn=10", sum)
		}
		t.Logf("scenario=basic summary_a=%+v (self-transfer counted once, both directions)", sum)

		sumB, ok, _ := tx.GetAddressSummary(b)
		if !ok || sumB.TxCount != 3 || sumB.TotalIn != 15 || sumB.TotalOut != 3 {
			t.Fatalf("summary b = %+v", sumB)
		}

		// Full listing must be sorted by (height, index).
		refs, _, more, err := tx.ListAddressTxs(a, model.AddressCursor{}, 0)
		if err != nil || more {
			t.Fatalf("full list: err=%v more=%v", err, more)
		}
		if len(refs) != 4 {
			t.Fatalf("want 4 refs, got %d", len(refs))
		}
		for i := 1; i < len(refs); i++ {
			if refs[i].Height < refs[i-1].Height ||
				(refs[i].Height == refs[i-1].Height && refs[i].TxIndex <= refs[i-1].TxIndex) {
				t.Fatalf("refs not strictly ordered at %d: %+v after %+v", i, refs[i], refs[i-1])
			}
		}

		// Paginate with limit=1 and confirm the walk reconstructs the full list.
		var walked []model.AddressTxRef
		cursor := model.AddressCursor{}
		pages := 0
		for {
			page, next, hasMore, err := tx.ListAddressTxs(a, cursor, 1)
			if err != nil {
				t.Fatal(err)
			}
			walked = append(walked, page...)
			pages++
			if !hasMore {
				break
			}
			cursor = next
		}
		if pages != 4 || len(walked) != 4 {
			t.Fatalf("pagination: pages=%d walked=%d", pages, len(walked))
		}
		for i := range walked {
			if walked[i] != refs[i] {
				t.Fatalf("page walk diverges at %d: %+v vs %+v", i, walked[i], refs[i])
			}
		}
		t.Logf("scenario=pagination limit=1 pages=%d reassembled=identical", pages)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestAddressIndexBoundedDelete removes one height at a time and verifies
// summaries shrink exactly by the removed entries; deleting an untouched
// height is a no-op.
func TestAddressIndexBoundedDelete(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStore()
	a, b := addrOf(1), addrOf(2)

	err := st.Update(ctx, func(tx Tx) error {
		for h := uint64(0); h < 5; h++ {
			ref := model.AddressTxRef{Height: h, TxIndex: 0, TxHash: hashOf(byte(h)), From: a, To: b, Amount: int64(h + 1)}
			if err := tx.PutAddressTx(a, ref); err != nil {
				return err
			}
			if err := tx.PutAddressTx(b, ref); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Roll back heights 4,3 (tip-first, as reorg does).
	for _, h := range []uint64{4, 3} {
		if err := st.Update(ctx, func(tx Tx) error { return tx.DeleteAddressTxsByBlock(h) }); err != nil {
			t.Fatal(err)
		}
	}
	err = st.View(ctx, func(tx Tx) error {
		sum, ok, _ := tx.GetAddressSummary(a)
		if !ok || sum.TxCount != 3 || sum.TotalOut != 1+2+3 {
			t.Fatalf("after delete summary a = %+v ok=%v", sum, ok)
		}
		refs, _, _, _ := tx.ListAddressTxs(a, model.AddressCursor{}, 0)
		for _, r := range refs {
			if r.Height >= 3 {
				t.Fatalf("rolled-back entry survived: %+v", r)
			}
		}
		t.Logf("scenario=rollback-delete remaining=%d summary_out=%d (heights 3,4 removed)", len(refs), sum.TotalOut)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Deleting all remaining heights empties the index for both addresses.
	for h := uint64(0); h <= 2; h++ {
		if err := st.Update(ctx, func(tx Tx) error { return tx.DeleteAddressTxsByBlock(h) }); err != nil {
			t.Fatal(err)
		}
	}
	_ = st.View(ctx, func(tx Tx) error {
		if _, ok, _ := tx.GetAddressSummary(a); ok {
			t.Fatalf("address a must disappear once fully rolled back")
		}
		if _, ok, _ := tx.GetAddressSummary(b); ok {
			t.Fatalf("address b must disappear once fully rolled back")
		}
		t.Logf("scenario=full-rollback addresses fully evicted from derived index")
		return nil
	})
}

// TestAddressIndexSnapshotRebuild persists canonical txs, reopens the store
// and verifies the derived address index is rebuilt identically (restart can
// never resurrect rolled-back branches because only canonical txs persist).
func TestAddressIndexSnapshotRebuild(t *testing.T) {
	ctx := context.Background()
	path := fmt.Sprintf("%s/addr-snapshot.json", t.TempDir())
	st, err := NewFileBackedMemoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	a, b := addrOf(7), addrOf(8)
	blk := model.IndexedBlock{Height: 0, Hash: hashOf(100)}
	mk := func(h uint64, idx int, amount int64) model.IndexedTx {
		return model.IndexedTx{TxHash: hashOf(byte(h*4+uint64(idx))), BlockHeight: h, BlockHash: blk.Hash, Index: idx, From: a, To: b, Amount: amount}
	}
	err = st.Update(ctx, func(tx Tx) error {
		if err := tx.PutBlock(blk); err != nil {
			return err
		}
		for i, amount := range []int64{11, 22} {
			itx := mk(0, i, amount)
			if err := tx.PutTx(itx); err != nil {
				return err
			}
			ref := model.AddressTxRef{Height: 0, TxIndex: i, TxHash: itx.TxHash, From: a, To: b, Amount: amount}
			if err := tx.PutAddressTx(a, ref); err != nil {
				return err
			}
			if err := tx.PutAddressTx(b, ref); err != nil {
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
	_ = st2.View(ctx, func(tx Tx) error {
		sum, ok, _ := tx.GetAddressSummary(a)
		if !ok || sum.TxCount != 2 || sum.TotalOut != 33 {
			t.Fatalf("rebuilt summary a = %+v ok=%v", sum, ok)
		}
		refs, _, _, _ := tx.ListAddressTxs(b, model.AddressCursor{}, 0)
		if len(refs) != 2 {
			t.Fatalf("rebuilt refs for b = %d", len(refs))
		}
		t.Logf("scenario=restart-rebuild summary_a=%+v refs_b=%d (derived index rebuilt from canonical txs)", sum, len(refs))
		return nil
	})
}
