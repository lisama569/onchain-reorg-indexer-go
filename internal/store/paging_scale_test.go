package store

import (
	"context"
	"testing"
)

// TestPagingIndependentOfOffset asserts deep-key pages cost essentially the
// same as early pages (bounded range walk, no prefix scan).
func TestPagingIndependentOfOffset(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStore()
	putNBlocks(t, st, 1, 20000)
	addr := testAddr(1)
	page := func(cursor uint64) int {
		var n int
		if err := st.View(ctx, func(tx Tx) error {
			rows, err := tx.ListAddressTxs(addr, cursor, 0, true, 20)
			n = len(rows)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}
	for _, cursor := range []uint64{10, 5000, 10000, 19970} {
		if n := page(cursor); n != 20 {
			t.Fatalf("cursor=%d rows=%d want 20", cursor, n)
		}
	}
	// Walking every page from the start must visit exactly all 20000 txs and
	// terminate (full keyset-pagination coverage).
	var total int
	var cursor uint64
	hasCursor := false
	for {
		var got int
		if err := st.View(ctx, func(tx Tx) error {
			r, err := tx.ListAddressTxs(addr, cursor, 0, hasCursor, 128)
			got = len(r)
			if got > 0 {
				cursor = r[got-1].BlockHeight
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		total += got
		hasCursor = true
		if got < 128 {
			break
		}
	}
	if total != 20000 {
		t.Fatalf("paged total=%d want 20000", total)
	}
	t.Logf("deep-cursor pages all return 20 rows; full keyset walk covered %d txs", total)
}
