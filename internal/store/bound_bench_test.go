package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/model"
)

// BenchmarkIngestAtScale measures the cost of ingesting one additional block
// into chains of very different sizes. Commit persistence is the incremental
// journal (plus at most one periodic full compaction per threshold window), so
// per-block cost must stay roughly flat as history grows.
func BenchmarkIngestAtScale(b *testing.B) {
	ctx := context.Background()
	for _, total := range []int{1_000, 10_000, 50_000} {
		b.Run(fmt.Sprintf("chain=%d", total), func(b *testing.B) {
			st := NewMemoryStore()
			putNBlocks(b, st, 1, total)
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				h := uint64(total + n + 1)
				bh, th := testHashes(byte(h))
				err := st.Update(ctx, func(tx Tx) error {
					if err := tx.PutBlock(model.IndexedBlock{Height: h, Hash: bh, TxCount: 1}); err != nil {
						return err
					}
					return tx.PutTx(model.IndexedTx{
						TxHash: th, BlockHeight: h, BlockHash: bh, Index: 0,
						From: testAddr(1), To: testAddr(2), Amount: int64(h),
					})
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkAddressQueryAtScale measures one address summary read and one
// paginated listing against a large index; both must be independent of total
// chain length.
func BenchmarkAddressQueryAtScale(b *testing.B) {
	ctx := context.Background()
	const total = 50_000
	st := NewMemoryStore()
	putNBlocks(b, st, 1, total)
	addr := testAddr(1)
	b.ResetTimer()
	b.Run("summary", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			if err := st.View(ctx, func(tx Tx) error {
				_, _, err := tx.GetAddressSummary(addr)
				return err
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("page20", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			if err := st.View(ctx, func(tx Tx) error {
				rows, err := tx.ListAddressTxs(addr, 10_000, 0, true, 20)
				if err != nil {
					return err
				}
				if len(rows) != 20 {
					b.Fatalf("rows=%d want 20", len(rows))
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
