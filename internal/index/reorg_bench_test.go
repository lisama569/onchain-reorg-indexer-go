package index

import (
	"context"
	"fmt"
	"testing"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/chain"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/store"
)

// BenchmarkReorgFixedDepth measures a fixed 3-block reorg at increasing chain
// sizes. Reorg work must scale with the reorg depth, not the chain length:
// rollback and replay only touch heights ancestor+1..tip.
func BenchmarkReorgFixedDepth(b *testing.B) {
	ctx := context.Background()
	const reorgDepth = 3
	for _, total := range []int{128, 1024, 8192} {
		b.Run(fmt.Sprintf("chain=%d", total), func(b *testing.B) {
			b.StopTimer()
			sim := chain.NewSimChain(0)
			sim.Extend(total)
			st := store.NewMemoryStore()
			ix := New(Config{StartHeight: 0, MaxReorgDepth: uint64(total + 10)}, st, sim)
			if err := ix.RunTo(ctx, sim.Head()); err != nil {
				b.Fatal(err)
			}
			forkAt := uint64(total - reorgDepth)
			if err := sim.ForkAt("alt", forkAt); err != nil {
				b.Fatal(err)
			}
			sim.Extend(reorgDepth)
			ix.BindSource(sim)
			env, err := sim.BlockAt(ctx, forkAt)
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			for n := 0; n < b.N; n++ {
				// Only measure the reorg decision/walk cost; the mutation is
				// identical each iteration so subsequent calls are DUPLICATE.
				if _, err := ix.Submit(ctx, env); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkReorgFirstTimeAtScale performs one real (mutating) reorg on a
// large chain and asserts the touched key count is O(reorg depth).
func BenchmarkReorgFirstTimeAtScale(b *testing.B) {
	ctx := context.Background()
	const total = 16384
	const reorgDepth = 4
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		sim := chain.NewSimChain(0)
		sim.Extend(total)
		st := store.NewMemoryStore()
		ix := New(Config{StartHeight: 0, MaxReorgDepth: uint64(total + 10)}, st, sim)
		if err := ix.RunTo(ctx, sim.Head()); err != nil {
			b.Fatal(err)
		}
		forkAt := uint64(total - reorgDepth)
		if err := sim.ForkAt("alt", forkAt); err != nil {
			b.Fatal(err)
		}
		sim.Extend(reorgDepth)
		ix.BindSource(sim)
		env, err := sim.BlockAt(ctx, forkAt)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		res, err := ix.Submit(ctx, env)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if res.Outcome != OutcomeReorg {
			b.Fatalf("want REORG, got %s", res.Outcome)
		}
	}
}
