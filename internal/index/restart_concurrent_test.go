package index

import (
	"context"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/chain"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/store"
)

// TestEndToEndRestartContinuation: ingest a prefix, shut down, rebuild from
// the JSON snapshot with a fresh source instance, resume, and confirm the
// final chain matches block-for-block.
func TestEndToEndRestartContinuation(t *testing.T) {
	ctx := context.Background()
	const start = uint64(10)
	snap := filepath.Join(t.TempDir(), "e2e-snapshot.json")

	build := func() (*chain.SimChain, *Indexer) {
		sim := chain.NewSimChain(start)
		sim.Extend(8) // 10..17
		st, err := store.NewFileBackedMemoryStore(snap)
		if err != nil {
			t.Fatal(err)
		}
		return sim, New(Config{StartHeight: start}, st, sim)
	}

	_, ix1 := build()
	if err := ix1.RunTo(ctx, start+3); err != nil { // stop after ingesting 13
		t.Fatal(err)
	}
	statLog(t, ix1, "before shutdown")
	if err := ix1.Close(); err != nil {
		t.Fatal(err)
	}

	sim2, ix2 := build()
	stats := statLog(t, ix2, "after restart (checkpoint recovered)")
	if stats.NextHeight != start+4 {
		t.Fatalf("restart must resume at %d, got %d", start+4, stats.NextHeight)
	}
	if err := ix2.RunTo(ctx, sim2.Head()); err != nil {
		t.Fatalf("resume: %v", err)
	}
	statLog(t, ix2, "after resume")
	for h := start; h <= 17; h++ {
		want, err := sim2.BlockHashAt("canonical", h)
		if err != nil {
			t.Fatal(err)
		}
		if got := mustBlock(t, ix2, h); got.Hash.Hex() != want {
			t.Fatalf("h=%d mismatch after restart", h)
		}
	}
}

// perm returns a deterministic permutation of [start, start+n), seeded per
// worker so runs are reproducible.
func perm(worker, n int, start uint64) []uint64 {
	r := rand.New(rand.NewSource(int64(worker*1_000_003 + n)))
	out := make([]uint64, n)
	for i := range out {
		out[i] = start + uint64(i)
	}
	r.Shuffle(n, func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// TestConcurrentIngestionIsLossless: workers submit the full range in
// shuffled order, with the chain growing concurrently. Every envelope is
// delivered at least once, many delivered many times and out of order. The
// final blocks must form exactly the canonical chain: contiguous, correctly
// linked, no gaps, no duplicates, complete transaction sets.
func TestConcurrentIngestionIsLossless(t *testing.T) {
	ctx := context.Background()
	const start = uint64(0)
	const total = uint64(60)
	const workers = 12
	sim := chain.NewSimChain(start)
	ix, st := newTestIndexer(t, Config{StartHeight: start, MaxReorgDepth: 200}, sim)

	// Grower produces blocks gradually across the whole ingestion window so
	// workers genuinely race a moving head and must buffer/reorder.
	var growWG sync.WaitGroup
	growWG.Add(1)
	go func() {
		defer growWG.Done()
		for h := uint64(0); h < total; h++ {
			sim.Extend(1)
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// Each worker tracks its own delivery set and keeps retrying the
			// missing heights (with shuffled order) until all are indexed.
			delivered := map[uint64]bool{}
			round := 0
			for len(delivered) < int(total) {
				head := sim.Head()
				round++
				// Head() on a still-empty chain is start-1, which underflows
				// to MaxUint64 when start == 0; only scan heights this test
				// will ever produce. Without the cap a worker that wins the
				// race against the grower spins to 2^64 and OOMs.
				scanTo := head
				if max := start + total - 1; scanTo > max {
					scanTo = max
				}
				missing := make([]uint64, 0)
				for hh := start; hh <= scanTo; hh++ {
					if !delivered[hh] {
						missing = append(missing, hh)
					}
				}
				if len(missing) == 0 {
					// Chain not grown yet; yield instead of busy-spinning.
					time.Sleep(time.Millisecond)
					continue
				}
				idx := perm(w*31+round, len(missing), 0)
				shuffled := make([]uint64, len(missing))
				for i, pos := range idx {
					shuffled[i] = missing[pos-start]
				}
				for _, h := range shuffled {
					env, err := sim.BlockAt(ctx, h)
					if err != nil {
						return
					}
					res, err := ix.Submit(ctx, env)
					if err != nil {
						t.Errorf("worker %d submit h=%d: %v", w, h, err)
						return
					}
					if res.Outcome == OutcomeRejected {
						t.Errorf("worker %d unexpected rejection h=%d: %s (%s)", w, h, res.Reason, res.Detail)
						return
					}
					delivered[h] = true
				}
			}
		}(w)
	}
	growWG.Wait()
	wg.Wait()

	stats := statLog(t, ix, "after concurrent ingestion")
	if stats.TipHeight != start+total-1 || stats.Buffered != 0 || stats.Failures != 0 {
		t.Fatalf("bad final state: %+v", stats)
	}
	for h := uint64(0); h < total; h++ {
		want, err := sim.BlockHashAt("canonical", start+h)
		if err != nil {
			t.Fatal(err)
		}
		got := mustBlock(t, ix, start+h)
		if got.Hash.Hex() != want {
			t.Fatalf("h=%d got %s want %s", h, got.Hash.Short(), want)
		}
		var txCount int
		if err := st.View(ctx, func(tx store.Tx) error {
			txs, verr := tx.ListTxs(start + h)
			txCount = len(txs)
			return verr
		}); err != nil {
			t.Fatal(err)
		}
		if txCount != got.TxCount || txCount < 1 {
			t.Fatalf("h=%d txs=%d block.TxCount=%d", h, txCount, got.TxCount)
		}
		if h > 0 {
			prev := mustBlock(t, ix, start+h-1)
			if got.ParentHash != prev.Hash {
				t.Fatalf("h=%d parent linkage broken", h)
			}
		}
	}
	t.Logf("verified %d contiguous blocks with linked parents and complete tx sets", total)
}
