package index

import (
	"context"
	"testing"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/chain"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/model"
)

// TestReorgRollbackAndReplay: ingest canonical 0..7, fork alt at height 5
// (ancestor 4), submit alt block 5' and catch up. The indexed chain must
// match alt exactly: blocks 0..4 shared, 5..7 rolled back, 5'..8' replayed.
func TestReorgRollbackAndReplay(t *testing.T) {
	ctx := context.Background()
	const start = uint64(0)
	sim := chain.NewSimChain(start)
	sim.Extend(8) // 0..7
	ix, _ := newTestIndexer(t, Config{StartHeight: start, MaxReorgDepth: 64}, sim)
	if err := ix.RunTo(ctx, sim.Head()); err != nil {
		t.Fatalf("initial ingest: %v", err)
	}
	statLog(t, ix, "canonical ingested")

	canonicalHashes := map[uint64]string{}
	for h := uint64(0); h <= 7; h++ {
		hh, err := sim.BlockHashAt("canonical", h)
		if err != nil {
			t.Fatal(err)
		}
		canonicalHashes[h] = hh
	}

	const forkAt = uint64(5)
	if err := sim.ForkAt("alt", forkAt); err != nil {
		t.Fatal(err)
	}
	sim.Extend(4) // 5',6',7',8'
	ix.BindSource(sim)

	// Back-dated submission of alt block 5' triggers the reorg.
	res := mustSubmit(t, ix, envAt(t, sim, "alt", forkAt))
	tlog(t, res, "back-dated alt block")
	if res.Outcome != OutcomeReorg {
		t.Fatalf("want REORG, got %s (%s)", res.Outcome, res.Detail)
	}

	// Catch up to alt head.
	if err := ix.RunTo(ctx, sim.Head()); err != nil {
		t.Fatalf("replay: %v", err)
	}
	stats := statLog(t, ix, "after reorg replay")
	if stats.TipHeight != 8 {
		t.Fatalf("want tip 8, got %d", stats.TipHeight)
	}

	// Exact state equality with the alt chain, height by height.
	for h := uint64(0); h <= 8; h++ {
		wantHash, err := sim.BlockHashAt("alt", h)
		if err != nil {
			t.Fatal(err)
		}
		got := mustBlock(t, ix, h)
		if got.Hash.Hex() != wantHash {
			t.Fatalf("height %d mismatch after reorg: want %s got %s", h, wantHash, got.Hash.Hex())
		}
		if h < forkAt && got.Hash.Hex() != canonicalHashes[h] {
			t.Fatalf("shared height %d must keep canonical hash", h)
		}
		t.Logf("verify h=%d block=%s matches alt (shared=%v)", h, got.Hash.Short(), h < forkAt)
	}
	// No residue at any other height and no new failures.
	assertNoBlock(t, ix, 9)
	if got := failureReasons(t, ix.store); len(got) != 0 {
		t.Fatalf("clean reorg must not record failures, got %v", got)
	}
}

// TestReorgTooDeepRejected: a fork whose divergence exceeds the finality
// window must be explicitly REJECTED and leave zero residual mutation.
func TestReorgTooDeepRejected(t *testing.T) {
	ctx := context.Background()
	const start = uint64(0)
	const depth = uint64(2)
	sim := chain.NewSimChain(start)
	sim.Extend(6) // 0..5
	ix, st := newTestIndexer(t, Config{StartHeight: start, MaxReorgDepth: depth}, sim)
	if err := ix.RunTo(ctx, sim.Head()); err != nil {
		t.Fatal(err)
	}
	before := statLog(t, ix, "before deep reorg")

	if err := sim.ForkAt("alt", 0); err != nil { // diverges at genesis: rollback depth 6 > 2
		t.Fatal(err)
	}
	sim.Extend(3)
	ix.BindSource(sim)

	res := mustSubmit(t, ix, envAt(t, sim, "alt", 0))
	tlog(t, res, "fork beyond finality window")
	if res.Outcome != OutcomeRejected || res.Reason != model.ReasonReorgTooDeep {
		t.Fatalf("want REJECTED/REORG_TOO_DEEP, got %s/%s", res.Outcome, res.Reason)
	}
	// The indexed state must be byte-for-byte unchanged.
	after := statLog(t, ix, "after rejected deep reorg")
	if after.TipHeight != before.TipHeight || after.TipHash != before.TipHash {
		t.Fatalf("state mutated despite rejected reorg: %+v vs %+v", before, after)
	}
	for h := uint64(0); h <= 5; h++ {
		want, err := sim.BlockHashAt("canonical", h)
		if err != nil {
			t.Fatal(err)
		}
		if got := mustBlock(t, ix, h); got.Hash.Hex() != want {
			t.Fatalf("height %d changed after rejected reorg", h)
		}
	}
	if got := failureReasons(t, st)[model.ReasonReorgTooDeep]; got != 1 {
		t.Fatalf("want 1 REORG_TOO_DEEP failure record, got %d", got)
	}
}

// TestForwardReorgAtTip: the expected next height arrives with a parent that
// does not match the indexed tip; the fork diverges at the tip itself, so the
// common ancestor is tip-1 and exactly one block is rolled back and replayed.
func TestForwardReorgAtTip(t *testing.T) {
	ctx := context.Background()
	const start = uint64(100)
	sim := chain.NewSimChain(start)
	sim.Extend(3) // 100..102
	ix, _ := newTestIndexer(t, Config{StartHeight: start, MaxReorgDepth: 64}, sim)
	if err := ix.RunTo(ctx, sim.Head()); err != nil {
		t.Fatal(err)
	}
	tip := start + 2
	if err := sim.ForkAt("alt", tip); err != nil { // alt shares 100..101, replaces tip 102
		t.Fatal(err)
	}
	sim.Extend(1) // 102' whose parent is canonical 101
	ix.BindSource(sim)
	res := mustSubmit(t, ix, envAt(t, sim, "alt", tip))
	tlog(t, res, "alt block at tip height")
	if res.Outcome != OutcomeReorg {
		t.Fatalf("want REORG, got %s: %s", res.Outcome, res.Detail)
	}
	want, _ := sim.BlockHashAt("alt", tip)
	if got := mustBlock(t, ix, tip); got.Hash.Hex() != want {
		t.Fatalf("forward reorg result mismatch")
	}
}
