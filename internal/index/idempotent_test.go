package index

import (
	"context"
	"testing"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/chain"
)

// TestDuplicateSubmissionIsIdempotent: resubmitting already-indexed blocks in
// arbitrary order never creates duplicates and never moves the checkpoint.
func TestDuplicateSubmissionIsIdempotent(t *testing.T) {
	ctx := context.Background()
	const start = uint64(30)
	sim := chain.NewSimChain(start)
	sim.Extend(5) // 30..34
	ix, _ := newTestIndexer(t, Config{StartHeight: start}, sim)
	if err := ix.RunTo(ctx, sim.Head()); err != nil {
		t.Fatal(err)
	}
	first := statLog(t, ix, "fully ingested")

	// Replay the whole range three times, shuffled, including duplicates
	// within a single batch.
	order := []uint64{33, 30, 34, 31, 33, 32, 30, 34, 31, 32}
	for _, h := range order {
		res := mustSubmit(t, ix, envAt(t, sim, "canonical", h))
		tlog(t, res, "duplicate replay")
		if res.Outcome != OutcomeDuplicate {
			t.Fatalf("h=%d want DUPLICATE, got %s (%s)", h, res.Outcome, res.Detail)
		}
		if res.Tip != 34 {
			t.Fatalf("duplicate must not move checkpoint, tip=%d", res.Tip)
		}
	}
	second := statLog(t, ix, "after duplicate replay")
	if second.NextHeight != first.NextHeight || second.TipHash != first.TipHash {
		t.Fatalf("checkpoint changed after duplicates: %+v vs %+v", first, second)
	}
	if second.Blocks != first.Blocks || second.Failures != 0 {
		t.Fatalf("state changed after duplicates: blocks %d->%d failures=%d",
			first.Blocks, second.Blocks, second.Failures)
	}
	// Exactly one tx set per block, contiguous and in index order.
}

// TestOutOfOrderIngestionDrainsInOrder: submit with gaps and in reverse;
// blocks ahead of the checkpoint are buffered and auto-drain as gaps close.
func TestOutOfOrderIngestionDrainsInOrder(t *testing.T) {
	const start = uint64(40)
	sim := chain.NewSimChain(start)
	sim.Extend(6) // 40..45
	ix, _ := newTestIndexer(t, Config{StartHeight: start}, sim)

	// Submit future blocks first: all must buffer, nothing indexed.
	for _, h := range []uint64{45, 43, 42, 44} {
		res := mustSubmit(t, ix, envAt(t, sim, "canonical", h))
		tlog(t, res, "future block submitted early")
		if res.Outcome != OutcomeBuffered {
			t.Fatalf("h=%d want BUFFERED, got %s", h, res.Outcome)
		}
	}
	st := statLog(t, ix, "gaps buffered")
	if st.Buffered != 4 || st.NextHeight != start {
		t.Fatalf("want 4 buffered and checkpoint at %d, got %+v", start, st)
	}

	// Submitting the genesis starts a cascade drain up to 41 only (40,41 gap).
	mustSubmit(t, ix, envAt(t, sim, "canonical", 40))
	res41 := mustSubmit(t, ix, envAt(t, sim, "canonical", 41))
	tlog(t, res41, "gap-closing submission drains 42..45")
	if res41.Tip != 45 {
		t.Fatalf("after gaps close tip must be 45, got %d", res41.Tip)
	}
	final := statLog(t, ix, "drained")
	if final.Buffered != 0 || final.TipHeight != 45 {
		t.Fatalf("buffered=%d tip=%d, want 0/45", final.Buffered, final.TipHeight)
	}
	for h := start; h <= 45; h++ {
		mustBlock(t, ix, h) // no height lost
	}
}

// TestCheckpointRegressionAndResubmit: a stream rewinds to old heights and
// replays identical blocks, then a genuinely different hash at an old height
// triggers a reorg instead of being silently overwritten.
func TestCheckpointRegressionAndResubmit(t *testing.T) {
	ctx := context.Background()
	const start = uint64(50)
	sim := chain.NewSimChain(start)
	sim.Extend(5) // 50..54
	ix, _ := newTestIndexer(t, Config{StartHeight: start, MaxReorgDepth: 64}, sim)
	if err := ix.RunTo(ctx, sim.Head()); err != nil {
		t.Fatal(err)
	}

	// Consumer offset rewinds: resubmit 52..50.
	for _, h := range []uint64{52, 51, 50} {
		res := mustSubmit(t, ix, envAt(t, sim, "canonical", h))
		tlog(t, res, "checkpoint-regressed replay")
		if res.Outcome != OutcomeDuplicate {
			t.Fatalf("regressed identical block must be DUPLICATE, got %s", res.Outcome)
		}
	}

	// Different hash at an already-committed height => reorg, not overwrite.
	if err := sim.ForkAt("alt", 53); err != nil {
		t.Fatal(err)
	}
	sim.Extend(3)
	ix.BindSource(sim)
	res := mustSubmit(t, ix, envAt(t, sim, "alt", 53))
	tlog(t, res, "different hash at committed height")
	if res.Outcome != OutcomeReorg {
		t.Fatalf("old-height fork must REORG, got %s", res.Outcome)
	}
}

// TestBeforeStartHeightRejected: blocks below the configured start height are
// rejected with a dedicated reason and never stored.
func TestBeforeStartHeightRejected(t *testing.T) {
	const start = uint64(70)
	sim := chain.NewSimChain(start - 1) // source legitimately contains start-1
	sim.Extend(2)
	ix, st := newTestIndexer(t, Config{StartHeight: start}, sim)
	low := envAt(t, sim, "canonical", start-1) // valid envelope, below start
	res := mustSubmit(t, ix, low)
	tlog(t, res, "below-start envelope")
	if res.Outcome != OutcomeRejected || res.Reason != "BEFORE_START_HEIGHT" {
		t.Fatalf("want REJECTED/BEFORE_START_HEIGHT, got %s/%s", res.Outcome, res.Reason)
	}
	if got := failureReasons(t, st)["BEFORE_START_HEIGHT"]; got != 1 {
		t.Fatalf("want 1 before-start failure, got %d", got)
	}
	statLog(t, ix, "after below-start")
}
