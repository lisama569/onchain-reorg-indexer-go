package index

import (
	"context"
	"testing"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/chain"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/model"
)

// TestMalformedInputsAreRejected covers every malformed-input class. Each
// rejected record must carry a distinct, stable reason code and be persisted
// as a FailureRecord; the checkpoint must not advance past it.
func TestMalformedInputsAreRejected(t *testing.T) {
	ctx := context.Background()
	const start = uint64(20)
	cases := []struct {
		name   string
		kind   chain.MalformedKind
		reason string
	}{
		{"unsupported version", chain.MalUnsupportedVersion, model.ReasonUnsupportedVersion},
		{"malformed json", chain.MalJSON, model.ReasonMalformedJSON},
		{"missing block hash", chain.MalMissingBlockHash, model.ReasonMissingBlockHash},
		{"missing parent hash", chain.MalMissingParentHash, model.ReasonMissingParentHash},
		{"missing tx hash", chain.MalMissingTxHash, model.ReasonMissingTxHash},
		{"bad address", chain.MalBadAddress, model.ReasonBadAddress},
		{"bad amount", chain.MalBadAmount, model.ReasonBadAmount},
		{"bad block checksum", chain.MalChecksumBlock, model.ReasonChecksumMismatch},
		{"bad tx checksum", chain.MalChecksumTx, model.ReasonChecksumMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sim := chain.NewSimChain(start)
			sim.Extend(1) // clean h=20
			sim.ExtendMalformed(1, tc.kind)
			ix, st := newTestIndexer(t, Config{StartHeight: start}, sim)

			clean := mustSubmit(t, ix, envAt(t, sim, "canonical", start))
			tlog(t, clean, "accepted clean predecessor")

			badEnv, err := sim.BlockAt(ctx, start+1)
			if err != nil {
				t.Fatalf("fetch malformed: %v", err)
			}
			res := mustSubmit(t, ix, badEnv)
			tlog(t, res, "malformed envelope")
			if res.Outcome != OutcomeRejected || res.Reason != tc.reason {
				t.Fatalf("want REJECTED/%s, got %s/%s", tc.reason, res.Outcome, res.Reason)
			}
			// Re-submitting the same bad record is idempotent: still rejected,
			// but no duplicate FailureRecord is stored.
			before := len(failureReasons(t, st))
			res2 := mustSubmit(t, ix, badEnv)
			tlog(t, res2, "duplicate malformed envelope")
			after := len(failureReasons(t, st))
			if before != after {
				t.Fatalf("duplicate rejection must not add a failure record: %d -> %d", before, after)
			}

			stats := statLog(t, ix, tc.name)
			if stats.NextHeight != start+1 {
				t.Fatalf("checkpoint must stay at %d (bad block not indexed), got %d", start+1, stats.NextHeight)
			}
			assertNoBlock(t, ix, start+1)
			if got := failureReasons(t, st)[tc.reason]; got != 1 {
				t.Fatalf("want exactly 1 persisted failure %s, got %d", tc.reason, got)
			}
		})
	}
}

// TestDistinguishableReasons asserts all reason codes in the catalog are
// distinct strings (stable public contract).
func TestDistinguishableReasons(t *testing.T) {
	reasons := []string{
		model.ReasonUnsupportedVersion, model.ReasonMalformedJSON,
		model.ReasonMissingBlockHash, model.ReasonMissingParentHash,
		model.ReasonMissingTxHash, model.ReasonBadAddress,
		model.ReasonBadAmount, model.ReasonChecksumMismatch,
		model.ReasonBadParentLink, model.ReasonBeforeStart,
		model.ReasonReorgTooDeep,
	}
	seen := map[string]bool{}
	for _, r := range reasons {
		if seen[r] {
			t.Fatalf("duplicate reason code %s", r)
		}
		seen[r] = true
	}
	t.Logf("catalog has %d distinguishable reason codes: %v", len(reasons), reasons)
}
