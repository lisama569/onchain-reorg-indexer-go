package index

import (
	"context"
	"testing"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/chain"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/model"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/store"
)

// tlog prints the mandated diagnostic fields: height, block identifier,
// checkpoint position and the decision basis.
func tlog(t *testing.T, res Result, extra string) {
	t.Helper()
	t.Logf("height=%d block=%s outcome=%s reason=%s checkpoint_next=%d basis=%q %s",
		res.Height, res.Hash.Short(), res.Outcome, res.Reason, res.Tip+1, res.Detail, extra)
}

func statLog(t *testing.T, ix *Indexer, phase string) Stats {
	t.Helper()
	st, err := ix.Stats(context.Background())
	if err != nil {
		t.Fatalf("%s stats: %v", phase, err)
	}
	t.Logf("[%s] checkpoint next_height=%d tip_height=%d tip_hash=%s buffered=%d blocks=%d failures=%d",
		phase, st.NextHeight, st.TipHeight, st.TipHash.Short(), st.Buffered, st.Blocks, st.Failures)
	return st
}

func newTestIndexer(t *testing.T, cfg Config, src chain.Source) (*Indexer, *store.MemoryStore) {
	t.Helper()
	st := store.NewMemoryStore()
	return New(cfg, st, src), st
}

func mustBlock(t *testing.T, ix *Indexer, h uint64) model.IndexedBlock {
	t.Helper()
	b, ok, err := ix.getBlock(context.Background(), h)
	if err != nil || !ok {
		t.Fatalf("expected block at height %d present, got ok=%v err=%v", h, ok, err)
	}
	return b
}

func assertNoBlock(t *testing.T, ix *Indexer, h uint64) {
	t.Helper()
	b, ok, err := ix.getBlock(context.Background(), h)
	if err != nil {
		t.Fatalf("get block %d: %v", h, err)
	}
	if ok {
		t.Fatalf("height %d must be absent after rollback, found hash %s", h, b.Hash.Short())
	}
}

func failureReasons(t *testing.T, st store.Store) map[string]int {
	t.Helper()
	out := map[string]int{}
	_ = st.View(context.Background(), func(tx store.Tx) error {
		fs, _ := tx.ListFailures()
		for _, f := range fs {
			out[f.Reason]++
		}
		return nil
	})
	return out
}

func mustSubmit(t *testing.T, ix *Indexer, env chain.Envelope) Result {
	t.Helper()
	res, err := ix.Submit(context.Background(), env)
	if err != nil {
		t.Fatalf("submit h=%d: unexpected error %v", env.Height, err)
	}
	return res
}

func envAt(t *testing.T, sim *chain.SimChain, branch string, h uint64) chain.Envelope {
	t.Helper()
	env, err := sim.BlockAtBranch(context.Background(), branch, h)
	if err != nil {
		t.Fatalf("fetch %s h=%d: %v", branch, h, err)
	}
	return env
}
