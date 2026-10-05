package query

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/chain"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/decode"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/index"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/model"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/store"
)

const genesisParent = "0x0000000000000000000000000000000000000000000000000000000000000001"

// forkPlan describes tx content of canonical (h 1..6) and alt (h 5..6).
func canonicalSpecs(h uint64) []txSpec {
	switch h {
	case 1:
		return []txSpec{
			{testAccounts[0], testAccounts[1], 100},
			{testAccounts[2], testAccounts[0], 40},
		}
	case 2:
		return []txSpec{
			{testAccounts[1], testAccounts[2], 30},
			{testAccounts[0], testAccounts[0], 25}, // self-transfer: one tx, both flows
		}
	case 3:
		return []txSpec{
			{testAccounts[3], testAccounts[0], 700},
		}
	case 4:
		return []txSpec{
			{testAccounts[0], testAccounts[4], 500},
			{testAccounts[4], testAccounts[1], 200},
			{testAccounts[2], testAccounts[3], 10},
		}
	case 5:
		return []txSpec{
			{testAccounts[1], testAccounts[0], 999}, // stale on the old branch
		}
	case 6:
		return []txSpec{
			{testAccounts[0], testAccounts[2], 888}, // stale on the old branch
		}
	}
	return nil
}

func altSpecs(h uint64) []txSpec {
	switch h {
	case 5:
		return []txSpec{
			{testAccounts[4], testAccounts[2], 333}, // replaces canonical h5
		}
	case 6:
		return []txSpec{
			{testAccounts[2], testAccounts[4], 444},
			{testAccounts[0], testAccounts[3], 555},
		}
	}
	return nil
}

type builtBlock struct {
	env chain.Envelope
	b   model.IndexedBlock
}

// buildChain constructs a linked chain of envelopes from height 1.
func buildChain(t *testing.T, highest uint64, specs func(h uint64) []txSpec) map[uint64]builtBlock {
	t.Helper()
	out := map[uint64]builtBlock{}
	parent := genesisParent
	for h := uint64(1); h <= highest; h++ {
		env := makeEnvelope(t, h, parent, specs(h))
		dec, err := decode.Decode(env)
		if err != nil {
			t.Fatalf("decode h=%d: %v", h, err)
		}
		out[h] = builtBlock{env: env, b: dec.Block}
		parent = dec.Block.Hash.Hex()
	}
	return out
}

// buildAltFrom constructs an alt chain that literally shares blocks 1..fork-1
// with the given canonical chain and diverges from fork onwards.
func buildAltFrom(t *testing.T, canon map[uint64]builtBlock, fork, highest uint64, specs func(h uint64) []txSpec) map[uint64]builtBlock {
	t.Helper()
	out := map[uint64]builtBlock{}
	for h := uint64(1); h < fork; h++ {
		out[h] = canon[h]
	}
	parent := canon[fork-1].b.Hash.Hex()
	for h := fork; h <= highest; h++ {
		env := makeEnvelope(t, h, parent, specs(h))
		dec, err := decode.Decode(env)
		if err != nil {
			t.Fatalf("decode alt h=%d: %v", h, err)
		}
		out[h] = builtBlock{env: env, b: dec.Block}
		parent = dec.Block.Hash.Hex()
	}
	return out
}

// sourceFrom registers all blocks of a chain in a test memSource.
func sourceFrom(ms *memSource, blocks map[uint64]builtBlock) {
	for _, blk := range blocks {
		ms.put(blk.env)
	}
}

// submit submits an envelope and requires a non-reject outcome.
func submit(t *testing.T, ix *index.Indexer, env chain.Envelope) index.Result {
	t.Helper()
	res, err := ix.Submit(context.Background(), env)
	if err != nil {
		t.Fatalf("submit h=%d: %v", env.Height, err)
	}
	if res.Outcome == index.OutcomeRejected {
		t.Fatalf("submit h=%d rejected: %s (%s)", env.Height, res.Reason, res.Detail)
	}
	return res
}

// TestAddressQueriesAndSummary covers the new consumer-facing read model:
// ordered paginated per-address tx listing and aggregate summaries.
func TestAddressQueriesAndSummary(t *testing.T) {
	blocks := buildChain(t, 4, canonicalSpecs)
	st := store.NewMemoryStore()
	ix := index.New(index.Config{StartHeight: 1}, st, nil)
	q := New(st)
	m := newExpectedModel()

	t.Log("== scenario A: incremental ingestion, paginated listing + summary ==")
	for h := uint64(1); h <= 4; h++ {
		res := submit(t, ix, blocks[h].env)
		t.Logf("ingest h=%d outcome=%s tip=%d basis=%q", h, res.Outcome, res.Tip, res.Detail)
		m.addBlock(t, blocks[h].env)
		assertAddressQueriesMatch(t, q, st, m, "ingest")
	}
}

// TestAddressQueriesSurviveReorg verifies that after rolling back to the
// common ancestor and replaying the new branch, stale-branch txs disappear
// from both the listing and the summary and new-branch txs appear, with no
// duplicates and full agreement with the primary index.
func TestAddressQueriesSurviveReorg(t *testing.T) {
	canon := buildChain(t, 6, canonicalSpecs)
	src := newMemSource()
	st := store.NewMemoryStore()
	ix := index.New(index.Config{StartHeight: 1, MaxReorgDepth: 10}, st, src)
	q := New(st)
	m := newExpectedModel()

	t.Log("== scenario B: ingest canonical 1..6 ==")
	for h := uint64(1); h <= 6; h++ {
		submit(t, ix, canon[h].env)
		m.addBlock(t, canon[h].env)
	}
	assertAddressQueriesMatch(t, q, st, m, "canonical")

	// Stale-branch amounts must be visible before the fork.
	acct1 := acct(t, testAccounts[0])
	if sum, ok, _ := q.Summary(context.Background(), acct1); !ok || sum.Outflow != 100+25+500+888 {
		t.Fatalf("pre-reorg outflow for acct1: %+v ok=%v", sum, ok)
	}

	t.Log("== scenario B: fork at height 5 (ancestor = block 4), replay alt 5..6 ==")
	alt := buildAltFrom(t, canon, 5, 6, altSpecs)
	sourceFrom(src, alt)
	// Sanity: alt and canonical share blocks 1..4.
	for h := uint64(1); h <= 4; h++ {
		if alt[h].b.Hash != canon[h].b.Hash {
			t.Fatalf("alt/canonical block %d unexpectedly differs", h)
		}
	}
	res := submit(t, ix, alt[5].env)
	t.Logf("reorg h=5 outcome=%s basis=%q", res.Outcome, res.Detail)
	if res.Outcome != index.OutcomeReorg {
		t.Fatalf("want REORG, got %s", res.Outcome)
	}
	res = submit(t, ix, alt[6].env)
	t.Logf("reorg h=6 outcome=%s basis=%q", res.Outcome, res.Detail)

	m.dropAfter(4)
	m.addBlock(t, alt[5].env)
	m.addBlock(t, alt[6].env)
	assertAddressQueriesMatch(t, q, st, m, "post-reorg")

	// Explicit stale/new amount assertions.
	if sum, ok, _ := q.Summary(context.Background(), acct1); !ok || sum.Outflow != 100+25+500+555 {
		t.Fatalf("post-reorg outflow for acct1: %+v ok=%v (stale 888 must be gone, new 555 present)", sum, ok)
	}
	staleHash := txHashOf(t, canon[6].env, 0)
	for _, tx := range allTxsFor(t, q, acct1, 50) {
		if tx.TxHash == staleHash {
			t.Fatalf("stale-branch tx %s survived reorg in address listing", staleHash.Short())
		}
	}
	newHash := txHashOf(t, alt[6].env, 1)
	found := false
	for _, tx := range allTxsFor(t, q, acct1, 50) {
		if tx.TxHash == newHash {
			found = true
		}
	}
	if !found {
		t.Fatalf("new-branch tx %s missing from address listing", newHash.Short())
	}
	t.Log("reorg consistency verified: stale txs absent, new txs present, summaries recomputed")
}

// TestAddressQueriesRestart verifies checkpoint recovery: close, reopen the
// file-backed store, resume ingestion and confirm the query view is intact and
// a reorg performed before restart cannot be queried back afterwards.
func TestAddressQueriesRestart(t *testing.T) {
	snap := filepath.Join(t.TempDir(), "addr-snapshot.json")
	canon := buildChain(t, 6, canonicalSpecs)

	st1, err := store.NewFileBackedMemoryStore(snap)
	if err != nil {
		t.Fatal(err)
	}
	src1 := newMemSource()
	ix1 := index.New(index.Config{StartHeight: 1, MaxReorgDepth: 10}, st1, src1)
	q1 := New(st1)
	m := newExpectedModel()
	for h := uint64(1); h <= 5; h++ {
		submit(t, ix1, canon[h].env)
		m.addBlock(t, canon[h].env)
	}

	t.Log("== scenario C: reorg h5 to alt before shutdown ==")
	alt := buildAltFrom(t, canon, 5, 6, altSpecs)
	sourceFrom(src1, alt)
	res := submit(t, ix1, alt[5].env)
	if res.Outcome != index.OutcomeReorg {
		t.Fatalf("want REORG pre-restart, got %s", res.Outcome)
	}
	m.dropAfter(4)
	m.addBlock(t, alt[5].env)
	assertAddressQueriesMatch(t, q1, st1, m, "pre-restart reorg")
	if err := ix1.Close(); err != nil {
		t.Fatal(err)
	}

	t.Log("== scenario C: restart from disk, verify no stale revival and resume ==")
	st2, err := store.NewFileBackedMemoryStore(snap)
	if err != nil {
		t.Fatal(err)
	}
	q2 := New(st2)
	assertAddressQueriesMatch(t, q2, st2, m, "post-restart")
	src2 := newMemSource()
	sourceFrom(src2, alt)
	ix2 := index.New(index.Config{StartHeight: 1, MaxReorgDepth: 10}, st2, src2)
	submit(t, ix2, alt[6].env)
	m.addBlock(t, alt[6].env)
	assertAddressQueriesMatch(t, q2, st2, m, "post-restart resume")
	if err := ix2.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("restart verified: derived indexes recovered, stale branch stayed gone, resume consistent")
}

// TestAddressQueriesDuplicateOutOfOrder verifies duplicate delivery and
// out-of-order buffering never add or lose a derived row.
func TestAddressQueriesDuplicateOutOfOrder(t *testing.T) {
	blocks := buildChain(t, 4, canonicalSpecs)
	st := store.NewMemoryStore()
	ix := index.New(index.Config{StartHeight: 1}, st, nil)
	q := New(st)
	m := newExpectedModel()

	t.Log("== scenario D: gap delivery (3,4 buffered), duplicates ==")
	submit(t, ix, blocks[1].env)
	m.addBlock(t, blocks[1].env)
	submit(t, ix, blocks[3].env) // buffered
	submit(t, ix, blocks[4].env) // buffered
	submit(t, ix, blocks[1].env) // duplicate
	assertAddressQueriesMatch(t, q, st, m, "gapped")
	submit(t, ix, blocks[2].env) // closes gap; drains 3,4
	m.addBlock(t, blocks[2].env)
	m.addBlock(t, blocks[3].env)
	m.addBlock(t, blocks[4].env)
	submit(t, ix, blocks[2].env)
	submit(t, ix, blocks[4].env)
	assertAddressQueriesMatch(t, q, st, m, "drained")
	t.Log("duplicate/out-of-order verified: derived rows neither duplicated nor missing")
}

// TestAddressQueriesConcurrent drives many concurrent submitters with
// duplicates and verifies the final derived view exactly matches the primary
// index and the expected model.
func TestAddressQueriesConcurrent(t *testing.T) {
	blocks := buildChain(t, 6, canonicalSpecs)
	st := store.NewMemoryStore()
	ix := index.New(index.Config{StartHeight: 1, MaxReorgDepth: 10}, st, nil)
	q := New(st)
	m := newExpectedModel()

	t.Log("== scenario E: concurrent delivery, every block delivered many times ==")
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			order := []uint64{3, 1, 4, 2, 6, 5, 1, 2, 5, 3, 6, 4}
			for _, h := range order {
				_, _ = ix.Submit(context.Background(), blocks[h].env)
			}
		}(w)
	}
	wg.Wait()
	for h := uint64(1); h <= 6; h++ {
		m.addBlock(t, blocks[h].env)
	}
	assertAddressQueriesMatch(t, q, st, m, "concurrent")
	t.Log("concurrent ingestion verified: derived view == primary index == expected model")
}

// txHashOf returns the tx hash of tx at index i inside a built envelope.
func txHashOf(t *testing.T, env chain.Envelope, i int) model.Hash {
	t.Helper()
	dec, err := decode.Decode(env)
	if err != nil {
		t.Fatal(err)
	}
	return dec.Txs[i].TxHash
}
