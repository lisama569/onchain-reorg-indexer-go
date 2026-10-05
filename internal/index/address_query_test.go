package index

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/chain"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/model"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/store"
)

// testHashHex renders a deterministic 32-byte hex hash from a seed.
func testHashHex(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return "0x" + hex.EncodeToString(sum[:])
}

// hotAddress is a recurring address used to give one account deep history.
var hotAddress = func() model.Address {
	sum := sha256.Sum256([]byte("hot-address"))
	var a model.Address
	copy(a[:], sum[:20])
	return a
}()

// customEnv builds a valid v1 envelope at height whose every block contains
// two transactions involving the hot address (one out, one in), plus a
// hot-unrelated transaction. parent is the previous block's hash hex.
func customEnv(height uint64, parent string, seq int) chain.Envelope {
	mk := func(tag string, from, to string, amount int64) chain.WireTx {
		t := chain.WireTx{Hash: testHashHex(fmt.Sprintf("custom:tx:%d:%s", height, tag)), From: from, To: to, Amount: amount}
		t.Checksum = chain.TxChecksum(t)
		return t
	}
	hot := hotAddress.Hex()
	otherSum := sha256.Sum256([]byte(fmt.Sprintf("other:%d", height)))
	other := "0x" + hex.EncodeToString(otherSum[:20])
	w := chain.WireBlock{
		Version:    chain.WireVersion1,
		Height:     height,
		Hash:       testHashHex(fmt.Sprintf("custom:blk:%d:%d", height, seq)),
		ParentHash: parent,
		Time:       int64(1_700_000_000 + height*12),
		Txs: []chain.WireTx{
			mk("out", hot, other, int64(100+height)),
			mk("in", other, hot, int64(50+height)),
			mk("plain", other, other, 1),
		},
	}
	w.Checksum = chain.BlockChecksum(w)
	env, err := chain.EncodeEnvelope(w, chain.OK)
	if err != nil {
		panic(err)
	}
	return env
}

// submitCustomChain feeds n custom blocks (starting at 0) into ix and returns
// the final tip hash hex.
func submitCustomChain(t *testing.T, ix *Indexer, n int) string {
	t.Helper()
	parent := testHashHex("custom-genesis-parent")
	for h := 0; h < n; h++ {
		env := customEnv(uint64(h), parent, 0)
		res := mustSubmit(t, ix, env)
		if res.Outcome != OutcomeApplied {
			t.Fatalf("h=%d want APPLIED, got %s (%s)", h, res.Outcome, res.Detail)
		}
		parent = testHashHex(fmt.Sprintf("custom:blk:%d:%d", h, 0))
	}
	return parent
}

// recomputeAddressData rebuilds the expected address index purely from the
// canonical main index (blocks/txs in the store), giving an independent
// oracle for the derived query layer.
func recomputeAddressData(t *testing.T, ix *Indexer, start, tip uint64) (map[model.Address][]model.AddressTxRef, map[model.Address]model.AddressSummary) {
	t.Helper()
	refs := map[model.Address][]model.AddressTxRef{}
	sums := map[model.Address]model.AddressSummary{}
	err := ix.store.View(context.Background(), func(tx store.Tx) error {
		for h := start; h <= tip; h++ {
			txs, err := tx.ListTxs(h)
			if err != nil {
				return err
			}
			for _, itx := range txs {
				ref := model.AddressTxRef{Height: itx.BlockHeight, TxIndex: itx.Index, TxHash: itx.TxHash, From: itx.From, To: itx.To, Amount: itx.Amount}
				addrs := []model.Address{itx.From}
				if itx.To != itx.From { // self-transfer counts once
					addrs = append(addrs, itx.To)
				}
				for _, addr := range addrs {
					refs[addr] = append(refs[addr], ref)
					sum := sums[addr]
					sum.Address = addr
					sum.TxCount++
					if itx.From == addr {
						sum.TotalOut += itx.Amount
					}
					if itx.To == addr {
						sum.TotalIn += itx.Amount
					}
					sums[addr] = sum
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return refs, sums
}

// assertAddressIndexConsistent walks every address through the paged query
// API and the summary API and compares both against the oracle.
func assertAddressIndexConsistent(t *testing.T, ix *Indexer, refs map[model.Address][]model.AddressTxRef, sums map[model.Address]model.AddressSummary, phase string) {
	t.Helper()
	ctx := context.Background()
	checked := 0
	for addr, wantRefs := range refs {
		var got []model.AddressTxRef
		cursor := model.AddressCursor{}
		pages := 0
		for {
			page, err := ix.QueryAddressTxs(ctx, addr, cursor, 4)
			if err != nil {
				t.Fatalf("[%s] query %s: %v", phase, addr.Hex(), err)
			}
			got = append(got, page.Txs...)
			pages++
			if !page.HasMore {
				break
			}
			cursor = page.NextCursor
		}
		if len(got) != len(wantRefs) {
			t.Fatalf("[%s] addr %s: got %d refs, want %d", phase, addr.Hex(), len(got), len(wantRefs))
		}
		for i := range got {
			if got[i] != wantRefs[i] {
				t.Fatalf("[%s] addr %s ref %d: got %+v want %+v", phase, addr.Hex(), i, got[i], wantRefs[i])
			}
		}
		sum, ok, err := ix.AddressSummary(ctx, addr)
		if err != nil || !ok {
			t.Fatalf("[%s] addr %s summary: ok=%v err=%v", phase, addr.Hex(), ok, err)
		}
		if sum != sums[addr] {
			t.Fatalf("[%s] addr %s summary got %+v want %+v", phase, addr.Hex(), sum, sums[addr])
		}
		checked++
	}
	t.Logf("[%s] consistency: %d addresses match main index (paged listings + summaries)", phase, checked)
}

// TestAddressQueryPaginationAndSummary: deep history for one hot address,
// exact pagination and summary math, cross-checked against the main index.
func TestAddressQueryPaginationAndSummary(t *testing.T) {
	const blocks = 12 // 24 hot-address entries
	ix, _ := newTestIndexer(t, Config{StartHeight: 0}, nil)
	submitCustomChain(t, ix, blocks)

	ctx := context.Background()
	var walked []model.AddressTxRef
	cursor := model.AddressCursor{}
	pages := 0
	for {
		page, err := ix.QueryAddressTxs(ctx, hotAddress, cursor, 5)
		if err != nil {
			t.Fatal(err)
		}
		walked = append(walked, page.Txs...)
		pages++
		if !page.HasMore {
			break
		}
		cursor = page.NextCursor
	}
	if len(walked) != 2*blocks {
		t.Fatalf("hot address entries: got %d want %d", len(walked), 2*blocks)
	}
	for i := 1; i < len(walked); i++ {
		if walked[i].Height < walked[i-1].Height ||
			(walked[i].Height == walked[i-1].Height && walked[i].TxIndex <= walked[i-1].TxIndex) {
			t.Fatalf("entries not ordered by (height,index) at %d", i)
		}
	}
	sum, ok, err := ix.AddressSummary(ctx, hotAddress)
	if err != nil || !ok {
		t.Fatalf("hot summary: ok=%v err=%v", ok, err)
	}
	var wantOut, wantIn int64
	for h := 0; h < blocks; h++ {
		wantOut += int64(100 + h)
		wantIn += int64(50 + h)
	}
	if sum.TxCount != 2*blocks || sum.TotalOut != wantOut || sum.TotalIn != wantIn {
		t.Fatalf("hot summary got %+v want TxCount=%d Out=%d In=%d", sum, 2*blocks, wantOut, wantIn)
	}
	t.Logf("scenario=pagination pages=%d page_size=5 entries=%d ordered=true", pages, len(walked))
	t.Logf("scenario=summary addr=%s tx_count=%d total_in=%d total_out=%d", hotAddress.Hex(), sum.TxCount, sum.TotalIn, sum.TotalOut)

	// Unknown address: empty page, no summary.
	var unknown model.Address
	unknown[0] = 0xff
	page, err := ix.QueryAddressTxs(ctx, unknown, model.AddressCursor{}, 10)
	if err != nil || len(page.Txs) != 0 || page.HasMore {
		t.Fatalf("unknown address page: %+v err=%v", page, err)
	}
	if _, ok, _ := ix.AddressSummary(ctx, unknown); ok {
		t.Fatalf("unknown address must have no summary")
	}

	// Full oracle cross-check over every address (hot + all one-off others).
	refs, sums := recomputeAddressData(t, ix, 0, blocks-1)
	assertAddressIndexConsistent(t, ix, refs, sums, "basic")
}

// TestAddressQueryReorgConsistency: after rollback+replay the dead branch's
// transactions vanish from address queries and summaries, the new branch's
// appear, and the whole derived layer matches the canonical main index.
func TestAddressQueryReorgConsistency(t *testing.T) {
	ctx := context.Background()
	sim := chain.NewSimChain(0)
	sim.Extend(10) // heights 0..9 canonical
	ix, st := newTestIndexer(t, Config{StartHeight: 0, MaxReorgDepth: 16}, sim)
	if err := ix.RunTo(ctx, 9); err != nil {
		t.Fatal(err)
	}

	// Snapshot the soon-to-be-orphaned branch's txs (heights 7..9).
	oldTxHash := map[model.Hash]bool{}
	oldOnlyAddr := map[model.Address]bool{}
	_ = st.View(ctx, func(tx store.Tx) error {
		for h := uint64(7); h <= 9; h++ {
			txs, _ := tx.ListTxs(h)
			for _, itx := range txs {
				oldTxHash[itx.TxHash] = true
				oldOnlyAddr[itx.From] = true
				oldOnlyAddr[itx.To] = true
			}
		}
		return nil
	})

	// Fork at 7, extend the alt branch by 3 and reorg onto it.
	if err := sim.ForkAt("alt", 7); err != nil {
		t.Fatal(err)
	}
	sim.Extend(3)
	ix.BindSource(sim)
	res := mustSubmit(t, ix, envAt(t, sim, "alt", 7))
	tlog(t, res, "scenario=reorg fork=7")
	if res.Outcome != OutcomeReorg {
		t.Fatalf("want REORG, got %s", res.Outcome)
	}
	if err := ix.RunTo(ctx, 9); err != nil {
		t.Fatal(err)
	}

	// 1. Dead-branch txs must not appear in any dead-branch address' results.
	for addr := range oldOnlyAddr {
		page, err := ix.QueryAddressTxs(ctx, addr, model.AddressCursor{}, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, ref := range page.Txs {
			if oldTxHash[ref.TxHash] {
				t.Fatalf("rolled-back tx %s still listed for %s", ref.TxHash.Short(), addr.Hex())
			}
		}
		if _, ok, _ := ix.AddressSummary(ctx, addr); ok {
			t.Fatalf("dead-branch-only address %s still has a summary", addr.Hex())
		}
	}
	t.Logf("scenario=reorg-cleanup dead_branch_addrs=%d dead_branch_txs=%d all_absent=true", len(oldOnlyAddr), len(oldTxHash))

	// 2. New branch txs are indexed, and the entire derived layer equals the
	//    canonical main index.
	refs, sums := recomputeAddressData(t, ix, 0, 9)
	assertAddressIndexConsistent(t, ix, refs, sums, "post-reorg")
}

// TestAddressQueryRestartConsistency: a reorg persisted to the snapshot must
// survive a restart exactly — the old branch must not come back in address
// queries, and the rebuilt derived index must match the main index.
func TestAddressQueryRestartConsistency(t *testing.T) {
	ctx := context.Background()
	snap := filepath.Join(t.TempDir(), "addr-restart.json")
	sim := chain.NewSimChain(0)
	sim.Extend(8) // 0..7

	open := func() *Indexer {
		st, err := store.NewFileBackedMemoryStore(snap)
		if err != nil {
			t.Fatal(err)
		}
		return New(Config{StartHeight: 0, MaxReorgDepth: 16}, st, sim)
	}

	ix1 := open()
	if err := ix1.RunTo(ctx, 7); err != nil {
		t.Fatal(err)
	}
	// Record dead-branch-only addresses (heights 5..7 will be rolled back).
	deadAddr := map[model.Address]bool{}
	_ = ix1.store.View(ctx, func(tx store.Tx) error {
		for h := uint64(5); h <= 7; h++ {
			txs, _ := tx.ListTxs(h)
			for _, itx := range txs {
				deadAddr[itx.From] = true
				deadAddr[itx.To] = true
			}
		}
		return nil
	})
	if err := sim.ForkAt("alt", 5); err != nil {
		t.Fatal(err)
	}
	sim.Extend(3) // alt 5..7
	ix1.BindSource(sim)
	res := mustSubmit(t, ix1, envAt(t, sim, "alt", 5))
	tlog(t, res, "scenario=pre-restart-reorg fork=5")
	if err := ix1.RunTo(ctx, 7); err != nil {
		t.Fatal(err)
	}
	if err := ix1.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart from the snapshot with a fresh indexer.
	sim2 := chain.NewSimChain(0)
	_ = sim2 // canonical state irrelevant; we only query the store.
	ix2 := open()
	statLog(t, ix2, "after restart")

	for addr := range deadAddr {
		page, err := ix2.QueryAddressTxs(ctx, addr, model.AddressCursor{}, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Txs) != 0 {
			t.Fatalf("dead-branch address %s resurrected after restart: %+v", addr.Hex(), page.Txs[0])
		}
		if _, ok, _ := ix2.AddressSummary(ctx, addr); ok {
			t.Fatalf("dead-branch address %s summary resurrected after restart", addr.Hex())
		}
	}
	t.Logf("scenario=restart-no-resurrection dead_branch_addrs=%d all_absent=true", len(deadAddr))

	refs, sums := recomputeAddressData(t, ix2, 0, 7)
	assertAddressIndexConsistent(t, ix2, refs, sums, "post-restart")
	if err := ix2.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestAddressQueryConcurrentDuplicates: shuffled, duplicated, concurrent
// submissions must leave the derived address index exactly equal to the
// canonical main index — not one entry more or less.
func TestAddressQueryConcurrentDuplicates(t *testing.T) {
	ctx := context.Background()
	const total = 40
	const workers = 6
	sim := chain.NewSimChain(0)
	sim.Extend(total)
	ix, _ := newTestIndexer(t, Config{StartHeight: 0, MaxReorgDepth: 64}, sim)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for round := 0; round < 2; round++ { // every block delivered >= 2x
				idx := perm(w*17+round, total, 0)
				for _, h := range idx {
					env, err := sim.BlockAt(ctx, h)
					if err != nil {
						t.Errorf("worker %d: %v", w, err)
						return
					}
					res, err := ix.Submit(ctx, env)
					if err != nil {
						t.Errorf("worker %d submit h=%d: %v", w, h, err)
						return
					}
					if res.Outcome == OutcomeRejected {
						t.Errorf("worker %d unexpected rejection h=%d: %s", w, h, res.Reason)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()

	stats := statLog(t, ix, "after concurrent duplicate ingestion")
	if stats.TipHeight != total-1 || stats.Buffered != 0 {
		t.Fatalf("bad final state: %+v", stats)
	}
	refs, sums := recomputeAddressData(t, ix, 0, total-1)
	assertAddressIndexConsistent(t, ix, refs, sums, "concurrent-duplicates")
}

// BenchmarkAddressQueryAtScale measures a first-page query (limit 50) and a
// summary lookup for the hot address at increasing history depths. Both must
// stay flat: O(log n + page size) and O(1) respectively.
func BenchmarkAddressQueryAtScale(b *testing.B) {
	ctx := context.Background()
	for _, total := range []int{256, 2048, 8192} {
		b.Run(fmt.Sprintf("history=%d", total), func(b *testing.B) {
			ix, _ := newTestIndexer(&testing.T{}, Config{StartHeight: 0}, nil)
			parent := testHashHex("custom-genesis-parent")
			for h := 0; h < total; h++ {
				env := customEnv(uint64(h), parent, 0)
				if _, err := ix.Submit(ctx, env); err != nil {
					b.Fatal(err)
				}
				parent = testHashHex(fmt.Sprintf("custom:blk:%d:%d", h, 0))
			}
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if _, err := ix.QueryAddressTxs(ctx, hotAddress, model.AddressCursor{}, 50); err != nil {
					b.Fatal(err)
				}
				if _, _, err := ix.AddressSummary(ctx, hotAddress); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
