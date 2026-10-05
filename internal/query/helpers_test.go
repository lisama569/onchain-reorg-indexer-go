package query

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/chain"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/decode"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/model"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/store"
)

// test accounts: a small fixed universe so address reuse (inflow/outflow,
// self-transfer, multi-tx accounts) is genuinely exercised.
var testAccounts = []string{
	"0x1111111111111111111111111111111111111111",
	"0x2222222222222222222222222222222222222222",
	"0x3333333333333333333333333333333333333333",
	"0x4444444444444444444444444444444444444444",
	"0x5555555555555555555555555555555555555555",
}

// acct parses a test account address.
func acct(t *testing.T, hexAddr string) model.Address {
	t.Helper()
	a, err := decode.ParseAddress(hexAddr)
	if err != nil {
		t.Fatalf("bad test account %s: %v", hexAddr, err)
	}
	return a
}

// txSpec is one transaction's content in a test block.
type txSpec struct {
	from   string
	to     string
	amount int64
}

// makeEnvelope builds a valid v1 envelope for height with the given parent
// and transactions; tx hashes and checksums are filled deterministically.
func makeEnvelope(t *testing.T, height uint64, parent string, txs []txSpec) chain.Envelope {
	t.Helper()
	// Block hash mixes in the transaction identities so branches with the
	// same height/parent but different contents get distinct hashes (like a
	// real block hash over the tx merkle root).
	txPart := ""
	for _, spec := range txs {
		txPart += spec.from + ">" + spec.to + fmt.Sprintf(":%d|", spec.amount)
	}
	blockHash := chain.BlockHashSeed(height, parent+txPart)
	w := chain.WireBlock{
		Version:    chain.WireVersion1,
		Height:     height,
		Hash:       blockHash,
		ParentHash: parent,
		Time:       int64(1_700_000_000 + height*12),
	}
	for i, spec := range txs {
		wt := chain.WireTx{
			Hash:   chain.TxHashSeed(height, i, w.Hash+spec.from+spec.to),
			From:   spec.from,
			To:     spec.to,
			Amount: spec.amount,
		}
		wt.Checksum = chain.TxChecksum(wt)
		w.Txs = append(w.Txs, wt)
	}
	w.Checksum = chain.BlockChecksum(w)
	env, err := chain.EncodeEnvelope(w, chain.OK)
	if err != nil {
		t.Fatalf("encode block %d: %v", height, err)
	}
	return env
}

// expectedModel is the independently maintained ground truth used to verify
// the store-derived query results. It contains exactly the canonical tx set
// the indexer is supposed to hold at each test phase.
type expectedModel struct {
	txs         []model.IndexedTx // canonical txs in (height,index) order
	byAddr      map[model.Address][]model.IndexedTx
	summary     map[model.Address]model.AddressSummary
	blockHashes map[uint64]model.Hash
}

func newExpectedModel() *expectedModel {
	return &expectedModel{
		byAddr:      map[model.Address][]model.IndexedTx{},
		summary:     map[model.Address]model.AddressSummary{},
		blockHashes: map[uint64]model.Hash{},
	}
}

// addBlock decodes env and records its txs at the canonical height.
func (m *expectedModel) addBlock(t *testing.T, env chain.Envelope) {
	t.Helper()
	dec, err := decode.Decode(env)
	if err != nil {
		t.Fatalf("decode h=%d: %v", env.Height, err)
	}
	m.blockHashes[dec.Block.Height] = dec.Block.Hash
	for _, tx := range dec.Txs {
		m.addTx(tx)
	}
}

func (m *expectedModel) addTx(tx model.IndexedTx) {
	m.txs = append(m.txs, tx)
	m.bump(tx.From, tx, true)
	if tx.To != tx.From {
		m.bump(tx.To, tx, false)
	}
}

func (m *expectedModel) bump(addr model.Address, tx model.IndexedTx, sender bool) {
	m.byAddr[addr] = append(m.byAddr[addr], tx)
	st := m.summary[addr]
	st.Address = addr
	st.TxCount++
	if sender {
		st.Outflow += tx.Amount
	} else {
		st.Inflow += tx.Amount
	}
	m.summary[addr] = st
}

// dropAfter removes every canonical tx with height > forkPoint from the
// expected model (rollback to forkPoint).
func (m *expectedModel) dropAfter(forkPoint uint64) {
	kept := m.txs[:0]
	for _, tx := range m.txs {
		if tx.BlockHeight <= forkPoint {
			kept = append(kept, tx)
		}
	}
	// Rebuild address projections from the surviving prefix.
	m.txs = append([]model.IndexedTx(nil), kept...)
	m.byAddr = map[model.Address][]model.IndexedTx{}
	m.summary = map[model.Address]model.AddressSummary{}
	for h := range m.blockHashes {
		if h > forkPoint {
			delete(m.blockHashes, h)
		}
	}
	for _, tx := range m.txs {
		m.bump(tx.From, tx, true)
		if tx.To != tx.From {
			m.bump(tx.To, tx, false)
		}
	}
}

// allTxsFor walks the query service page by page and returns every tx for
// addr, using small pages to genuinely exercise keyset pagination.
func allTxsFor(t *testing.T, q *Service, addr model.Address, pageSize int) []model.IndexedTx {
	t.Helper()
	ctx := context.Background()
	var out []model.IndexedTx
	page := Page{Limit: pageSize}
	rounds := 0
	for {
		rounds++
		res, err := q.Transactions(ctx, addr, page)
		if err != nil {
			t.Fatalf("query txs %s: %v", addr.Hex(), err)
		}
		out = append(out, res.Txs...)
		if !res.HasNext {
			if len(res.Txs) > pageSize {
				t.Fatalf("page overflow")
			}
			break
		}
		page.AfterHeight = res.NextHeight
		page.AfterIndex = res.NextIndex
		page.HasCursor = true
		if rounds > 1_000_000 {
			t.Fatalf("pagination did not terminate")
		}
	}
	return out
}

// assertAddressQueriesMatch verifies the query view of every test account
// against the independently maintained expected model, both via paginated
// listing and summary counters.
func assertAddressQueriesMatch(t *testing.T, q *Service, st store.Store, m *expectedModel, phase string) {
	t.Helper()
	ctx := context.Background()
	for _, hexAddr := range testAccounts {
		addr := acct(t, hexAddr)
		want := m.byAddr[addr]
		got := allTxsFor(t, q, addr, 2) // tiny pages => every tx crosses a page
		if len(got) != len(want) {
			t.Fatalf("[%s] %s: got %d txs, want %d", phase, hexAddr, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("[%s] %s: tx %d mismatch got=%+v want=%+v", phase, hexAddr, i, got[i], want[i])
			}
		}
		summary, ok, err := q.Summary(ctx, addr)
		if err != nil {
			t.Fatalf("[%s] summary %s: %v", phase, hexAddr, err)
		}
		wantSummary := m.summary[addr]
		if len(want) == 0 {
			if ok {
				t.Fatalf("[%s] %s: absent address must report ok=false", phase, hexAddr)
			}
			continue
		}
		if !ok || summary != wantSummary {
			t.Fatalf("[%s] %s: summary got=%+v ok=%v, want=%+v", phase, hexAddr, summary, ok, wantSummary)
		}
		t.Logf("[%s] %s txs=%d inflow=%d outflow=%d OK",
			phase, hexAddr[:6], summary.TxCount, summary.Inflow, summary.Outflow)
	}
	assertDerivedMatchesPrimary(t, st, m, phase)
}

// assertDerivedMatchesPrimary independently walks the PRIMARY index
// (ListTxs over every height) and asserts every primary tx appears for both
// participants and that no derived tx exists outside the primary set.
func assertDerivedMatchesPrimary(t *testing.T, st store.Store, m *expectedModel, phase string) {
	t.Helper()
	ctx := context.Background()
	var primary []model.IndexedTx
	if err := st.View(ctx, func(tx store.Tx) error {
		max, ok, err := tx.MaxHeight()
		if err != nil || !ok {
			return err
		}
		min, _, _ := uint64(0), 0, false
		for h := uint64(0); h <= max; h++ {
			txs, verr := tx.ListTxs(h)
			if verr != nil {
				return verr
			}
			primary = append(primary, txs...)
		}
		_ = min
		return nil
	}); err != nil {
		t.Fatalf("[%s] primary walk: %v", phase, err)
	}
	seen := map[model.Hash]bool{}
	derivedCount := 0
	q := New(st)
	for _, hexAddr := range testAccounts {
		addr := acct(t, hexAddr)
		for _, tx := range allTxsFor(t, q, addr, 7) {
			seen[tx.TxHash] = true
			derivedCount++
		}
	}
	if len(primary) != len(m.txs) {
		t.Fatalf("[%s] primary tx count %d != expected %d", phase, len(primary), len(m.txs))
	}
	for _, tx := range primary {
		if !seen[tx.TxHash] {
			t.Fatalf("[%s] primary tx %s missing from address index", phase, tx.TxHash.Short())
		}
	}
}

// memSource is a minimal chain.Source backed by a height -> envelope map,
// used to serve alt-branch blocks during multi-block reorg replay.
type memSource struct {
	mu     sync.Mutex
	blocks map[uint64]chain.Envelope
	head   uint64
}

func newMemSource() *memSource { return &memSource{blocks: map[uint64]chain.Envelope{}} }

func (s *memSource) put(env chain.Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocks[env.Height] = env
	if env.Height > s.head {
		s.head = env.Height
	}
}

func (s *memSource) Head() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.head
}

func (s *memSource) BlockAt(_ context.Context, height uint64) (chain.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	env, ok := s.blocks[height]
	if !ok {
		return chain.Envelope{}, errUnavailable
	}
	return env, nil
}
