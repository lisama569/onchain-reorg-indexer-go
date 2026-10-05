// Package store: memory.go is an atomic in-memory Store implementation. All
// state lives in ordered maps/sets behind a single mutex; transactions stage
// idempotent operations that become visible together at Commit. Secondary
// per-address indexes are maintained inside the same mutations as blocks/txs,
// so the query model and the main index can never diverge.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/model"
)

// MemoryStore is an in-memory Store, optionally mirrored to disk so runs are
// locally reproducible and can restart from a checkpoint. Persistence is
// incremental (an append-only operation journal, periodically compacted into
// a full snapshot): commit cost is proportional to the transaction's own
// mutations, never to the stored history.
type MemoryStore struct {
	mu       sync.Mutex
	path     string
	blocks   map[uint64]model.IndexedBlock
	byHash   map[model.Hash]uint64
	heights  *orderedSet[uint64, struct{}]
	txs      map[uint64]map[uint64]model.IndexedTx // height -> (index -> tx)
	failures map[string]model.FailureRecord

	// Derived per-address query structures, maintained in every mutation.
	addrTxs   map[model.Address]*orderedSet[ascKey, model.IndexedTx]
	addrStats map[model.Address]model.AddressSummary

	// journalCount is the number of operations in the on-disk journal since
	// the last compaction; compactThreshold is the current (growing) interval
	// that triggers a full snapshot rewrite.
	journalCount     int
	compactThreshold int
}

// NewMemoryStore creates an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return newMemory("")
}

// NewFileBackedMemoryStore loads state from path (snapshot + operation
// journal) if present and persists every committed Update transaction after.
func NewFileBackedMemoryStore(path string) (*MemoryStore, error) {
	s := newMemory(path)
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func newMemory(path string) *MemoryStore {
	return &MemoryStore{
		path:             path,
		blocks:           map[uint64]model.IndexedBlock{},
		byHash:           map[model.Hash]uint64{},
		heights:          newOrderedSet[uint64, struct{}](func(a, b uint64) bool { return a < b }),
		txs:              map[uint64]map[uint64]model.IndexedTx{},
		failures:         map[string]model.FailureRecord{},
		addrTxs:          map[model.Address]*orderedSet[ascKey, model.IndexedTx]{},
		addrStats:        map[model.Address]model.AddressSummary{},
		compactThreshold: journalCompactMin,
	}
}

// ---- transaction ----

// memTx stages idempotent store operations. The staged closures are applied to
// live state on Commit; the same operations can be replayed against a fresh
// store during restart recovery, which is why each is idempotent.
type memTx struct {
	s         *MemoryStore
	mutations []func()
	journal   []journalEntry
	committed bool
}

func (t *memTx) GetBlock(height uint64) (model.IndexedBlock, bool, error) {
	b, ok := t.s.blocks[height]
	return b, ok, nil
}

func (t *memTx) GetBlockByHash(hash model.Hash) (model.IndexedBlock, bool, error) {
	height, ok := t.s.byHash[hash]
	if !ok {
		return model.IndexedBlock{}, false, nil
	}
	return t.s.blocks[height], true, nil
}

// applyTx inserts or replaces one transaction and updates both per-address
// secondary structures. Re-inserting the same (height,index) with an equal
// payload is a no-op; a differing payload first reverses the old entry.
func (s *MemoryStore) applyTx(tx model.IndexedTx) {
	byIndex := s.txs[tx.BlockHeight]
	if old, ok := byIndex[uint64(tx.Index)]; ok {
		if old.TxHash == tx.TxHash {
			return // idempotent replay
		}
		s.unindexAddress(old)
	}
	if byIndex == nil {
		byIndex = map[uint64]model.IndexedTx{}
		s.txs[tx.BlockHeight] = byIndex
	}
	byIndex[uint64(tx.Index)] = tx
	s.indexAddress(tx)
}

// indexAddress registers one transaction in its sender's and recipient's
// ordered sets and adjusts the aggregate counters. A self-transfer counts as
// one transaction but moves the amount through both in- and outflow.
func (s *MemoryStore) indexAddress(tx model.IndexedTx) {
	key := ascKey{height: tx.BlockHeight, index: uint64(tx.Index)}
	fromSet := s.addrSet(tx.From)
	if fromSet.insert(key, tx) {
		st := s.addrStats[tx.From]
		st.Address = tx.From
		st.TxCount++
		st.Outflow += tx.Amount
		s.addrStats[tx.From] = st
	}
	if tx.To != tx.From {
		toSet := s.addrSet(tx.To)
		if toSet.insert(key, tx) {
			st := s.addrStats[tx.To]
			st.Address = tx.To
			st.TxCount++
			st.Inflow += tx.Amount
			s.addrStats[tx.To] = st
		}
	}
}

// unindexAddress reverses indexAddress for a removed/replaced transaction.
func (s *MemoryStore) unindexAddress(tx model.IndexedTx) {
	key := ascKey{height: tx.BlockHeight, index: uint64(tx.Index)}
	if set := s.addrTxs[tx.From]; set != nil && set.delete(key) {
		st := s.addrStats[tx.From]
		st.TxCount--
		st.Outflow -= tx.Amount
		if st.TxCount == 0 {
			delete(s.addrStats, tx.From)
		} else {
			s.addrStats[tx.From] = st
		}
	}
	if tx.To != tx.From {
		if set := s.addrTxs[tx.To]; set != nil && set.delete(key) {
			st := s.addrStats[tx.To]
			st.TxCount--
			st.Inflow -= tx.Amount
			if st.TxCount == 0 {
				delete(s.addrStats, tx.To)
			} else {
				s.addrStats[tx.To] = st
			}
		}
	}
}

func (s *MemoryStore) addrSet(addr model.Address) *orderedSet[ascKey, model.IndexedTx] {
	set := s.addrTxs[addr]
	if set == nil {
		set = newOrderedSet[ascKey, model.IndexedTx](lessAscKey)
		s.addrTxs[addr] = set
	}
	return set
}

func (t *memTx) PutBlock(b model.IndexedBlock) error {
	t.mutations = append(t.mutations, func() {
		if old, ok := t.s.blocks[b.Height]; ok {
			delete(t.s.byHash, old.Hash)
		}
		t.s.blocks[b.Height] = b
		t.s.byHash[b.Hash] = b.Height
		t.s.heights.insert(b.Height, struct{}{})
	})
	t.journal = append(t.journal, journalEntry{Op: opPutBlock, Block: &b})
	return nil
}

func (t *memTx) DeleteBlock(height uint64) error {
	t.mutations = append(t.mutations, func() {
		if b, ok := t.s.blocks[height]; ok {
			delete(t.s.byHash, b.Hash)
			delete(t.s.blocks, height)
			t.s.heights.delete(height)
		}
	})
	t.journal = append(t.journal, journalEntry{Op: opDeleteBlock, Height: height})
	return nil
}

func (t *memTx) PutTx(tx model.IndexedTx) error {
	t.mutations = append(t.mutations, func() { t.s.applyTx(tx) })
	t.journal = append(t.journal, journalEntry{Op: opPutTx, Tx: &tx})
	return nil
}

func (t *memTx) DeleteTxsByBlock(height uint64) error {
	t.mutations = append(t.mutations, func() {
		byIndex := t.s.txs[height]
		for _, tx := range byIndex {
			t.s.unindexAddress(tx)
		}
		delete(t.s.txs, height)
	})
	t.journal = append(t.journal, journalEntry{Op: opDeleteTxs, Height: height})
	return nil
}

func (t *memTx) ListTxs(height uint64) ([]model.IndexedTx, error) {
	byIndex := t.s.txs[height]
	out := make([]model.IndexedTx, 0, len(byIndex))
	for _, tx := range byIndex {
		out = append(out, tx)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out, nil
}

func (t *memTx) PutFailure(f model.FailureRecord) (bool, error) {
	fp := f.Fingerprint()
	if _, exists := t.s.failures[fp]; exists {
		return false, nil
	}
	t.mutations = append(t.mutations, func() { t.s.failures[fp] = f })
	t.journal = append(t.journal, journalEntry{Op: opPutFailure, Failure: &f})
	return true, nil
}

func (t *memTx) ListFailures() ([]model.FailureRecord, error) {
	out := make([]model.FailureRecord, 0, len(t.s.failures))
	for _, f := range t.s.failures {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Height != out[j].Height {
			return out[i].Height < out[j].Height
		}
		return out[i].Reason < out[j].Reason
	})
	return out, nil
}

func (t *memTx) CountFailures() (int, error) { return len(t.s.failures), nil }

func (t *memTx) GetAddressSummary(addr model.Address) (model.AddressSummary, bool, error) {
	st, ok := t.s.addrStats[addr]
	return st, ok, nil
}

func (t *memTx) ListAddressTxs(addr model.Address, afterHeight, afterIndex uint64, hasCursor bool, limit int) ([]model.IndexedTx, error) {
	set := t.s.addrTxs[addr]
	if set == nil {
		return nil, nil
	}
	cursor := ascKey{height: afterHeight, index: afterIndex}
	return set.appendAscending(nil, cursor, hasCursor, limit), nil
}

func (t *memTx) MaxHeight() (uint64, bool, error) {
	h, _, ok := t.s.heights.last()
	return h, ok, nil
}

func (t *memTx) Commit() error {
	if t.committed {
		return errors.New("transaction already committed")
	}
	if err := t.s.persistCommit(t.journal); err != nil {
		return err
	}
	for _, m := range t.mutations {
		m()
	}
	t.committed = true
	return nil
}

func (t *memTx) Rollback() { t.mutations = nil; t.journal = nil; t.committed = true }

// Update runs fn in a single atomic read-modify-write transaction. Mutations
// are only visible after Commit; a returned error rolls everything back.
func (s *MemoryStore) Update(ctx context.Context, fn func(Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	t := &memTx{s: s}
	if err := fn(t); err != nil {
		t.Rollback()
		return err
	}
	return t.Commit()
}

// View runs fn against a read-only snapshot.
func (s *MemoryStore) View(ctx context.Context, fn func(Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(&memTx{s: s})
}

// Close flushes a compacted snapshot to disk and removes the journal.
func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.compactLocked()
}

// ---- snapshot (used by journal compaction; see journal.go) ----

type snapshotJSON struct {
	Version          int                          `json:"version"`
	Blocks           []model.IndexedBlock         `json:"blocks"`
	Txs              map[uint64][]model.IndexedTx `json:"txs"`
	Failures         []model.FailureRecord        `json:"failures"`
	CompactThreshold int                          `json:"compact_threshold,omitempty"`
}

// rebuildDerived reconstructs the address secondary indexes and the height set
// from primary state. It is used after loading an on-disk snapshot: derived
// structures are intentionally not serialized (no duplication drift).
func (s *MemoryStore) rebuildDerived() {
	s.heights = newOrderedSet[uint64, struct{}](func(a, b uint64) bool { return a < b })
	s.addrTxs = map[model.Address]*orderedSet[ascKey, model.IndexedTx]{}
	s.addrStats = map[model.Address]model.AddressSummary{}
	for h := range s.blocks {
		s.heights.insert(h, struct{}{})
	}
	for _, byIndex := range s.txs {
		// Apply in index order so deterministic replay ordering is stable.
		idxs := make([]uint64, 0, len(byIndex))
		for i := range byIndex {
			idxs = append(idxs, i)
		}
		sort.Slice(idxs, func(a, b int) bool { return idxs[a] < idxs[b] })
		for _, i := range idxs {
			s.indexAddress(byIndex[i])
		}
	}
}

func (s *MemoryStore) load() error {
	if s.path == "" {
		return nil
	}
	if err := s.loadSnapshot(); err != nil {
		return err
	}
	if err := s.replayJournal(); err != nil {
		return err
	}
	s.rebuildDerived()
	return nil
}

func (s *MemoryStore) loadSnapshot() error {
	raw, err := os.ReadFile(s.snapshotPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read snapshot %s: %w", s.snapshotPath(), err)
	}
	var snap snapshotJSON
	if err := json.Unmarshal(raw, &snap); err != nil {
		return fmt.Errorf("parse snapshot %s: %w", s.snapshotPath(), err)
	}
	for _, b := range snap.Blocks {
		s.blocks[b.Height] = b
		s.byHash[b.Hash] = b.Height
	}
	for h, list := range snap.Txs {
		byIndex := map[uint64]model.IndexedTx{}
		for _, tx := range list {
			byIndex[uint64(tx.Index)] = tx
		}
		s.txs[h] = byIndex
	}
	for _, f := range snap.Failures {
		s.failures[f.Fingerprint()] = f
	}
	if snap.CompactThreshold > 0 {
		s.compactThreshold = snap.CompactThreshold
	}
	return nil
}

func (s *MemoryStore) snapshot() snapshotJSON {
	// Blocks are naturally unordered in the map; gather and sort heights.
	heights := make([]uint64, 0, len(s.blocks))
	for h := range s.blocks {
		heights = append(heights, h)
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
	blocks := make([]model.IndexedBlock, 0, len(s.blocks))
	for _, h := range heights {
		blocks = append(blocks, s.blocks[h])
	}
	txs := map[uint64][]model.IndexedTx{}
	for h, byIndex := range s.txs {
		list := make([]model.IndexedTx, 0, len(byIndex))
		for _, tx := range byIndex {
			list = append(list, tx)
		}
		sort.Slice(list, func(i, j int) bool { return list[i].Index < list[j].Index })
		txs[h] = list
	}
	fails := make([]model.FailureRecord, 0, len(s.failures))
	for _, f := range s.failures {
		fails = append(fails, f)
	}
	sort.Slice(fails, func(i, j int) bool { return fails[i].Fingerprint() < fails[j].Fingerprint() })
	threshold := s.compactThreshold
	if threshold == 0 {
		threshold = journalCompactMin
	}
	return snapshotJSON{Version: snapshotVersion, Blocks: blocks, Txs: txs, Failures: fails, CompactThreshold: threshold}
}

func (s *MemoryStore) snapshotPath() string { return s.path }

// writeSnapshotAtomically serializes state to a temp file and renames it over
// the snapshot path.
func writeSnapshotAtomic(path string, snap snapshotJSON) error {
	raw, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	dir := filepath.Dir(path)
	tmp := filepath.Join(dir, ".indexer-snapshot.tmp")
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("write snapshot temp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace snapshot: %w", err)
	}
	return nil
}
