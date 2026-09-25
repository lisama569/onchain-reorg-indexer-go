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

// MemoryStore is an in-memory Store, optionally mirrored to a JSON snapshot
// file so runs are locally reproducible and can restart from a checkpoint.
type MemoryStore struct {
	mu       sync.Mutex
	path     string
	blocks   map[uint64]model.IndexedBlock
	byHash   map[model.Hash]uint64
	txs      map[uint64][]model.IndexedTx
	failures map[string]model.FailureRecord
}

// NewMemoryStore creates an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return newMemory("")
}

// NewFileBackedMemoryStore loads state from path if present and atomically
// re-saves the snapshot after every committed Update transaction.
func NewFileBackedMemoryStore(path string) (*MemoryStore, error) {
	s := newMemory(path)
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func newMemory(path string) *MemoryStore {
	return &MemoryStore{
		path:     path,
		blocks:   map[uint64]model.IndexedBlock{},
		byHash:   map[model.Hash]uint64{},
		txs:      map[uint64][]model.IndexedTx{},
		failures: map[string]model.FailureRecord{},
	}
}

// ---- transaction ----

type memTx struct {
	s         *MemoryStore
	mutations []func()
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

func (t *memTx) PutBlock(b model.IndexedBlock) error {
	t.mutations = append(t.mutations, func() {
		if old, ok := t.s.blocks[b.Height]; ok {
			delete(t.s.byHash, old.Hash)
		}
		t.s.blocks[b.Height] = b
		t.s.byHash[b.Hash] = b.Height
	})
	return nil
}

func (t *memTx) DeleteBlock(height uint64) error {
	t.mutations = append(t.mutations, func() {
		if b, ok := t.s.blocks[height]; ok {
			delete(t.s.byHash, b.Hash)
			delete(t.s.blocks, height)
		}
	})
	return nil
}

func (t *memTx) PutTx(tx model.IndexedTx) error {
	t.mutations = append(t.mutations, func() {
		t.s.txs[tx.BlockHeight] = append(t.s.txs[tx.BlockHeight], tx)
	})
	return nil
}

func (t *memTx) DeleteTxsByBlock(height uint64) error {
	t.mutations = append(t.mutations, func() {
		delete(t.s.txs, height)
	})
	return nil
}

func (t *memTx) ListTxs(height uint64) ([]model.IndexedTx, error) {
	out := append([]model.IndexedTx(nil), t.s.txs[height]...)
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out, nil
}

func (t *memTx) PutFailure(f model.FailureRecord) (bool, error) {
	fp := f.Fingerprint()
	if _, exists := t.s.failures[fp]; exists {
		return false, nil
	}
	t.mutations = append(t.mutations, func() { t.s.failures[fp] = f })
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

func (t *memTx) MaxHeight() (uint64, bool, error) {
	if len(t.s.blocks) == 0 {
		return 0, false, nil
	}
	max := uint64(0)
	for h := range t.s.blocks {
		if h > max {
			max = h
		}
	}
	return max, true, nil
}

func (t *memTx) Commit() error {
	if t.committed {
		return errors.New("transaction already committed")
	}
	for _, m := range t.mutations {
		m()
	}
	t.committed = true
	return t.s.save()
}

func (t *memTx) Rollback() { t.mutations = nil; t.committed = true }

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

// Close flushes the snapshot to disk.
func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked()
}

// ---- snapshot ----

type snapshotJSON struct {
	Version  int                          `json:"version"`
	Blocks   []model.IndexedBlock         `json:"blocks"`
	Txs      map[uint64][]model.IndexedTx `json:"txs"`
	Failures []model.FailureRecord        `json:"failures"`
}

func (s *MemoryStore) load() error {
	if s.path == "" {
		return nil
	}
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read snapshot %s: %w", s.path, err)
	}
	var snap snapshotJSON
	if err := json.Unmarshal(raw, &snap); err != nil {
		return fmt.Errorf("parse snapshot %s: %w", s.path, err)
	}
	for _, b := range snap.Blocks {
		s.blocks[b.Height] = b
		s.byHash[b.Hash] = b.Height
	}
	for h, list := range snap.Txs {
		s.txs[h] = list
	}
	for _, f := range snap.Failures {
		s.failures[f.Fingerprint()] = f
	}
	return nil
}

func (s *MemoryStore) snapshot() snapshotJSON {
	heights := make([]uint64, 0, len(s.blocks))
	for h := range s.blocks {
		heights = append(heights, h)
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
	blocks := make([]model.IndexedBlock, 0, len(heights))
	for _, h := range heights {
		blocks = append(blocks, s.blocks[h])
	}
	txs := map[uint64][]model.IndexedTx{}
	for h, list := range s.txs {
		cp := append([]model.IndexedTx(nil), list...)
		sort.Slice(cp, func(i, j int) bool { return cp[i].Index < cp[j].Index })
		txs[h] = cp
	}
	fails := make([]model.FailureRecord, 0, len(s.failures))
	for _, f := range s.failures {
		fails = append(fails, f)
	}
	sort.Slice(fails, func(i, j int) bool { return fails[i].Fingerprint() < fails[j].Fingerprint() })
	return snapshotJSON{Version: 1, Blocks: blocks, Txs: txs, Failures: fails}
}

func (s *MemoryStore) save() error { return s.saveLocked() }

func (s *MemoryStore) saveLocked() error {
	if s.path == "" {
		return nil
	}
	raw, err := json.MarshalIndent(s.snapshot(), "", "  ")
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	dir := filepath.Dir(s.path)
	tmp := filepath.Join(dir, ".indexer-snapshot.tmp")
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("write snapshot temp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("replace snapshot: %w", err)
	}
	return nil
}
