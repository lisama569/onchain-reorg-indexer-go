// Package store: journal.go provides bounded incremental persistence. Each
// committed transaction appends only its own operations to an append-only
// journal (O(mutations)); once the journal reaches journalCompactThreshold
// operations it is compacted into a full snapshot in the same commit, so both
// per-commit write volume and restart replay work have fixed upper bounds
// independent of total chain length.
package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/model"
)

const (
	snapshotVersion = 2
	// journalCompactMin is the first compaction interval. The interval then
	// doubles after every compaction up to journalCompactMax, so:
	//   - restart replay never has more than journalCompactMax journal ops
	//     to reapply on top of the snapshot (a fixed bound in chain length);
	//   - the number of full snapshot rewrites grows only logarithmically
	//     until the cap, after which one rewrite happens per fixed window.
	journalCompactMin = 64
	journalCompactMax = 4096
)

// journalCompactThreshold is retained as the minimum interval constant for
// tests/diagnostics.
const journalCompactThreshold = journalCompactMin

// journal operation codes.
const (
	opPutBlock    = "put_block"
	opDeleteBlock = "delete_block"
	opPutTx       = "put_tx"
	opDeleteTxs   = "delete_txs"
	opPutFailure  = "put_failure"
)

// journalEntry is one idempotent, replayable committed operation.
type journalEntry struct {
	Op      string               `json:"op"`
	Height  uint64               `json:"h,omitempty"`
	Block   *model.IndexedBlock  `json:"b,omitempty"`
	Tx      *model.IndexedTx     `json:"t,omitempty"`
	Failure *model.FailureRecord `json:"f,omitempty"`
}

func (s *MemoryStore) journalPath() string { return s.path + ".journal" }

// persistCommit durably appends the transaction's operations and, when the
// journal crosses the compaction threshold, atomically rewrites a full
// snapshot and drops the journal. Caller holds s.mu.
func (s *MemoryStore) persistCommit(entries []journalEntry) error {
	if s.path == "" || len(entries) == 0 {
		return nil
	}
	f, err := os.OpenFile(s.journalPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open journal: %w", err)
	}
	bw := bufio.NewWriter(f)
	enc := json.NewEncoder(bw)
	for i := range entries {
		if err := enc.Encode(&entries[i]); err != nil {
			f.Close()
			return fmt.Errorf("write journal: %w", err)
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return fmt.Errorf("flush journal: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync journal: %w", err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	s.journalCount += len(entries)
	if s.journalCount >= s.compactThreshold {
		return s.compactLocked()
	}
	return nil
}

// compactLocked writes a full snapshot of live state and removes the journal.
// The compaction interval doubles (up to journalCompactMax), so full snapshot
// rewrites become increasingly sparse while journal replay stays capped.
func (s *MemoryStore) compactLocked() error {
	if s.path == "" {
		return nil
	}
	if err := writeSnapshotAtomic(s.snapshotPath(), s.snapshot()); err != nil {
		return err
	}
	if err := os.Remove(s.journalPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove journal after compaction: %w", err)
	}
	s.journalCount = 0
	if s.compactThreshold < journalCompactMax {
		s.compactThreshold *= 2
		if s.compactThreshold > journalCompactMax {
			s.compactThreshold = journalCompactMax
		}
	}
	return nil
}

// replayJournal applies every journaled operation on top of the loaded
// snapshot using the same idempotent mutators used by live commits. A torn
// final line (an unterminated partial line from a crash mid-append) is
// ignored; recovery proceeds from the last fully durable boundary.
func (s *MemoryStore) replayJournal() error {
	f, err := os.Open(s.journalPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open journal for replay: %w", err)
	}
	defer f.Close()

	lines := 0
	reader := bufio.NewReader(f)
	for {
		line, rerr := reader.ReadBytes('\n')
		if len(line) > 0 {
			var e journalEntry
			if jerr := json.Unmarshal(line, &e); jerr != nil {
				if errors.Is(rerr, io.EOF) {
					// Unterminated/partial final line from a crash: stop at
					// the last durable boundary.
					break
				}
				return fmt.Errorf("corrupt journal line %d: %w", lines+1, jerr)
			}
			s.applyJournalEntry(e)
			lines++
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return fmt.Errorf("read journal: %w", rerr)
		}
	}
	s.journalCount = lines
	return nil
}

func (s *MemoryStore) applyJournalEntry(e journalEntry) {
	switch e.Op {
	case opPutBlock:
		if e.Block != nil {
			b := *e.Block
			if old, ok := s.blocks[b.Height]; ok {
				delete(s.byHash, old.Hash)
			}
			s.blocks[b.Height] = b
			s.byHash[b.Hash] = b.Height
		}
	case opDeleteBlock:
		if b, ok := s.blocks[e.Height]; ok {
			delete(s.byHash, b.Hash)
			delete(s.blocks, e.Height)
		}
	case opPutTx:
		if e.Tx != nil {
			tx := *e.Tx
			byIndex := s.txs[tx.BlockHeight]
			if byIndex == nil {
				byIndex = map[uint64]model.IndexedTx{}
				s.txs[tx.BlockHeight] = byIndex
			}
			byIndex[uint64(tx.Index)] = tx
		}
	case opDeleteTxs:
		delete(s.txs, e.Height)
	case opPutFailure:
		if e.Failure != nil {
			f := *e.Failure
			s.failures[f.Fingerprint()] = f
		}
	}
}
