// Package store contains storage drivers for the normalized index data.
package store

import (
	"context"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/model"
)

// Tx is an atomic read-modify-write transaction over the index state.
type Tx interface {
	GetBlock(height uint64) (model.IndexedBlock, bool, error)
	GetBlockByHash(hash model.Hash) (model.IndexedBlock, bool, error)
	PutBlock(b model.IndexedBlock) error
	DeleteBlock(height uint64) error
	PutTx(tx model.IndexedTx) error
	DeleteTxsByBlock(height uint64) error
	ListTxs(height uint64) ([]model.IndexedTx, error)
	// PutAddressTx records one derived address-index entry. It is idempotent
	// per (Address implied by the caller, Height, TxIndex): re-applying the
	// same entry is a no-op so replays never double-count.
	PutAddressTx(addr model.Address, ref model.AddressTxRef) error
	// DeleteAddressTxsByBlock removes every derived entry at height and
	// adjusts the affected summaries. Cost is bounded by the number of
	// entries at that height, not by total history.
	DeleteAddressTxsByBlock(height uint64) error
	// ListAddressTxs returns up to limit entries for addr strictly after
	// cursor, ordered by (Height, TxIndex). The returned cursor resumes the
	// next page; hasMore reports whether more entries exist.
	ListAddressTxs(addr model.Address, cursor model.AddressCursor, limit int) (refs []model.AddressTxRef, next model.AddressCursor, hasMore bool, err error)
	// GetAddressSummary returns the maintained aggregate for addr in O(1).
	GetAddressSummary(addr model.Address) (model.AddressSummary, bool, error)
	PutFailure(f model.FailureRecord) (bool, error)
	ListFailures() ([]model.FailureRecord, error)
	MaxHeight() (uint64, bool, error)
	// Commit atomically flushes all mutations performed in the transaction.
	Commit() error
	Rollback()
}

// Store is the persistence driver.
type Store interface {
	Update(ctx context.Context, fn func(Tx) error) error
	View(ctx context.Context, fn func(Tx) error) error
	Close() error
}
