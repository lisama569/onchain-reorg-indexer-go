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
