// Package query exposes the consumer-facing read model derived from the main
// block/tx index: per-address transaction listing with keyset pagination and
// per-address aggregate summaries. Every answer is read from secondary
// structures maintained inside the same store transactions as the primary
// index, so they can never disagree with it, including across reorg replay and
// restart.
package query

import (
	"context"
	"fmt"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/decode"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/model"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/store"
)

// DefaultPageSize is used when Page.Limit is zero.
const DefaultPageSize = 100

// MaxPageSize bounds the work and result size of a single page request.
const MaxPageSize = 1000

// Page selects one ascending page strictly after the supplied position.
type Page struct {
	// AfterHeight / AfterIndex is the exclusive keyset cursor; HasCursor=false
	// (the zero Page) starts from the address's first transaction.
	AfterHeight uint64
	AfterIndex  uint64
	HasCursor   bool
	Limit       int
}

// TxPage is one page of an address's transactions.
type TxPage struct {
	Txs []model.IndexedTx
	// NextHeight / NextIndex form the cursor for the following page; HasNext
	// reports whether such a page exists.
	NextHeight uint64
	NextIndex  uint64
	HasNext    bool
}

// Service answers address-centric queries against a Store.
type Service struct {
	store store.Store
}

// New creates a query Service.
func New(st store.Store) *Service { return &Service{store: st} }

// Transactions returns the indexed transactions involving addr (as sender or
// recipient), ordered by (block height, in-block index) ascending. Cost is
// O(limit log n) map/tree reads plus one extra probe; it never scans the
// address's full history.
func (q *Service) Transactions(ctx context.Context, addr model.Address, p Page) (TxPage, error) {
	limit := p.Limit
	switch {
	case limit == 0:
		limit = DefaultPageSize
	case limit < 0:
		return TxPage{}, fmt.Errorf("page limit must be non-negative, got %d", limit)
	case limit > MaxPageSize:
		limit = MaxPageSize
	}
	var page TxPage
	err := q.store.View(ctx, func(tx store.Tx) error {
		// Fetch one extra row to decide whether another page exists, keeping
		// both the returned set and the internal work bounded.
		rows, verr := tx.ListAddressTxs(addr, p.AfterHeight, p.AfterIndex, p.HasCursor, limit+1)
		if verr != nil {
			return verr
		}
		if len(rows) > limit {
			page.HasNext = true
			rows = rows[:limit]
		}
		page.Txs = rows
		if len(rows) > 0 {
			last := rows[len(rows)-1]
			page.NextHeight = last.BlockHeight
			page.NextIndex = uint64(last.Index)
		}
		return nil
	})
	if err != nil {
		return TxPage{}, err
	}
	return page, nil
}

// TransactionsHex is the hex-address convenience wrapper around Transactions.
func (q *Service) TransactionsHex(ctx context.Context, hexAddr string, p Page) (TxPage, error) {
	addr, err := decode.ParseAddress(hexAddr)
	if err != nil {
		return TxPage{}, err
	}
	return q.Transactions(ctx, addr, p)
}

// Summary returns the aggregate counters for an address. The boolean is false
// for addresses that never participated in any indexed transaction; the read
// is O(1) (the counters are maintained transactionally with the main index).
func (q *Service) Summary(ctx context.Context, addr model.Address) (model.AddressSummary, bool, error) {
	var (
		summary model.AddressSummary
		ok      bool
	)
	err := q.store.View(ctx, func(tx store.Tx) error {
		var verr error
		summary, ok, verr = tx.GetAddressSummary(addr)
		return verr
	})
	if err != nil {
		return model.AddressSummary{}, false, err
	}
	return summary, ok, nil
}

// SummaryHex is the hex-address convenience wrapper around Summary.
func (q *Service) SummaryHex(ctx context.Context, hexAddr string) (model.AddressSummary, bool, error) {
	addr, err := decode.ParseAddress(hexAddr)
	if err != nil {
		return model.AddressSummary{}, false, err
	}
	return q.Summary(ctx, addr)
}
