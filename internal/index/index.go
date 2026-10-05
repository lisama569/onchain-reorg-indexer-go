// Package index implements incremental ingestion, reorg handling and
// idempotent checkpointed continuation on top of a Store and a chain.Source.
package index

import (
	"context"
	"fmt"
	"sync"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/chain"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/decode"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/model"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/store"
)

// defaultMaxReorgDepth is used when Config.MaxReorgDepth is zero.
const defaultMaxReorgDepth = 64

// Config configures an Indexer.
type Config struct {
	// StartHeight is the first height the indexer accepts.
	StartHeight uint64
	// MaxReorgDepth bounds how deep a fork may diverge from the indexed tip
	// before it is rejected (finality window). 0 => defaultMaxReorgDepth.
	MaxReorgDepth uint64
	// SnapshotEvery is the checkpoint logging stride (0 disables progress
	// logging). The store snapshot itself is flushed on every commit.
	SnapshotEvery int
}

// Outcome classifies how a submitted envelope was handled.
type Outcome string

const (
	OutcomeApplied   Outcome = "APPLIED"
	OutcomeReorg     Outcome = "REORG"
	OutcomeDuplicate Outcome = "DUPLICATE"
	OutcomeBuffered  Outcome = "BUFFERED"
	OutcomeRejected  Outcome = "REJECTED"
)

// Result is the result of submitting one envelope. Detail always carries the
// decision basis (heights, hashes, ancestor/rollback information).
type Result struct {
	Height  uint64
	Hash    model.Hash
	Outcome Outcome
	// Reason is set to a model.Reason* code when Outcome == REJECTED.
	Reason string
	Detail string
	// Tip is the indexed tip height after the operation.
	Tip uint64
}

// Stats summarizes indexer state and doubles as the persisted checkpoint
// view: NextHeight/TipHash are derived from stored blocks, so the checkpoint
// can never disagree with block state.
type Stats struct {
	NextHeight uint64
	TipHash    model.Hash
	TipHeight  uint64
	Buffered   int
	Blocks     int
	Failures   int
}

// Indexer incrementally indexes a chain source into a store.
type Indexer struct {
	cfg    Config
	depth  uint64
	store  store.Store
	mu     sync.Mutex
	src    chain.Source
	buffer map[uint64]*decoded
}

type decoded struct {
	block model.IndexedBlock
	txs   []model.IndexedTx
}

// New builds an Indexer. src may be nil and supplied later via BindSource.
func New(cfg Config, st store.Store, src chain.Source) *Indexer {
	depth := cfg.MaxReorgDepth
	if depth == 0 {
		depth = defaultMaxReorgDepth
	}
	return &Indexer{cfg: cfg, depth: depth, store: st, src: src, buffer: map[uint64]*decoded{}}
}

// BindSource attaches (or replaces) the chain data source. It is safe to
// switch a source whose canonical chain matches the indexed tip (used after
// a simulated reorg activates a new branch).
func (ix *Indexer) BindSource(src chain.Source) {
	ix.mu.Lock()
	ix.src = src
	ix.mu.Unlock()
}

// Submit ingests one envelope. It is safe for concurrent use and is
// idempotent: duplicate, reordered or checkpoint-regressing submissions are
// never lost or double-applied. A rejected envelope is returned with
// Outcome=REJECTED and a stable Reason; it is also persisted as a
// FailureRecord. Submit itself returns a non-nil error only for
// infrastructure failures (storage/source).
func (ix *Indexer) Submit(ctx context.Context, env chain.Envelope) (Result, error) {
	dec, derr := decode.Decode(env)
	if derr != nil {
		return ix.recordDecodeFailure(ctx, env, derr)
	}
	d := &decoded{block: dec.Block, txs: dec.Txs}

	ix.mu.Lock()
	defer ix.mu.Unlock()
	res, err := ix.applyLocked(ctx, d)
	if err != nil {
		return Result{}, err
	}
	// Any submission can close a gap (or a reorg may invalidate buffered
	// heights), so always try to connect the buffered frontier.
	drained, err := ix.drainLocked(ctx)
	if err != nil {
		return Result{}, err
	}
	if drained > 0 {
		res.Tip = ix.tipHeight(ctx)
		if res.Outcome == OutcomeApplied || res.Outcome == OutcomeReorg {
			res.Detail = fmt.Sprintf("%s; drained %d buffered block(s), new tip %d", res.Detail, drained, res.Tip)
		}
	}
	return res, nil
}

func (ix *Indexer) recordDecodeFailure(ctx context.Context, env chain.Envelope, derr error) (Result, error) {
	de := derr.(*decode.DecodeError)
	rec := model.FailureRecord{
		Height: env.Height,
		Stage:  "decode",
		Reason: de.Reason,
		Detail: de.Detail,
		Raw:    string(env.Payload),
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	added := false
	err := ix.store.Update(ctx, func(tx store.Tx) error {
		var perr error
		added, perr = tx.PutFailure(rec)
		return perr
	})
	if err != nil {
		return Result{}, err
	}
	basis := de.Detail
	if !added {
		basis += " (identical failure already recorded)"
	}
	return Result{
		Height:  env.Height,
		Outcome: OutcomeRejected,
		Reason:  de.Reason,
		Detail:  basis,
		Tip:     ix.tipHeight(ctx),
	}, nil
}

// applyLocked classifies and applies one valid decoded block. Caller holds
// ix.mu; store transactions are used inside for atomicity.
func (ix *Indexer) applyLocked(ctx context.Context, d *decoded) (Result, error) {
	h := d.block.Height
	tip, has, err := ix.tip(ctx)
	if err != nil {
		return Result{}, err
	}

	// Below the configured start height: always rejected, never mutates.
	if h < ix.cfg.StartHeight {
		return ix.rejectLink(ctx, d, model.ReasonBeforeStart,
			fmt.Sprintf("height %d is below configured start %d", h, ix.cfg.StartHeight))
	}

	expected := ix.cfg.StartHeight
	if has {
		expected = tip.Height + 1
	}

	switch {
	case has && h <= tip.Height:
		// Idempotence / checkpoint-regression path.
		existing, ok, err := ix.getBlock(ctx, h)
		if err != nil {
			return Result{}, err
		}
		if ok && existing.Hash == d.block.Hash {
			return Result{Height: h, Hash: d.block.Hash, Outcome: OutcomeDuplicate,
				Detail: fmt.Sprintf("height %d already indexed with same hash %s; checkpoint not moved", h, d.block.Hash.Short()),
				Tip:    tip.Height}, nil
		}
		// Same height, different hash: a late fork landing inside history.
		return ix.reorgLocked(ctx, d, tip.Height)
	case h > expected:
		// Out-of-order / gap: buffer until the predecessor is indexed.
		ix.buffer[h] = d
		var bufferedTip uint64
		if has {
			bufferedTip = tip.Height
		} else {
			bufferedTip = ix.cfg.StartHeight - 1
		}
		return Result{Height: h, Hash: d.block.Hash, Outcome: OutcomeBuffered,
			Detail: fmt.Sprintf("height %d is ahead of expected %d; buffered", h, expected),
			Tip:    bufferedTip}, nil
	default:
		// h == expected.
		if !has {
			// Genesis of the indexed range.
			if err := ix.putBlock(ctx, d); err != nil {
				return Result{}, err
			}
			return Result{Height: h, Hash: d.block.Hash, Outcome: OutcomeApplied,
				Detail: fmt.Sprintf("genesis block at start height %d accepted", h), Tip: h}, nil
		}
		if d.block.ParentHash == tip.Hash {
			if err := ix.putBlock(ctx, d); err != nil {
				return Result{}, err
			}
			return Result{Height: h, Hash: d.block.Hash, Outcome: OutcomeApplied,
				Detail: fmt.Sprintf("parent hash %s matches tip %d", d.block.ParentHash.Short(), tip.Height), Tip: h}, nil
		}
		return ix.reorgLocked(ctx, d, tip.Height)
	}
}

// reorgLocked locates the common ancestor of the incoming fork and the
// indexed chain, rejects forks deeper than the finality window, then
// atomically rolls back and replays in one transaction.
func (ix *Indexer) reorgLocked(ctx context.Context, d *decoded, tipHeight uint64) (Result, error) {
	h := d.block.Height
	// Walk the fork backwards via the source until a block hash matches the
	// indexed chain. The submitted block itself is the first known alt block.
	type alt struct {
		height uint64
		hash   model.Hash
	}
	altByHeight := map[uint64]model.Hash{h: d.block.Hash}
	parent := d.block.ParentHash
	var ancestor uint64
	found := false

	cur := h - 1
	rollsBack := 0
	if h > tipHeight {
		rollsBack = 0
	} else {
		rollsBack = int(tipHeight - h + 1)
	}
	for cur >= ix.cfg.StartHeight {
		if rollsBack > int(ix.depth) {
			return ix.rejectLink(ctx, d, model.ReasonReorgTooDeep,
				fmt.Sprintf("fork search exceeded max reorg depth %d while looking for ancestor (below %d)",
					ix.depth, tipHeight-ix.depth))
		}
		stored, ok, err := ix.getBlock(ctx, cur)
		if err != nil {
			return Result{}, err
		}
		if ok && stored.Hash == parent {
			ancestor = cur
			found = true
			break
		}
		// Need the alt block at cur to continue walking back.
		altBlock, err := ix.fetchAlt(ctx, cur, altByHeight)
		if err != nil {
			return Result{}, err
		}
		altByHeight[cur] = altBlock.block.Hash
		parent = altBlock.block.ParentHash
		rollsBack++
		cur--
	}
	if !found {
		return ix.rejectLink(ctx, d, model.ReasonReorgTooDeep,
			fmt.Sprintf("no common ancestor within retained window [start=%d, depth=%d, tip=%d]",
				ix.cfg.StartHeight, ix.depth, tipHeight))
	}
	rollbackCount := 0
	if tipHeight > ancestor {
		rollbackCount = int(tipHeight - ancestor)
	}
	if uint64(rollbackCount) > ix.depth {
		return ix.rejectLink(ctx, d, model.ReasonReorgTooDeep,
			fmt.Sprintf("rollback depth %d exceeds max reorg depth %d (finalized height %d)",
				rollbackCount, ix.depth, tipHeight-ix.depth))
	}

	// Gather the alt blocks that must be replayed: ancestor+1 .. h.
	replay := make([]*decoded, 0, h-ancestor)
	for rh := ancestor + 1; rh <= h; rh++ {
		if rh == h {
			replay = append(replay, d)
			continue
		}
		env, err := ix.source().BlockAt(ctx, rh)
		if err != nil {
			return Result{}, fmt.Errorf("reorg replay: fetch alt block %d: %w", rh, err)
		}
		dec, derr := decode.Decode(env)
		if derr != nil {
			return Result{}, fmt.Errorf("reorg replay: alt block %d undecodable: %w", rh, derr)
		}
		replay = append(replay, &decoded{block: dec.Block, txs: dec.Txs})
	}

	// Atomic rollback + replay.
	err := ix.store.Update(ctx, func(tx store.Tx) error {
		for rh := tipHeight; rh > ancestor && rh >= ix.cfg.StartHeight; rh-- {
			if err := tx.DeleteAddressTxsByBlock(rh); err != nil {
				return err
			}
			if err := tx.DeleteTxsByBlock(rh); err != nil {
				return err
			}
			if err := tx.DeleteBlock(rh); err != nil {
				return err
			}
		}
		for _, rd := range replay {
			if err := tx.PutBlock(rd.block); err != nil {
				return err
			}
			for _, t := range rd.txs {
				if err := putTxWithAddressRefs(tx, t); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	// Discard buffered blocks invalidated by the branch switch.
	for bh := range ix.buffer {
		if bh <= tipHeight {
			delete(ix.buffer, bh)
		}
	}
	return Result{Height: h, Hash: d.block.Hash, Outcome: OutcomeReorg,
		Detail: fmt.Sprintf("fork at %d: common ancestor %d (%s), rolled back %d block(s) [%d..%d], replayed %d alt block(s)",
			h, ancestor, parent.Short(), rollbackCount, ancestor+1, tipHeight, len(replay)),
		Tip: h}, nil
}

// fetchAlt returns a decoded alt-branch block at height, preferring already
// known blocks (the submitted block and blocks fetched during the ancestor
// walk). A bound source is required for heights not yet known.
func (ix *Indexer) fetchAlt(ctx context.Context, height uint64, known map[uint64]model.Hash) (*decoded, error) {
	if _, ok := known[height]; ok {
		// Hash known but full record not; must still fetch the payload.
	}
	if ix.src == nil {
		return nil, fmt.Errorf("reorg deeper than one block requires a bound chain.Source")
	}
	env, err := ix.src.BlockAt(ctx, height)
	if err != nil {
		return nil, fmt.Errorf("fetch alt block %d: %w", height, err)
	}
	dec, derr := decode.Decode(env)
	if derr != nil {
		return nil, fmt.Errorf("alt block %d undecodable: %w", height, derr)
	}
	return &decoded{block: dec.Block, txs: dec.Txs}, nil
}

// drainLocked connects buffered blocks in height order starting from the
// current expected height. Buffered blocks are applied directly: by
// construction the previous height is already committed, so they never need
// the buffering branch of applyLocked. A buffered fork block (parent mismatch)
// is routed through applyLocked so normal reorg handling still applies.
func (ix *Indexer) drainLocked(ctx context.Context) (int, error) {
	consumed := 0
	for {
		tip, has, err := ix.tip(ctx)
		if err != nil {
			return consumed, err
		}
		next := ix.cfg.StartHeight
		var wantParent model.Hash
		if has {
			next = tip.Height + 1
			wantParent = tip.Hash
		}
		d, ok := ix.buffer[next]
		if !ok {
			return consumed, nil
		}
		delete(ix.buffer, next)
		if has && d.block.ParentHash != wantParent {
			// Late fork among previously buffered heights.
			if _, err := ix.reorgLocked(ctx, d, tip.Height); err != nil {
				return consumed, err
			}
			consumed++
			continue
		}
		if err := ix.putBlock(ctx, d); err != nil {
			return consumed, err
		}
		consumed++
	}
}

func (ix *Indexer) rejectLink(ctx context.Context, d *decoded, reason, detail string) (Result, error) {
	rec := model.FailureRecord{
		Height: d.block.Height,
		Hash:   d.block.Hash.Hex(),
		Stage:  "link",
		Reason: reason,
		Detail: detail,
	}
	err := ix.store.Update(ctx, func(tx store.Tx) error {
		_, perr := tx.PutFailure(rec)
		return perr
	})
	if err != nil {
		return Result{}, err
	}
	return Result{Height: d.block.Height, Hash: d.block.Hash, Outcome: OutcomeRejected,
		Reason: reason, Detail: detail, Tip: ix.tipHeight(ctx)}, nil
}

// putTxWithAddressRefs writes one canonical tx plus its derived address
// entries inside the same store transaction, so the two can never diverge.
func putTxWithAddressRefs(tx store.Tx, t model.IndexedTx) error {
	if err := tx.PutTx(t); err != nil {
		return err
	}
	ref := model.AddressTxRef{
		Height:  t.BlockHeight,
		TxIndex: t.Index,
		TxHash:  t.TxHash,
		From:    t.From,
		To:      t.To,
		Amount:  t.Amount,
	}
	if err := tx.PutAddressTx(t.From, ref); err != nil {
		return err
	}
	if t.To != t.From {
		if err := tx.PutAddressTx(t.To, ref); err != nil {
			return err
		}
	}
	return nil
}

func (ix *Indexer) putBlock(ctx context.Context, d *decoded) error {
	return ix.store.Update(ctx, func(tx store.Tx) error {
		if err := tx.PutBlock(d.block); err != nil {
			return err
		}
		for _, t := range d.txs {
			if err := putTxWithAddressRefs(tx, t); err != nil {
				return err
			}
		}
		return nil
	})
}

func (ix *Indexer) getBlock(ctx context.Context, h uint64) (model.IndexedBlock, bool, error) {
	var b model.IndexedBlock
	var ok bool
	err := ix.store.View(ctx, func(tx store.Tx) error {
		var verr error
		b, ok, verr = tx.GetBlock(h)
		return verr
	})
	return b, ok, err
}

func (ix *Indexer) tip(ctx context.Context) (model.IndexedBlock, bool, error) {
	var b model.IndexedBlock
	var has bool
	err := ix.store.View(ctx, func(tx store.Tx) error {
		h, ok, verr := tx.MaxHeight()
		if verr != nil || !ok {
			has = ok
			return verr
		}
		b, has, verr = tx.GetBlock(h)
		return verr
	})
	return b, has, err
}

func (ix *Indexer) tipHeight(ctx context.Context) uint64 {
	b, has, _ := ix.tip(ctx)
	if !has {
		if ix.cfg.StartHeight > 0 {
			return ix.cfg.StartHeight - 1
		}
		return 0
	}
	return b.Height
}

func (ix *Indexer) source() chain.Source { return ix.src }

// RunTo drives ingestion from the source from the current checkpoint up to
// min(targetHeight, source head). A permanently malformed block stops
// advancement and is returned as an error carrying the reason.
func (ix *Indexer) RunTo(ctx context.Context, targetHeight uint64) error {
	if ix.src == nil {
		return fmt.Errorf("RunTo requires a bound chain.Source")
	}
	for {
		head := ix.src.Head()
		stats, err := ix.Stats(ctx)
		if err != nil {
			return err
		}
		h := stats.NextHeight
		if h > targetHeight || h > head {
			return nil
		}
		env, err := ix.src.BlockAt(ctx, h)
		if err != nil {
			return fmt.Errorf("fetch block %d: %w", h, err)
		}
		res, err := ix.Submit(ctx, env)
		if err != nil {
			return err
		}
		if res.Outcome == OutcomeRejected {
			return &ProgressError{Height: h, Reason: res.Reason, Detail: res.Detail}
		}
		if ix.cfg.SnapshotEvery > 0 && h%uint64(ix.cfg.SnapshotEvery) == 0 {
			fmt.Printf("[checkpoint] height=%d tip=%s outcome=%s basis=%s\n",
				h, res.Hash.Short(), res.Outcome, res.Detail)
		}
	}
}

// ProgressError is returned by RunTo when a block at the next height is
// rejected; ingestion halts at that checkpoint.
type ProgressError struct {
	Height uint64
	Reason string
	Detail string
}

func (e *ProgressError) Error() string {
	return fmt.Sprintf("ingestion halted at height %d: %s (%s)", e.Height, e.Reason, e.Detail)
}

// Stats returns a snapshot of indexer state (checkpoint view).
func (ix *Indexer) Stats(ctx context.Context) (Stats, error) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	var st Stats
	err := ix.store.View(ctx, func(tx store.Tx) error {
		if h, ok, verr := tx.MaxHeight(); verr != nil {
			return verr
		} else if ok {
			b, _, verr := tx.GetBlock(h)
			if verr != nil {
				return verr
			}
			st.TipHeight = h
			st.TipHash = b.Hash
			st.NextHeight = h + 1
		} else {
			st.NextHeight = ix.cfg.StartHeight
		}
		fails, verr := tx.ListFailures()
		if verr != nil {
			return verr
		}
		st.Failures = len(fails)
		return nil
	})
	if err != nil {
		return Stats{}, err
	}
	st.Buffered = len(ix.buffer)
	st.Blocks = 0
	if st.NextHeight > ix.cfg.StartHeight {
		st.Blocks = int(st.NextHeight - ix.cfg.StartHeight)
	}
	return st, nil
}

// AddressTxPage is one page of the derived address-transaction query.
type AddressTxPage struct {
	Txs []model.AddressTxRef
	// NextCursor resumes the listing; pass it back to QueryAddressTxs.
	NextCursor model.AddressCursor
	HasMore    bool
}

// QueryAddressTxs returns the transactions involving addr (as sender or
// receiver), ordered by (block height, in-block index), paginated by cursor.
// limit <= 0 returns the remaining entries. The result reflects exactly the
// canonical indexed chain: rolled-back branches never appear.
func (ix *Indexer) QueryAddressTxs(ctx context.Context, addr model.Address, cursor model.AddressCursor, limit int) (AddressTxPage, error) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	var page AddressTxPage
	err := ix.store.View(ctx, func(tx store.Tx) error {
		refs, next, more, verr := tx.ListAddressTxs(addr, cursor, limit)
		if verr != nil {
			return verr
		}
		page = AddressTxPage{Txs: refs, NextCursor: next, HasMore: more}
		return nil
	})
	return page, err
}

// AddressSummary returns the maintained aggregate (tx count, total in/out)
// for addr in O(1). ok is false when the address has no indexed activity.
func (ix *Indexer) AddressSummary(ctx context.Context, addr model.Address) (model.AddressSummary, bool, error) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	var sum model.AddressSummary
	var ok bool
	err := ix.store.View(ctx, func(tx store.Tx) error {
		var verr error
		sum, ok, verr = tx.GetAddressSummary(addr)
		return verr
	})
	return sum, ok, err
}

// Close releases resources and flushes the final snapshot.
func (ix *Indexer) Close() error { return ix.store.Close() }
