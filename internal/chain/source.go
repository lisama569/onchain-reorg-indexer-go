package chain

import "context"

// Source is the data source consumed by the indexer. It reads blocks by
// height on the canonical (active) chain.
type Source interface {
	// Head returns the current head height.
	Head() uint64
	// BlockAt returns the raw envelope at height on the active branch.
	BlockAt(ctx context.Context, height uint64) (Envelope, error)
}
