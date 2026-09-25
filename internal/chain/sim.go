// Package chain provides a local, fully controllable simulated blockchain
// data source: blocks can be extended, the head can fork into branches, and
// malformed envelopes can be injected.
package chain

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
)

// simBlock is one stored simulated block.
type simBlock struct {
	wire    WireBlock
	malware MalformedKind
}

// Branch is a fork branch in the simulated chain.
type Branch struct {
	Name   string
	blocks []*simBlock
}

func (b *Branch) tip() *simBlock {
	if len(b.blocks) == 0 {
		return nil
	}
	return b.blocks[len(b.blocks)-1]
}

// SimChain is an in-memory controllable blockchain used by tests and the
// local demo.
type SimChain struct {
	mu          sync.RWMutex
	branches    map[string]*Branch
	active      string
	startHeight uint64
}

// NewSimChain creates a simulated chain with a single canonical branch whose
// first block sits at startHeight (startHeight is treated as the genesis: its
// parent hash is zero).
func NewSimChain(startHeight uint64) *SimChain {
	s := &SimChain{
		branches:    map[string]*Branch{},
		active:      "canonical",
		startHeight: startHeight,
	}
	s.branches["canonical"] = &Branch{Name: "canonical"}
	return s
}

func hashHex(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return "0x" + hex.EncodeToString(sum[:])
}

func addrHex(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return "0x" + hex.EncodeToString(sum[:20])
}

// buildWire deterministically constructs the clean wire block for a branch
// slot. Branch name is mixed into the seed so forks produce different hashes
// while blocks shared by two branches (below the fork point) stay identical.
func (s *SimChain) buildWire(b *Branch, height uint64) WireBlock {
	var parentHash string
	if height == s.startHeight {
		// Deterministic non-zero 32-byte sentinel parent for the first
		// indexed block (64 hex digits total).
		parentHash = hashHex("genesis-parent")[:6] + strings.Repeat("0", 60)
	} else {
		parentHash = b.blocks[height-1-s.startHeight].wire.Hash
	}
	index := height - s.startHeight
	seed := fmt.Sprintf("%s:%d", b.Name, index)
	w := WireBlock{
		Version:    WireVersion1,
		Height:     height,
		Hash:       hashHex(seed),
		ParentHash: parentHash,
		Time:       int64(1_700_000_000 + height*12),
	}
	txCount := int(index%3) + 1 // 1..3 deterministic transactions
	for i := 0; i < txCount; i++ {
		t := WireTx{
			Hash:   hashHex(fmt.Sprintf("%s:tx:%d:%d", b.Name, index, i)),
			From:   addrHex(fmt.Sprintf("%s:from:%d:%d", b.Name, index, i)),
			To:     addrHex(fmt.Sprintf("%s:to:%d:%d", b.Name, index, i)),
			Amount: int64(100 + int(index)*7 + i),
		}
		t.Checksum = TxChecksum(t)
		w.Txs = append(w.Txs, t)
	}
	w.Checksum = BlockChecksum(w)
	return w
}

func (s *SimChain) appendBlock(b *Branch, malware MalformedKind) string {
	height := s.startHeight + uint64(len(b.blocks))
	blk := &simBlock{wire: s.buildWire(b, height), malware: malware}
	b.blocks = append(b.blocks, blk)
	return blk.wire.Hash
}

// Extend appends n clean blocks to the active branch and returns their hashes.
func (s *SimChain) Extend(n int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.branches[s.active]
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, s.appendBlock(b, OK))
	}
	return out
}

// ExtendMalformed appends blocks to the active branch, each corrupted with
// the given kind, and returns their (clean) hashes.
func (s *SimChain) ExtendMalformed(n int, kind MalformedKind) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.branches[s.active]
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, s.appendBlock(b, kind))
	}
	return out
}

// Fork creates a new branch sharing every block of the active branch up to
// its tip, marks it active, and returns the new branch name. The previous
// branch is retained (typically named "canonical") for reorg testing.
func (s *SimChain) Fork(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.branches[s.active]
	forked := &Branch{Name: name, blocks: make([]*simBlock, len(src.blocks))}
	copy(forked.blocks, src.blocks)
	s.branches[name] = forked
	s.active = name
}

// ForkAt creates a new branch sharing the active branch blocks strictly
// below height (the fork point is height-1), marks it active and returns the
// branch name. Blocks at or above height on the old branch are retained there
// so the source can switch sides and the indexer can observe a real reorg.
func (s *SimChain) ForkAt(name string, height uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.branches[s.active]
	if height < s.startHeight || height > s.startHeight+uint64(len(src.blocks)) {
		return fmt.Errorf("simchain: fork height %d out of range [%d..%d]",
			height, s.startHeight, s.startHeight+uint64(len(src.blocks)))
	}
	keep := int(height - s.startHeight)
	forked := &Branch{Name: name, blocks: make([]*simBlock, keep)}
	copy(forked.blocks, src.blocks[:keep])
	s.branches[name] = forked
	s.active = name
	return nil
}

// Activate switches the active branch used by Head and BlockAt.
func (s *SimChain) Activate(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.branches[name]; !ok {
		return fmt.Errorf("simchain: unknown branch %q", name)
	}
	s.active = name
	return nil
}

func (b *Branch) at(startHeight, height uint64) (*simBlock, error) {
	if height < startHeight {
		return nil, fmt.Errorf("simchain: height %d below start %d", height, startHeight)
	}
	idx := height - startHeight
	if idx >= uint64(len(b.blocks)) {
		return nil, fmt.Errorf("simchain: block not available at height %d (head index %d)", height, len(b.blocks)-1)
	}
	return b.blocks[idx], nil
}

func encodeBlock(blk *simBlock) (Envelope, error) {
	return EncodeEnvelope(blk.wire, blk.malware)
}

// Head returns the current head height of the active branch. Returns
// startHeight-1 when the chain is empty.
func (s *SimChain) Head() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b := s.branches[s.active]
	if len(b.blocks) == 0 {
		return s.startHeight - 1
	}
	return b.blocks[len(b.blocks)-1].wire.Height
}

// BlockAt returns the (possibly malformed) envelope at height on the active
// branch.
func (s *SimChain) BlockAt(ctx context.Context, height uint64) (Envelope, error) {
	s.mu.RLock()
	b := s.branches[s.active]
	blk, err := b.at(s.startHeight, height)
	s.mu.RUnlock()
	if err != nil {
		return Envelope{}, err
	}
	return encodeBlock(blk)
}

// CanonicalHead returns the head height of the "canonical" branch.
func (s *SimChain) CanonicalHead() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b := s.branches["canonical"]
	if len(b.blocks) == 0 {
		return s.startHeight - 1
	}
	return b.blocks[len(b.blocks)-1].wire.Height
}

// ActiveBranch reports the active branch name.
func (s *SimChain) ActiveBranch() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active
}

// BlockAtBranch reads an envelope from an arbitrary branch. It is a test
// helper for preparing replay payloads; the indexer itself only consumes the
// active branch through Source.
func (s *SimChain) BlockAtBranch(ctx context.Context, branch string, height uint64) (Envelope, error) {
	s.mu.RLock()
	b, ok := s.branches[branch]
	s.mu.RUnlock()
	if !ok {
		return Envelope{}, fmt.Errorf("simchain: unknown branch %q", branch)
	}
	blk, err := b.at(s.startHeight, height)
	if err != nil {
		return Envelope{}, err
	}
	return encodeBlock(blk)
}

// BlockHashAt exposes the clean hash at a height on a branch (test helper).
func (s *SimChain) BlockHashAt(branch string, height uint64) (string, error) {
	s.mu.RLock()
	b, ok := s.branches[branch]
	s.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("simchain: unknown branch %q", branch)
	}
	blk, err := b.at(s.startHeight, height)
	if err != nil {
		return "", err
	}
	return blk.wire.Hash, nil
}
