// Package model defines the normalized, queryable index data model and the
// catalog of distinguishable failure reasons.
package model

import (
	"encoding/hex"
	"fmt"
	"time"
)

// Hash is a 32-byte block identifier rendered as a 0x-prefixed hex string.
type Hash [32]byte

// Address is a 20-byte account identifier rendered as a 0x-prefixed hex string.
type Address [20]byte

// Hex renders h as a 0x-prefixed lowercase hex string.
func (h Hash) Hex() string { return "0x" + hex.EncodeToString(h[:]) }

// Short renders the first 10 characters of Hex, used in test logs.
func (h Hash) Short() string {
	s := h.Hex()
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

// String implements fmt.Stringer.
func (h Hash) String() string { return h.Hex() }

// Hex renders a as a 0x-prefixed lowercase hex string.
func (a Address) Hex() string { return "0x" + hex.EncodeToString(a[:]) }

// String implements fmt.Stringer.
func (a Address) String() string { return a.Hex() }

// ZeroHash reports whether h is the all-zero hash.
func (h Hash) IsZero() bool {
	for _, b := range h {
		if b != 0 {
			return false
		}
	}
	return true
}

// IndexedBlock is the normalized block record stored by the indexer.
type IndexedBlock struct {
	Height     uint64
	Hash       Hash
	ParentHash Hash
	TxCount    int
	IncludedAt time.Time
}

// IndexedTx is the normalized transaction record stored by the indexer.
type IndexedTx struct {
	TxHash      Hash
	BlockHeight uint64
	BlockHash   Hash
	Index       int
	From        Address
	To          Address
	Amount      int64
}

// Failure reason codes. Each malformed input is rejected with one of these
// stable codes instead of being silently dropped.
const (
	// ReasonUnsupportedVersion: envelope version is unknown / incompatible.
	ReasonUnsupportedVersion = "UNSUPPORTED_VERSION"
	// ReasonMalformedJSON: payload is not valid JSON.
	ReasonMalformedJSON = "MALFORMED_JSON"
	// ReasonMissingBlockHash: block hash field is absent/zero.
	ReasonMissingBlockHash = "MISSING_BLOCK_HASH"
	// ReasonMissingParentHash: parent hash field is absent/zero.
	ReasonMissingParentHash = "MISSING_PARENT_HASH"
	// ReasonMissingTxHash: a transaction hash field is absent/zero.
	ReasonMissingTxHash = "MISSING_TX_HASH"
	// ReasonBadAddress: a from/to address cannot be decoded.
	ReasonBadAddress = "BAD_ADDRESS"
	// ReasonBadAmount: a transaction amount is outside the valid range.
	ReasonBadAmount = "BAD_AMOUNT"
	// ReasonChecksumMismatch: block or transaction checksum does not verify.
	ReasonChecksumMismatch = "CHECKSUM_MISMATCH"
	// ReasonBadParentLink: parent hash does not match the prior block.
	ReasonBadParentLink = "BAD_PARENT_LINK"
	// ReasonBeforeStart: submitted height is below the configured start.
	ReasonBeforeStart = "BEFORE_START_HEIGHT"
	// ReasonReorgTooDeep: a fork diverges beyond the retained/finalized window.
	ReasonReorgTooDeep = "REORG_TOO_DEEP"
)

// AddressSummary is the per-address aggregate derived from the main tx
// index. TxCount counts distinct transactions involving the address (a
// self-transfer from==to is counted once); Inflow sums amounts where the
// address is the recipient and Outflow sums amounts where it is the sender.
type AddressSummary struct {
	Address Address
	TxCount int64
	Inflow  int64
	Outflow int64
}

// FailureRecord is persisted for every rejected record so failures are
// auditable and never silently lost.
type FailureRecord struct {
	Height    uint64
	Hash      string
	Stage     string
	Reason    string
	Detail    string
	Raw       string
	CreatedAt time.Time
}

// Fingerprint returns a stable identity used to de-duplicate repeated
// submissions of the same rejected record.
func (f FailureRecord) Fingerprint() string {
	return fmt.Sprintf("%d|%s|%s|%s", f.Height, f.Hash, f.Stage, f.Reason)
}
