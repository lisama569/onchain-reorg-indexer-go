// Package decode turns raw chain envelopes into normalized model records,
// rejecting malformed inputs with distinguishable reasons.
package decode

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/chain"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/model"
)

// DecodedBlock is the result of a successful decode.
type DecodedBlock struct {
	Block model.IndexedBlock
	Txs   []model.IndexedTx
}

// DecodeError attaches a stable reason code to a rejected envelope.
type DecodeError struct {
	// Reason is one of the model.Reason* codes.
	Reason string
	Detail string
}

func (e *DecodeError) Error() string {
	return fmt.Sprintf("decode rejected (%s): %s", e.Reason, e.Detail)
}

func reject(reason, detail string) error {
	return &DecodeError{Reason: reason, Detail: detail}
}

// Decode validates and normalizes a raw envelope. On failure it returns a
// *DecodeError whose Reason is one of the model.Reason* codes. Validation
// order is fixed so the first failure is deterministic and reproducible.
func Decode(env chain.Envelope) (*DecodedBlock, error) {
	var w chain.WireBlock
	if err := json.Unmarshal(env.Payload, &w); err != nil {
		return nil, reject(model.ReasonMalformedJSON, fmt.Sprintf("payload is not valid JSON: %v", err))
	}
	if w.Version != chain.WireVersion1 {
		return nil, reject(model.ReasonUnsupportedVersion, fmt.Sprintf("envelope version %d, supported %d", w.Version, chain.WireVersion1))
	}
	if w.Height != env.Height {
		return nil, reject(model.ReasonMalformedJSON, fmt.Sprintf("envelope height %d disagrees with payload height %d", env.Height, w.Height))
	}
	blockHash, err := requiredHash(w.Hash, "block.hash", model.ReasonMissingBlockHash)
	if err != nil {
		return nil, err
	}
	parentHash, err := requiredHash(w.ParentHash, "block.prev", model.ReasonMissingParentHash)
	if err != nil {
		return nil, err
	}

	txs := make([]model.IndexedTx, 0, len(w.Txs))
	for i, wt := range w.Txs {
		txHash, err := requiredHash(wt.Hash, fmt.Sprintf("txs[%d].hash", i), model.ReasonMissingTxHash)
		if err != nil {
			return nil, err
		}
		from, err := ParseAddress(wt.From)
		if err != nil {
			return nil, reject(model.ReasonBadAddress, fmt.Sprintf("txs[%d].from %q: %v", i, wt.From, err))
		}
		to, err := ParseAddress(wt.To)
		if err != nil {
			return nil, reject(model.ReasonBadAddress, fmt.Sprintf("txs[%d].to %q: %v", i, wt.To, err))
		}
		if wt.Amount < 0 {
			return nil, reject(model.ReasonBadAmount, fmt.Sprintf("txs[%d].amount must be non-negative, got %d", i, wt.Amount))
		}
		if want := chain.TxChecksum(wt); !strings.EqualFold(want, wt.Checksum) {
			return nil, reject(model.ReasonChecksumMismatch, fmt.Sprintf("txs[%d] checksum %s does not verify (want %s)", i, wt.Checksum, want))
		}
		txs = append(txs, model.IndexedTx{
			TxHash:      txHash,
			BlockHeight: w.Height,
			BlockHash:   blockHash,
			Index:       i,
			From:        from,
			To:          to,
			Amount:      wt.Amount,
		})
	}

	if want := chain.BlockChecksum(w); !strings.EqualFold(want, w.Checksum) {
		return nil, reject(model.ReasonChecksumMismatch, fmt.Sprintf("block %d checksum %s does not verify (want %s)", w.Height, w.Checksum, want))
	}

	inc := time.Unix(w.Time, 0).UTC()
	return &DecodedBlock{
		Block: model.IndexedBlock{
			Height:     w.Height,
			Hash:       blockHash,
			ParentHash: parentHash,
			TxCount:    len(txs),
			IncludedAt: inc,
		},
		Txs: txs,
	}, nil
}

func requiredHash(s, field, reason string) (model.Hash, error) {
	h, err := ParseHash(s)
	if err != nil {
		return model.Hash{}, reject(reason, fmt.Sprintf("%s is missing or invalid: %v", field, err))
	}
	if h.IsZero() {
		return model.Hash{}, reject(reason, fmt.Sprintf("%s must not be zero", field))
	}
	return h, nil
}

// ParseHash parses a 0x-prefixed 32-byte hash.
func ParseHash(s string) (model.Hash, error) {
	raw, err := parseHex(s, 32)
	if err != nil {
		return model.Hash{}, err
	}
	var h model.Hash
	copy(h[:], raw)
	return h, nil
}

// ParseAddress parses a 0x-prefixed 20-byte address.
func ParseAddress(s string) (model.Address, error) {
	raw, err := parseHex(s, 20)
	if err != nil {
		return model.Address{}, err
	}
	var a model.Address
	copy(a[:], raw)
	return a, nil
}

func parseHex(s string, n int) ([]byte, error) {
	if !strings.HasPrefix(s, "0x") {
		return nil, fmt.Errorf("expected 0x prefix")
	}
	raw, err := hex.DecodeString(s[2:])
	if err != nil {
		return nil, fmt.Errorf("invalid hex: %w", err)
	}
	if len(raw) != n {
		return nil, fmt.Errorf("expected %d bytes, got %d", n, len(raw))
	}
	return raw, nil
}
