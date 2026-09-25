package chain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// MalformedKind selects which kind of intentionally corrupted envelope a
// simulated source can emit (for testing error handling).
type MalformedKind string

const (
	OK                    MalformedKind = ""
	MalUnsupportedVersion MalformedKind = "unsupported_version"
	MalJSON               MalformedKind = "malformed_json"
	MalMissingBlockHash   MalformedKind = "missing_block_hash"
	MalMissingParentHash  MalformedKind = "missing_parent_hash"
	MalMissingTxHash      MalformedKind = "missing_tx_hash"
	MalBadAddress         MalformedKind = "bad_address"
	MalBadAmount          MalformedKind = "bad_amount"
	MalChecksumBlock      MalformedKind = "checksum_block"
	MalChecksumTx         MalformedKind = "checksum_tx"
)

// WireVersion1 is the only supported envelope version.
const WireVersion1 = 1

// WireBlock is the on-wire v1 block envelope produced by the simulated chain.
type WireBlock struct {
	Version    int      `json:"v"`
	Height     uint64   `json:"h"`
	Hash       string   `json:"hash"`
	ParentHash string   `json:"prev"`
	Checksum   string   `json:"sum"`
	Time       int64    `json:"ts"`
	Txs        []WireTx `json:"txs"`
}

// WireTx is the on-wire v1 transaction envelope.
type WireTx struct {
	Hash     string `json:"hash"`
	From     string `json:"from"`
	To       string `json:"to"`
	Amount   int64  `json:"amt"`
	Checksum string `json:"sum"`
}

// Envelope is a raw message consumed from a data source.
type Envelope struct {
	Height  uint64
	Payload []byte
}

// checksumHex returns the first 8 bytes (16 hex chars) of sha256(data),
// 0x-prefixed. Checksums are short fingerprints, not cryptographic proofs;
// they exist to demonstrate incompatible-encoding rejection.
func checksumHex(data string) string {
	sum := sha256.Sum256([]byte(data))
	return "0x" + hex.EncodeToString(sum[:8])
}

// TxChecksum computes the canonical v1 checksum of a transaction.
func TxChecksum(t WireTx) string {
	return checksumHex(fmt.Sprintf("tx|%s|%s|%s|%d", t.Hash, t.From, t.To, t.Amount))
}

// BlockChecksum computes the canonical v1 checksum of a block header.
func BlockChecksum(w WireBlock) string {
	txhashes := ""
	for i, t := range w.Txs {
		if i > 0 {
			txhashes += ","
		}
		txhashes += t.Hash
	}
	return checksumHex(fmt.Sprintf("blk|%d|%s|%d|%s", w.Height, w.ParentHash, w.Time, txhashes))
}

// EncodeEnvelope serializes a clean wire block, optionally corrupting it in
// exactly one distinguishable way selected by kind.
func EncodeEnvelope(w WireBlock, kind MalformedKind) (Envelope, error) {
	if kind != OK {
		w = corrupt(w, kind)
	}
	payload, err := json.Marshal(w)
	if err != nil {
		return Envelope{}, fmt.Errorf("encode block %d: %w", w.Height, err)
	}
	if kind == MalJSON {
		// Truncated payload: guaranteed invalid JSON.
		payload = append(append([]byte{}, payload[:len(payload)/2]...), '}', '}')
	}
	return Envelope{Height: w.Height, Payload: payload}, nil
}

func corrupt(w WireBlock, kind MalformedKind) WireBlock {
	switch kind {
	case MalUnsupportedVersion:
		w.Version = 255
	case MalMissingBlockHash:
		w.Hash = "0x0000000000000000000000000000000000000000000000000000000000000000"
	case MalMissingParentHash:
		w.ParentHash = "0x0000000000000000000000000000000000000000000000000000000000000000"
	case MalMissingTxHash:
		if len(w.Txs) > 0 {
			w.Txs[0].Hash = "0x0000000000000000000000000000000000000000000000000000000000000000"
			w.Txs[0].Checksum = TxChecksum(w.Txs[0])
		}
	case MalBadAddress:
		if len(w.Txs) > 0 {
			w.Txs[0].From = "0x12"
			w.Txs[0].Checksum = TxChecksum(w.Txs[0])
		}
	case MalBadAmount:
		if len(w.Txs) > 0 {
			w.Txs[0].Amount = -7
			w.Txs[0].Checksum = TxChecksum(w.Txs[0])
		}
	case MalChecksumBlock:
		w.Checksum = "0xdeadbeefdeadbeef"
	case MalChecksumTx:
		if len(w.Txs) > 0 {
			w.Txs[0].Checksum = "0x0badf00d0badf00d"
		}
	}
	return w
}
