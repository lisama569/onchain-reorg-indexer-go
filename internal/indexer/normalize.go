package indexer

import (
	"fmt"
	"strings"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/types"
)

// NormalizeBlock 将原始区块归一化为可查询索引记录。
// 字段缺失、编码不兼容、校验失败的记录会被拒绝并携带可区分的原因。
func NormalizeBlock(b types.Block) (types.IndexedBlock, error) {
	if b.Hash == "" || b.ParentHash == "" {
		return types.IndexedBlock{}, &types.RejectError{
			Reason: types.ReasonMissingField, Height: b.Height, Hash: b.Hash,
			Detail: "block hash or parentHash is empty",
		}
	}
	if err := checkHash(b.Hash); err != nil {
		return types.IndexedBlock{}, &types.RejectError{
			Reason: types.ReasonBadEncoding, Height: b.Height, Hash: b.Hash,
			Detail: "block hash: " + err.Error(),
		}
	}
	if b.Height > 0 {
		if err := checkHash(b.ParentHash); err != nil {
			return types.IndexedBlock{}, &types.RejectError{
				Reason: types.ReasonBadEncoding, Height: b.Height, Hash: b.Hash,
				Detail: "parentHash: " + err.Error(),
			}
		}
	}
	out := types.IndexedBlock{
		Height:     b.Height,
		Hash:       b.Hash,
		ParentHash: b.ParentHash,
		Timestamp:  b.Timestamp,
	}
	seen := make(map[types.Hash]struct{}, len(b.Txs))
	for i, tx := range b.Txs {
		itx, err := normalizeTx(b, i, tx)
		if err != nil {
			return types.IndexedBlock{}, err
		}
		if _, dup := seen[itx.Hash]; dup {
			return types.IndexedBlock{}, &types.RejectError{
				Reason: types.ReasonValidation, Height: b.Height, Hash: b.Hash,
				Detail: fmt.Sprintf("duplicate tx hash %s within block", itx.Hash),
			}
		}
		seen[itx.Hash] = struct{}{}
		out.Txs = append(out.Txs, itx)
	}
	return out, nil
}

func normalizeTx(b types.Block, idx int, tx types.Tx) (types.IndexedTx, error) {
	reject := func(reason types.RejectReason, detail string) (types.IndexedTx, error) {
		return types.IndexedTx{}, &types.RejectError{
			Reason: reason, Height: b.Height, Hash: b.Hash,
			Detail: fmt.Sprintf("tx[%d]: %s", idx, detail),
		}
	}
	if tx.Hash == "" || tx.From == "" || tx.To == "" || tx.Value == "" {
		return reject(types.ReasonMissingField, "hash/from/to/value is empty")
	}
	if err := checkHash(tx.Hash); err != nil {
		return reject(types.ReasonBadEncoding, "tx hash: "+err.Error())
	}
	if err := checkAddr(tx.From); err != nil {
		return reject(types.ReasonBadEncoding, "from: "+err.Error())
	}
	if err := checkAddr(tx.To); err != nil {
		return reject(types.ReasonBadEncoding, "to: "+err.Error())
	}
	if !isDecimal(tx.Value) {
		return reject(types.ReasonBadEncoding, "value is not a decimal integer: "+tx.Value)
	}
	return types.IndexedTx{
		Hash:        tx.Hash,
		BlockHash:   b.Hash,
		BlockHeight: b.Height,
		TxIndex:     idx,
		From:        tx.From,
		To:          tx.To,
		Value:       tx.Value,
		Nonce:       tx.Nonce,
	}, nil
}

// checkHash 校验 0x 前缀 32 字节十六进制编码。
func checkHash(h string) error {
	if !strings.HasPrefix(h, "0x") || len(h) != 66 {
		return fmt.Errorf("want 0x-prefixed 32-byte hex, got %q", h)
	}
	return checkHex(h[2:])
}

// checkAddr 校验 0x 前缀 20 字节十六进制编码。
func checkAddr(a string) error {
	if !strings.HasPrefix(a, "0x") || len(a) != 42 {
		return fmt.Errorf("want 0x-prefixed 20-byte hex, got %q", a)
	}
	return checkHex(a[2:])
}

func checkHex(s string) error {
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return fmt.Errorf("non-hex character %q", c)
		}
	}
	return nil
}

func isDecimal(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}
