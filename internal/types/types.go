// Package types 定义索引流程中使用的核心数据模型与错误分类。
package types

import "errors"

// Hash 是 0x 前缀的 32 字节十六进制编码。
type Hash = string

// Tx 是链上原始交易记录。
type Tx struct {
	Hash  Hash   `json:"hash"`
	From  string `json:"from"`
	To    string `json:"to"`
	Value string `json:"value"` // 十进制大整数字符串
	Nonce uint64 `json:"nonce"`
}

// Block 是链上原始区块记录。
type Block struct {
	Height     uint64 `json:"height"`
	Hash       Hash   `json:"hash"`
	ParentHash Hash   `json:"parentHash"`
	Timestamp  uint64 `json:"timestamp"`
	Txs        []Tx   `json:"txs"`
}

// IndexedTx 是归一化后的可查询交易索引记录。
type IndexedTx struct {
	Hash        Hash   `json:"hash"`
	BlockHash   Hash   `json:"blockHash"`
	BlockHeight uint64 `json:"blockHeight"`
	TxIndex     int    `json:"txIndex"`
	From        string `json:"from"`
	To          string `json:"to"`
	Value       string `json:"value"`
	Nonce       uint64 `json:"nonce"`
}

// IndexedBlock 是归一化后的可查询区块索引记录。
type IndexedBlock struct {
	Height     uint64      `json:"height"`
	Hash       Hash        `json:"hash"`
	ParentHash Hash        `json:"parentHash"`
	Timestamp  uint64      `json:"timestamp"`
	Txs        []IndexedTx `json:"txs"`
}

// Checkpoint 是摄取位点，表示已确认应用到索引存储的链头。
type Checkpoint struct {
	Height uint64 `json:"height"`
	Hash   Hash   `json:"hash"`
}

// RejectReason 区分记录被拒绝的失败原因。
type RejectReason string

const (
	ReasonMissingField RejectReason = "missing_field"  // 必填字段缺失
	ReasonBadEncoding  RejectReason = "bad_encoding"   // 编码不兼容（哈希/数值格式非法）
	ReasonValidation   RejectReason = "validation"     // 校验失败（父哈希/高度不连续等）
	ReasonReorgTooDeep RejectReason = "reorg_too_deep" // 重组超出保留范围
	ReasonDuplicate    RejectReason = "duplicate"      // 重复消费（幂等跳过）
	ReasonOutOfOrder   RejectReason = "out_of_order"   // 乱序且无法衔接
)

// RejectError 携带可区分的失败原因。
type RejectError struct {
	Reason RejectReason
	Height uint64
	Hash   Hash
	Detail string
}

func (e *RejectError) Error() string {
	return "reject[" + string(e.Reason) + "] height=" + itoa(e.Height) + " hash=" + e.Hash + ": " + e.Detail
}

// IsReason 判断 err 是否为指定原因的拒绝。
func IsReason(err error, r RejectReason) bool {
	var re *RejectError
	if errors.As(err, &re) {
		return re.Reason == r
	}
	return false
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
