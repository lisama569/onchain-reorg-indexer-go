// Package chain 提供本地模拟的链数据源，支持分叉与重组。
package chain

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/types"
)

// Source 是链数据源接口：按高度读取规范链上的区块。
type Source interface {
	// Head 返回当前规范链头高度。
	Head() uint64
	// BlockAt 返回规范链上指定高度的区块；不存在时返回 ok=false。
	BlockAt(height uint64) (types.Block, bool)
}

// SimChain 是本地模拟链。区块内容由 (branchID, height) 确定性生成，
// 因此测试可复现；通过 Fork 可在任意高度制造重组。
type SimChain struct {
	blocks  map[uint64]types.Block // 当前规范链
	head    uint64
	nextBr  int
	genesis types.Hash
}

// NewSimChain 创建只含创世块（高度 0）的模拟链。
func NewSimChain() *SimChain {
	c := &SimChain{
		blocks:  make(map[uint64]types.Block),
		genesis: hashOf("genesis", 0, 0),
	}
	b := types.Block{
		Height:     0,
		Hash:       c.genesis,
		ParentHash: types.Hash("0x" + fmt.Sprintf("%064d", 0)),
		Timestamp:  1_700_000_000,
	}
	c.blocks[0] = b
	return c
}

// Head 返回规范链头高度。
func (c *SimChain) Head() uint64 { return c.head }

// BlockAt 返回规范链上指定高度的区块。
func (c *SimChain) BlockAt(height uint64) (types.Block, bool) {
	b, ok := c.blocks[height]
	return b, ok
}

// Extend 在规范链头之后追加 n 个新区块，返回追加的区块。
func (c *SimChain) Extend(n int) []types.Block {
	c.nextBr++
	branch := c.nextBr
	out := make([]types.Block, 0, n)
	for i := 0; i < n; i++ {
		h := c.head + 1
		b := c.makeBlock(branch, h, c.blocks[c.head].Hash)
		c.blocks[h] = b
		c.head = h
		out = append(out, b)
	}
	return out
}

// Fork 从 fromHeight 开始制造分叉：废弃规范链上 [fromHeight, head]
// 的区块，替换为一条长度为 n 的新分支，返回新分支上的区块。
// 调用方据此可验证索引器回滚到共同祖先并按新分支重放。
func (c *SimChain) Fork(fromHeight uint64, n int) []types.Block {
	if fromHeight == 0 || fromHeight > c.head {
		panic(fmt.Sprintf("Fork: invalid fromHeight %d (head=%d)", fromHeight, c.head))
	}
	for h := fromHeight; h <= c.head; h++ {
		delete(c.blocks, h)
	}
	c.head = fromHeight - 1
	return c.Extend(n)
}

// makeBlock 确定性生成一个区块：哈希与交易内容仅取决于分支与高度。
func (c *SimChain) makeBlock(branch int, height uint64, parent types.Hash) types.Block {
	h := types.Block{
		Height:     height,
		Hash:       hashOf("block", uint64(branch), height),
		ParentHash: parent,
		Timestamp:  1_700_000_000 + height*12,
	}
	// 每个区块固定生成 2 笔确定性交易。
	for i := 0; i < 2; i++ {
		h.Txs = append(h.Txs, types.Tx{
			Hash:  hashOf("tx", uint64(branch), height*16+uint64(i)),
			From:  addrOf(uint64(branch)*1000 + height*10 + uint64(i)),
			To:    addrOf(uint64(branch)*2000 + height*10 + uint64(i)),
			Value: fmt.Sprintf("%d", 1000+height*10+uint64(i)),
			Nonce: height*2 + uint64(i),
		})
	}
	return h
}

// hashOf 生成确定性的 0x 前缀 32 字节十六进制哈希。
func hashOf(tag string, a, b uint64) types.Hash {
	var buf [8 + 8]byte
	binary.BigEndian.PutUint64(buf[0:8], a)
	binary.BigEndian.PutUint64(buf[8:16], b)
	sum := sha256.Sum256(append([]byte(tag), buf[:]...))
	return types.Hash("0x" + fmt.Sprintf("%x", sum))
}

// addrOf 生成确定性的 0x 前缀地址字符串。
func addrOf(v uint64) string {
	return "0x" + fmt.Sprintf("%040x", v)
}
