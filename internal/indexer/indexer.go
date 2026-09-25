// Package indexer 实现增量摄取、重组处理与位点管理。
package indexer

import (
	"fmt"
	"log"
	"sync"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/chain"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/store"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/types"
)

// Indexer 驱动区块与交易的增量索引。
// 所有摄取操作经互斥锁串行化，并发摄取天然幂等。
type Indexer struct {
	src       chain.Source
	st        store.Store
	retention uint64 // 链头之下保留可回滚的区块数
	mu        sync.Mutex
	logger    *log.Logger
}

// New 创建索引器。retention 为可回滚保留的区块数；超出该范围的重组将被拒绝。
// logger 为 nil 时使用默认日志。
func New(src chain.Source, st store.Store, retention uint64, logger *log.Logger) *Indexer {
	if logger == nil {
		logger = log.Default()
	}
	return &Indexer{src: src, st: st, retention: retention, logger: logger}
}

// IngestBlock 摄取单个原始区块：归一化、幂等去重、重组检测与处理。
// 返回 nil 表示已应用或幂等跳过；返回 *types.RejectError 表示被拒绝，
// 其 Reason 可区分失败类别。
func (ix *Indexer) IngestBlock(b types.Block) error {
	// 归一化校验在锁外完成：非法记录与链状态无关，直接拒绝。
	nb, err := NormalizeBlock(b)
	if err != nil {
		ix.logf("reject height=%d hash=%s reason=normalize err=%v", b.Height, b.Hash, err)
		return err
	}

	ix.mu.Lock()
	defer ix.mu.Unlock()

	cp, hasCP := ix.st.Checkpoint()

	// 首个区块必须是创世块。
	if !hasCP {
		if b.Height != 0 {
			return ix.reject(types.ReasonOutOfOrder, b,
				fmt.Sprintf("first ingested block must be genesis, checkpoint=none"))
		}
		return ix.apply(nb)
	}

	switch {
	case b.Height <= cp.Height:
		// 已索引区间：哈希一致则幂等跳过，不一致则是重组信号。
		stored, ok := ix.st.BlockAt(b.Height)
		if ok && stored.Hash == b.Hash {
			ix.logf("skip height=%d hash=%s reason=duplicate checkpoint=%d",
				b.Height, b.Hash, cp.Height)
			return nil
		}
		if !ok {
			// 该高度已被裁剪：与规范链一致视为重复历史，幂等跳过；
			// 不一致则是超出保留范围的重组，明确拒绝。
			if canon, srcOK := ix.src.BlockAt(b.Height); srcOK && canon.Hash == b.Hash {
				ix.logf("skip height=%d hash=%s reason=duplicate-pruned checkpoint=%d floor=%d",
					b.Height, b.Hash, cp.Height, ix.st.LowestRetained())
				return nil
			}
			return ix.reject(types.ReasonReorgTooDeep, b,
				fmt.Sprintf("height %d already pruned below floor=%d and conflicts with canonical chain",
					b.Height, ix.st.LowestRetained()))
		}
		return ix.handleReorg(nb, cp)

	case b.Height == cp.Height+1:
		if b.ParentHash != cp.Hash {
			// 父哈希不衔接：新分支顶块，按重组处理。
			return ix.handleReorg(nb, cp)
		}
		return ix.apply(nb)

	default: // b.Height > cp.Height+1
		return ix.reject(types.ReasonOutOfOrder, b,
			fmt.Sprintf("gap: block height %d beyond checkpoint %d+1", b.Height, cp.Height))
	}
}

// SyncTo 从当前位点顺序摄取规范链到目标高度。
func (ix *Indexer) SyncTo(height uint64) error {
	for {
		cp, hasCP := ix.st.Checkpoint()
		next := uint64(0)
		if hasCP {
			next = cp.Height + 1
		}
		if next > height {
			return nil
		}
		b, ok := ix.src.BlockAt(next)
		if !ok {
			return fmt.Errorf("source missing canonical block at height %d", next)
		}
		if err := ix.IngestBlock(b); err != nil {
			return err
		}
	}
}

// Checkpoint 返回当前位点。
func (ix *Indexer) Checkpoint() (types.Checkpoint, bool) {
	return ix.st.Checkpoint()
}

// apply 应用已归一化区块、推进位点并按保留策略裁剪历史。
func (ix *Indexer) apply(nb types.IndexedBlock) error {
	cp := types.Checkpoint{Height: nb.Height, Hash: nb.Hash}
	if err := ix.st.Apply(nb, cp); err != nil {
		return fmt.Errorf("store apply height=%d: %w", nb.Height, err)
	}
	if nb.Height > ix.retention {
		ix.st.Prune(nb.Height - ix.retention)
	}
	ix.logf("apply height=%d hash=%s parent=%s txs=%d checkpoint=%d floor=%d",
		nb.Height, nb.Hash, nb.ParentHash, len(nb.Txs), cp.Height, ix.st.LowestRetained())
	return nil
}

// handleReorg 检测共同祖先，回滚并按规范链新分支重放。
// 共同祖先低于保留下界时拒绝（ReasonReorgTooDeep），不改动任何数据。
func (ix *Indexer) handleReorg(nb types.IndexedBlock, cp types.Checkpoint) error {
	if nb.Height == 0 {
		return ix.reject(types.ReasonValidation, types.Block{Height: nb.Height, Hash: nb.Hash},
			"genesis block conflicts with indexed genesis; cannot reorg height 0")
	}
	floor := ix.st.LowestRetained()
	// 从分叉点向下寻找共同祖先：规范链与已索引数据哈希一致的最高高度。
	start := nb.Height - 1
	if cp.Height < start {
		start = cp.Height
	}
	if start < floor {
		return ix.reject(types.ReasonReorgTooDeep, types.Block{Height: nb.Height, Hash: nb.Hash},
			fmt.Sprintf("fork point %d below retained floor=%d checkpoint=%d", start, floor, cp.Height))
	}
	ancestor := uint64(0)
	found := false
	for h := start; ; h-- {
		srcBlk, srcOK := ix.src.BlockAt(h)
		stored, stOK := ix.st.BlockAt(h)
		if srcOK && stOK && srcBlk.Hash == stored.Hash {
			ancestor, found = h, true
			break
		}
		if h == floor {
			break
		}
	}
	if !found {
		return ix.reject(types.ReasonReorgTooDeep, types.Block{Height: nb.Height, Hash: nb.Hash},
			fmt.Sprintf("no common ancestor at or above retained floor=%d checkpoint=%d", floor, cp.Height))
	}

	// 校验新分支终点必须与规范链一致，否则输入块不在规范链上。
	tip, ok := ix.src.BlockAt(nb.Height)
	if !ok || tip.Hash != nb.Hash {
		return ix.reject(types.ReasonValidation, types.Block{Height: nb.Height, Hash: nb.Hash},
			"block is not on the canonical chain at its height")
	}

	ix.logf("reorg detected height=%d hash=%s ancestor=%d checkpoint=%d floor=%d; rollback+replay",
		nb.Height, nb.Hash, ancestor, cp.Height, floor)

	if err := ix.st.RollbackTo(ancestor); err != nil {
		return fmt.Errorf("rollback to ancestor %d: %w", ancestor, err)
	}
	// 按规范链重放 [ancestor+1, nb.Height]。
	for h := ancestor + 1; h <= nb.Height; h++ {
		srcBlk, ok := ix.src.BlockAt(h)
		if !ok {
			return fmt.Errorf("source missing canonical block at height %d during replay", h)
		}
		rnb, err := NormalizeBlock(srcBlk)
		if err != nil {
			return err
		}
		if err := ix.apply(rnb); err != nil {
			return err
		}
	}
	return nil
}

func (ix *Indexer) reject(reason types.RejectReason, b types.Block, detail string) error {
	err := &types.RejectError{Reason: reason, Height: b.Height, Hash: b.Hash, Detail: detail}
	ix.logf("reject height=%d hash=%s reason=%s detail=%s", b.Height, b.Hash, reason, detail)
	return err
}

func (ix *Indexer) logf(format string, args ...any) {
	ix.logger.Printf("[indexer] "+format, args...)
}
