// Package store 定义索引存储驱动接口与内存实现。
package store

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/types"
)

// Store 是索引存储驱动：区块应用、回滚与裁剪在同一接口内完成。
// 所有方法必须并发安全；Apply/RollbackTo/Prune 需原子生效。
type Store interface {
	// Checkpoint 返回当前持久化位点；无位点时 ok=false。
	Checkpoint() (types.Checkpoint, bool)
	// BlockAt 返回已索引的指定高度区块；不存在时 ok=false。
	BlockAt(height uint64) (types.IndexedBlock, bool)
	// Apply 原子地应用一个已归一化区块并推进位点。
	Apply(b types.IndexedBlock, cp types.Checkpoint) error
	// RollbackTo 回滚到指定高度：删除其上方全部数据，位点回退到该高度。
	RollbackTo(height uint64) error
	// Prune 裁剪低于指定高度的历史，抬升可回滚下界。
	Prune(below uint64)
	// LowestRetained 返回仍保留可回滚到的最低高度。
	LowestRetained() uint64
	// Heights 返回当前保留的全部高度（升序），用于测试与校验。
	Heights() []uint64
}

// Memory 是基于内存的 Store，可选落盘快照以支持重启续跑。
type Memory struct {
	mu     sync.RWMutex
	blocks map[uint64]types.IndexedBlock
	cp     types.Checkpoint
	hasCP  bool
	floor  uint64 // 可回滚下界（含）
	path   string // 快照文件路径；空表示纯内存
}

// NewMemory 创建纯内存存储。
func NewMemory() *Memory {
	return &Memory{blocks: make(map[uint64]types.IndexedBlock)}
}

// OpenFile 打开（或创建）带文件快照的存储；已存在快照时自动恢复，实现重启续跑。
func OpenFile(path string) (*Memory, error) {
	m := NewMemory()
	m.path = path
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return m, nil
		}
		return nil, fmt.Errorf("read snapshot: %w", err)
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}
	m.blocks = snap.Blocks
	if m.blocks == nil {
		m.blocks = make(map[uint64]types.IndexedBlock)
	}
	m.cp = snap.Checkpoint
	m.hasCP = snap.HasCheckpoint
	m.floor = snap.Floor
	return m, nil
}

type snapshot struct {
	Checkpoint    types.Checkpoint              `json:"checkpoint"`
	HasCheckpoint bool                          `json:"hasCheckpoint"`
	Floor         uint64                        `json:"floor"`
	Blocks        map[uint64]types.IndexedBlock `json:"blocks"`
}

func (m *Memory) Checkpoint() (types.Checkpoint, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cp, m.hasCP
}

func (m *Memory) BlockAt(height uint64) (types.IndexedBlock, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.blocks[height]
	return b, ok
}

// Apply 原子写入区块并推进位点；重复应用相同 (height, hash) 是幂等无操作。
func (m *Memory) Apply(b types.IndexedBlock, cp types.Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.blocks[b.Height]; ok && existing.Hash == b.Hash {
		// 幂等：相同区块重复应用，仅确保位点不回退。
		if !m.hasCP || cp.Height > m.cp.Height {
			m.cp, m.hasCP = cp, true
		}
		return m.persistLocked()
	}
	m.blocks[b.Height] = b
	m.cp, m.hasCP = cp, true
	return m.persistLocked()
}

// RollbackTo 删除 height 之上的全部区块，位点回退到 height。
func (m *Memory) RollbackTo(height uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if height < m.floor {
		return fmt.Errorf("rollback below retained floor: height=%d floor=%d", height, m.floor)
	}
	b, ok := m.blocks[height]
	if !ok {
		return fmt.Errorf("rollback target missing: height=%d", height)
	}
	for h := range m.blocks {
		if h > height {
			delete(m.blocks, h)
		}
	}
	m.cp = types.Checkpoint{Height: b.Height, Hash: b.Hash}
	m.hasCP = true
	return m.persistLocked()
}

// Prune 删除低于 below 的区块并抬升下界。
func (m *Memory) Prune(below uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for h := range m.blocks {
		if h < below {
			delete(m.blocks, h)
		}
	}
	if below > m.floor {
		m.floor = below
	}
	_ = m.persistLocked()
}

func (m *Memory) LowestRetained() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.floor
}

func (m *Memory) Heights() []uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]uint64, 0, len(m.blocks))
	for h := range m.blocks {
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// persistLocked 在持有写锁时落盘快照；纯内存模式直接返回。
func (m *Memory) persistLocked() error {
	if m.path == "" {
		return nil
	}
	snap := snapshot{
		Checkpoint:    m.cp,
		HasCheckpoint: m.hasCP,
		Floor:         m.floor,
		Blocks:        m.blocks,
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err := os.Rename(tmp, m.path); err != nil {
		return fmt.Errorf("commit snapshot: %w", err)
	}
	return nil
}
