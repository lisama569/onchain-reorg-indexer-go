// 命令行演示：本地模拟链上的增量索引、重组处理与重启续跑。
package main

import (
	"flag"
	"log"
	"os"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/chain"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/indexer"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/store"
)

func main() {
	var (
		snapshot  = flag.String("snapshot", "indexer-snapshot.json", "位点与索引快照文件路径")
		retention = flag.Uint64("retention", 16, "链头之下保留可回滚的区块数")
	)
	flag.Parse()
	logger := log.New(os.Stdout, "", log.LstdFlags)

	// 1. 本地模拟链：先延伸 20 个区块。
	src := chain.NewSimChain()
	src.Extend(20)

	// 2. 文件快照存储：重启后自动从位点续跑。
	st, err := store.OpenFile(*snapshot)
	if err != nil {
		logger.Fatalf("open store: %v", err)
	}
	ix := indexer.New(src, st, *retention, logger)

	if err := ix.SyncTo(20); err != nil {
		logger.Fatalf("initial sync: %v", err)
	}
	cp, _ := ix.Checkpoint()
	logger.Printf("[demo] synced to checkpoint=%+v floor=%d", cp, st.LowestRetained())

	// 3. 制造重组：从高度 15 分叉，新分支延伸到 24。
	src.Fork(15, 10)
	if err := ix.SyncTo(24); err != nil {
		logger.Fatalf("sync after reorg: %v", err)
	}
	cp, _ = ix.Checkpoint()
	logger.Printf("[demo] reorg handled, checkpoint=%+v", cp)

	// 4. 模拟重启：重新打开快照，继续同步。
	src.Extend(6)
	st2, err := store.OpenFile(*snapshot)
	if err != nil {
		logger.Fatalf("reopen store: %v", err)
	}
	ix2 := indexer.New(src, st2, *retention, logger)
	cp2, _ := ix2.Checkpoint()
	logger.Printf("[demo] restarted from checkpoint=%+v", cp2)
	if err := ix2.SyncTo(src.Head()); err != nil {
		logger.Fatalf("resume sync: %v", err)
	}
	cp2, _ = ix2.Checkpoint()
	logger.Printf("[demo] done, final checkpoint=%+v floor=%d", cp2, st2.LowestRetained())
}
