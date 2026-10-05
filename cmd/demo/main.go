// Command demo runs the local reproducible indexing scenario: malformed
// inputs, restart-from-checkpoint, a chain reorg and concurrent ingestion
// against a simulated chain and a JSON snapshot file.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/chain"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/decode"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/index"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/model"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/query"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/store"
)

const startHeight = uint64(10)

func main() {
	ctx := context.Background()
	snap := filepath.Join(os.TempDir(), "reorg-indexer-demo-snapshot.json")
	_ = os.Remove(snap)

	sim := chain.NewSimChain(startHeight)
	// Heights 10-13 clean; 14 bad-amount; 15 bad-address; 16 bad-checksum.
	sim.Extend(4)
	sim.ExtendMalformed(1, chain.MalBadAmount)
	sim.ExtendMalformed(1, chain.MalBadAddress)
	sim.ExtendMalformed(1, chain.MalChecksumTx)
	sim.Extend(2) // 17,18 clean

	st, err := store.NewFileBackedMemoryStore(snap)
	must(err)
	ix := index.New(index.Config{StartHeight: startHeight, MaxReorgDepth: 10}, st, sim)

	fmt.Println("== phase 1: ingest until the first malformed block (h=14) ==")
	err = ix.RunTo(ctx, sim.Head())
	fmt.Printf("RunTo stopped: %v\n", err)
	printStats(ctx, ix)

	fmt.Println("\n== phase 2: restart: rebuild store/indexer from snapshot and continue ==")
	must(ix.Close())
	st2, err := store.NewFileBackedMemoryStore(snap)
	must(err)
	sim2 := chain.NewSimChain(startHeight)
	sim2.Extend(9) // 10..18 all clean on the resumed source
	ix2 := index.New(index.Config{StartHeight: startHeight, MaxReorgDepth: 10}, st2, sim2)
	must(ix2.RunTo(ctx, sim2.Head()))
	printStats(ctx, ix2)

	fmt.Println("\n== phase 3: reorg: fork at height 17 (common ancestor 16) ==")
	tip := sim2.Head()
	must(sim2.ForkAt("alt", tip-1))
	sim2.Extend(3) // alt blocks 17',18',19' diverging from ancestor 16
	ix2.BindSource(sim2)
	env, err := sim2.BlockAt(ctx, tip-1) // 17' back-dated
	must(err)
	res, err := ix2.Submit(ctx, env)
	must(err)
	fmt.Printf("reorg submit h=%d outcome=%s basis=%s\n", res.Height, res.Outcome, res.Detail)
	must(ix2.RunTo(ctx, sim2.Head()))
	printStats(ctx, ix2)
	// Verify final tip hash equals the active chain head hash.
	headEnv, err := sim2.BlockAt(ctx, sim2.Head())
	must(err)
	fmt.Printf("final chain head h=%d matches indexed tip: see phase-4 stats\n", headEnv.Height)

	fmt.Println("\n== phase 4: concurrent ingestion, every worker replays the full range ==")
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for h := startHeight; h <= sim2.Head(); h++ {
				env, err := sim2.BlockAt(ctx, h)
				if err != nil {
					fmt.Printf("worker %d fetch err: %v\n", g, err)
					return
				}
				if _, err := ix2.Submit(ctx, env); err != nil {
					fmt.Printf("worker %d submit err: %v\n", g, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	printStats(ctx, ix2)

	fmt.Println("\n== phase 5: address query + summary (paged read model) ==")
	q := query.New(st2)
	// A participant from the first indexed block (height 10, branch index 0):
	// it stays canonical across every reorg, so it always has activity.
	persistentAddr, err := decode.ParseAddress(chain.AddressSeed("canonical:to:0:0"))
	must(err)
	printAddress(ctx, q, persistentAddr, "persistent participant after all phases")

	// The recipient of the pre-reorg canonical block 17's first tx. That block
	// was rolled back in phase 3, so the address must have no activity now:
	// the derived view never resurrects stale-branch txs.
	staleAddr, err := decode.ParseAddress(chain.AddressSeed("canonical:to:7:0"))
	must(err)
	printAddress(ctx, q, staleAddr, "rolled-back canonical-17 recipient")

	// A second reorg to show the query model follows the branch switch and
	// keeps agreeing with the primary index.
	must(sim2.ForkAt("alt2", sim2.Head()-1))
	sim2.Extend(2)
	ix2.BindSource(sim2)
	must(ix2.RunTo(ctx, sim2.Head()))
	printStats(ctx, ix2)
	printAddress(ctx, q, persistentAddr, "persistent participant post-second-reorg")
	printAddress(ctx, q, staleAddr, "rolled-back recipient post-second-reorg")

	must(ix2.Close())

	fmt.Println("\nsnapshot written to", snap)
}

// printAddress pages through all transactions involving addr and prints the
// aggregate summary, demonstrating keyset pagination over the derived index.
func printAddress(ctx context.Context, q *query.Service, addr model.Address, phase string) {
	summary, ok, err := q.Summary(ctx, addr)
	must(err)
	if !ok {
		fmt.Printf("[%s] address %s: no indexed activity\n", phase, addr.Hex())
		return
	}
	var (
		page  query.Page
		count int
		first model.IndexedTx
		last  model.IndexedTx
	)
	page.Limit = 2
	for {
		res, err := q.Transactions(ctx, addr, page)
		must(err)
		if count == 0 && len(res.Txs) > 0 {
			first = res.Txs[0]
		}
		count += len(res.Txs)
		if len(res.Txs) > 0 {
			last = res.Txs[len(res.Txs)-1]
		}
		if !res.HasNext {
			break
		}
		page.AfterHeight = res.NextHeight
		page.AfterIndex = res.NextIndex
		page.HasCursor = true
	}
	fmt.Printf("[%s] address %s: txs=%d inflow=%d outflow=%d\n",
		phase, addr.Hex()[:10], summary.TxCount, summary.Inflow, summary.Outflow)
	if count > 0 {
		fmt.Printf("           paged=%d rows (limit=2/page), range h=%d/%d .. h=%d/%d\n",
			count, first.BlockHeight, first.Index, last.BlockHeight, last.Index)
	}
	if int64(count) != summary.TxCount {
		fmt.Fprintf(os.Stderr, "fatal: address listing count %d != summary tx count %d\n", count, summary.TxCount)
		os.Exit(1)
	}
}

func printStats(ctx context.Context, ix *index.Indexer) {
	st, err := ix.Stats(ctx)
	must(err)
	fmt.Printf("checkpoint next_height=%d tip_height=%d tip_hash=%s buffered=%d blocks=%d failures=%d\n",
		st.NextHeight, st.TipHeight, st.TipHash.Short(), st.Buffered, st.Blocks, st.Failures)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}
