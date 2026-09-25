package decode

import (
	"testing"

	"github.com/lisama569/onchain-reorg-indexer-go/internal/chain"
	"github.com/lisama569/onchain-reorg-indexer-go/internal/model"
)

func simEnvelope(t *testing.T, kind chain.MalformedKind) chain.Envelope {
	t.Helper()
	sim := chain.NewSimChain(0)
	sim.Extend(1)
	env, err := sim.BlockAt(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Re-encode through the corruption helper to inject each defect.
	var w chain.WireBlock
	if err := jsonUnmarshal(env.Payload, &w); err != nil {
		t.Fatal(err)
	}
	out, err := chain.EncodeEnvelope(w, kind)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestDecodeHappyPath(t *testing.T) {
	d, err := Decode(simEnvelope(t, chain.OK))
	if err != nil {
		t.Fatalf("clean envelope: %v", err)
	}
	if d.Block.Height != 0 || d.Block.Hash.IsZero() || d.Block.ParentHash.IsZero() {
		t.Fatalf("bad block: %+v", d.Block)
	}
	if len(d.Txs) != d.Block.TxCount || len(d.Txs) == 0 {
		t.Fatalf("tx count mismatch: %d vs %d", len(d.Txs), d.Block.TxCount)
	}
	for i, tx := range d.Txs {
		if tx.Index != i || tx.BlockHash != d.Block.Hash || tx.Amount <= 0 {
			t.Fatalf("bad normalized tx: %+v", tx)
		}
	}
	t.Logf("decoded block height=%d hash=%s parent=%s txs=%d",
		d.Block.Height, d.Block.Hash.Short(), d.Block.ParentHash.Short(), len(d.Txs))
}

func TestDecodeReasons(t *testing.T) {
	cases := []struct {
		kind   chain.MalformedKind
		reason string
	}{
		{chain.MalUnsupportedVersion, model.ReasonUnsupportedVersion},
		{chain.MalJSON, model.ReasonMalformedJSON},
		{chain.MalMissingBlockHash, model.ReasonMissingBlockHash},
		{chain.MalMissingParentHash, model.ReasonMissingParentHash},
		{chain.MalMissingTxHash, model.ReasonMissingTxHash},
		{chain.MalBadAddress, model.ReasonBadAddress},
		{chain.MalBadAmount, model.ReasonBadAmount},
		{chain.MalChecksumBlock, model.ReasonChecksumMismatch},
		{chain.MalChecksumTx, model.ReasonChecksumMismatch},
	}
	for _, c := range cases {
		_, err := Decode(simEnvelope(t, c.kind))
		if err == nil {
			t.Fatalf("%s: expected rejection", c.kind)
		}
		de, ok := err.(*DecodeError)
		if !ok || de.Reason != c.reason {
			t.Fatalf("%s: want reason %s, got %v", c.kind, c.reason, err)
		}
		t.Logf("defect=%-20s reason=%-20s detail=%s", c.kind, de.Reason, de.Detail)
	}
}
