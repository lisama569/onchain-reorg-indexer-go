package chain

import "crypto/sha256"
import "encoding/hex"

// BlockHashSeed deterministically derives a 32-byte block hash hex from a
// height and parent hash. It is a test helper for building envelopes without
// a full SimChain (e.g. address-query scenarios).
func BlockHashSeed(height uint64, parent string) string {
	sum := sha256.Sum256([]byte("blk:" + itoa(int64(height)) + ":" + parent))
	return "0x" + hex.EncodeToString(sum[:])
}

// TxHashSeed deterministically derives a 32-byte tx hash hex.
func TxHashSeed(height uint64, index int, blockHash string) string {
	sum := sha256.Sum256([]byte("tx:" + itoa(int64(height)) + ":" + itoa(int64(index)) + ":" + blockHash))
	return "0x" + hex.EncodeToString(sum[:])
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [24]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// AddressSeed deterministically derives a 20-byte address hex from a seed.
// Exported for demos/tests constructing known participant addresses.
func AddressSeed(seed string) string {
	return addrHex(seed)
}
