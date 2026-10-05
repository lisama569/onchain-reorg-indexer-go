package store

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestOrderedSetInsertGetOrder(t *testing.T) {
	s := newOrderedSet[int, int](func(a, b int) bool { return a < b })
	const n = 500
	rng := rand.New(rand.NewSource(42))
	for _, k := range rng.Perm(n) {
		if !s.insert(k, k*10) {
			t.Fatalf("first insert of %d must report added", k)
		}
	}
	// Re-insert every key with a replacement value; size must not change.
	for k := 0; k < n; k++ {
		if s.insert(k, k) {
			t.Fatalf("re-insert of %d must not report added", k)
		}
		if got, ok := s.get(k); !ok || got != k {
			t.Fatalf("replaced value for %d: %d ok=%v", k, got, ok)
		}
	}
	if s.len() != n {
		t.Fatalf("size=%d want %d", s.len(), n)
	}

	// Keyset pagination with varying page sizes must reproduce the full order.
	var paged []int
	var after int
	hasAfter := false
	for {
		pg := s.appendAscending(nil, after, hasAfter, 37)
		if len(pg) == 0 {
			break
		}
		paged = append(paged, pg...)
		after = pg[len(pg)-1]
		hasAfter = true
		if len(pg) < 37 {
			break
		}
	}
	if len(paged) != n {
		t.Fatalf("paged len=%d want %d", len(paged), n)
	}
	for i, v := range paged {
		if v != i {
			t.Fatalf("paged[%d]=%d want %d", i, v, i)
		}
	}

	if k, _, ok := s.first(); !ok || k != 0 {
		t.Fatalf("first=%d ok=%v", k, ok)
	}
	if k, _, ok := s.last(); !ok || k != n-1 {
		t.Fatalf("last=%d ok=%v", k, ok)
	}
	if k, _, ok := s.firstAfter(99); !ok || k != 100 {
		t.Fatalf("firstAfter(99)=%d ok=%v", k, ok)
	}
	if _, _, ok := s.firstAfter(n - 1); ok {
		t.Fatalf("firstAfter of max key must be absent")
	}
	t.Logf("ordered set: %d keys, keyset pagination reproduces full order", n)
}

func TestOrderedSetDelete(t *testing.T) {
	s := newOrderedSet[int, int](func(a, b int) bool { return a < b })
	const n = 1000
	for k := 0; k < n; k++ {
		s.insert(k, k*10)
	}
	for _, k := range []int{0, n - 1, 250, 750, 500, 1, n - 2} {
		if !s.delete(k) {
			t.Fatalf("delete %d must report removed", k)
		}
		if s.delete(k) {
			t.Fatalf("second delete %d must report not removed", k)
		}
	}
	if s.len() != n-7 {
		t.Fatalf("size=%d want %d", s.len(), n-7)
	}
	all := s.appendAscending(nil, 0, false, n)
	if len(all) != n-7 {
		t.Fatalf("walk len=%d want %d", len(all), n-7)
	}
	prev := -1
	for _, v := range all {
		k := v / 10
		if k <= prev {
			t.Fatalf("order broken at %d after %d", k, prev)
		}
		prev = k
	}
	t.Logf("delete: %d remaining keys still ascending", len(all))
}

func TestOrderedSetDeleteRange(t *testing.T) {
	s := newOrderedSet[ascKey, string](lessAscKey)
	for h := uint64(0); h < 10; h++ {
		for i := uint64(0); i < 4; i++ {
			s.insert(ascKey{h, i}, fmt.Sprintf("%d:%d", h, i))
		}
	}
	deleted := map[string]bool{}
	s.deleteRange(ascKey{3, 0}, ascKey{6, 3}, func(v string) { deleted[v] = true })
	if s.len() != 40-16 {
		t.Fatalf("size=%d want 24", s.len())
	}
	if len(deleted) != 16 {
		t.Fatalf("callbacks=%d want 16", len(deleted))
	}
	got := s.appendAscending(nil, ascKey{}, false, 100)
	if len(got) != 24 {
		t.Fatalf("remaining=%d want 24: %v", len(got), got)
	}
	for _, v := range got {
		var h, i uint64
		if _, err := fmt.Sscanf(v, "%d:%d", &h, &i); err != nil {
			t.Fatal(err)
		}
		if h >= 3 && h <= 6 {
			t.Fatalf("key (%d,%d) survived range delete", h, i)
		}
	}
	t.Logf("range delete removed 16 keys across heights 3..6, %d remain", len(got))
}

func TestOrderedSetEmpty(t *testing.T) {
	s := newOrderedSet[int, int](func(a, b int) bool { return a < b })
	if _, _, ok := s.first(); ok {
		t.Fatal("first on empty set")
	}
	if _, _, ok := s.last(); ok {
		t.Fatal("last on empty set")
	}
	if got := s.appendAscending(nil, 0, false, 10); len(got) != 0 {
		t.Fatalf("empty walk=%v", got)
	}
	if s.delete(1) {
		t.Fatal("delete on empty set")
	}
}
