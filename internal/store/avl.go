// Package store: avl.go provides an in-memory ordered key/value set backed by
// a height-balanced AVL tree with an explicit comparator. It gives the
// secondary indexes O(log n) insertion/removal/lookup and ordered range
// pagination, independent of the total indexed history length.
package store

// orderedSet stores distinct keys of type K with an associated value V, kept
// sorted by the supplied less comparator. The tree is rebalanced after every
// mutation, so every operation costs O(log n) where n is the number of entries
// in this set.
type orderedSet[K comparable, V any] struct {
	root   *avlNode[K, V]
	size   int
	lessFn func(a, b K) bool
}

type avlNode[K comparable, V any] struct {
	key    K
	value  V
	left   *avlNode[K, V]
	right  *avlNode[K, V]
	height int
}

func newOrderedSet[K comparable, V any](less func(a, b K) bool) *orderedSet[K, V] {
	return &orderedSet[K, V]{lessFn: less}
}

func (s *orderedSet[K, V]) less(a, b K) bool { return s.lessFn(a, b) }

func nodeHeight[K comparable, V any](n *avlNode[K, V]) int {
	if n == nil {
		return 0
	}
	return n.height
}

func (n *avlNode[K, V]) updateHeight() {
	h := nodeHeight(n.left)
	if r := nodeHeight(n.right); r > h {
		h = r
	}
	n.height = h + 1
}

func (n *avlNode[K, V]) balanceFactor() int {
	return nodeHeight(n.right) - nodeHeight(n.left)
}

func rotateRight[K comparable, V any](y *avlNode[K, V]) *avlNode[K, V] {
	x := y.left
	y.left = x.right
	x.right = y
	y.updateHeight()
	x.updateHeight()
	return x
}

func rotateLeft[K comparable, V any](x *avlNode[K, V]) *avlNode[K, V] {
	y := x.right
	x.right = y.left
	y.left = x
	x.updateHeight()
	y.updateHeight()
	return y
}

func rebalance[K comparable, V any](n *avlNode[K, V]) *avlNode[K, V] {
	n.updateHeight()
	switch bf := n.balanceFactor(); {
	case bf > 1:
		if n.right.balanceFactor() < 0 {
			n.right = rotateRight(n.right)
		}
		return rotateLeft(n)
	case bf < -1:
		if n.left.balanceFactor() > 0 {
			n.left = rotateLeft(n.left)
		}
		return rotateRight(n)
	default:
		return n
	}
}

func (s *orderedSet[K, V]) insertNode(n *avlNode[K, V], key K, value V, added *bool) *avlNode[K, V] {
	if n == nil {
		*added = true
		return &avlNode[K, V]{key: key, value: value, height: 1}
	}
	switch {
	case s.less(key, n.key):
		n.left = s.insertNode(n.left, key, value, added)
	case s.less(n.key, key):
		n.right = s.insertNode(n.right, key, value, added)
	default:
		n.value = value
		return n
	}
	return rebalance(n)
}

// insert adds or replaces the value at key. It reports whether a new key was
// added (false means the value was replaced).
func (s *orderedSet[K, V]) insert(key K, value V) bool {
	added := false
	s.root = s.insertNode(s.root, key, value, &added)
	if added {
		s.size++
	}
	return added
}

func minNode[K comparable, V any](n *avlNode[K, V]) *avlNode[K, V] {
	for n.left != nil {
		n = n.left
	}
	return n
}

func (s *orderedSet[K, V]) removeMin(n *avlNode[K, V]) *avlNode[K, V] {
	if n.left == nil {
		return n.right
	}
	n.left = s.removeMin(n.left)
	return rebalance(n)
}

func (s *orderedSet[K, V]) deleteNode(n *avlNode[K, V], key K, removed *bool) *avlNode[K, V] {
	if n == nil {
		return nil
	}
	switch {
	case s.less(key, n.key):
		n.left = s.deleteNode(n.left, key, removed)
	case s.less(n.key, key):
		n.right = s.deleteNode(n.right, key, removed)
	default:
		*removed = true
		if n.right == nil {
			return n.left
		}
		succ := minNode(n.right)
		n.key = succ.key
		n.value = succ.value
		n.right = s.removeMin(n.right)
	}
	return rebalance(n)
}

// delete removes key if present. It reports whether the key existed.
func (s *orderedSet[K, V]) delete(key K) bool {
	removed := false
	before := s.size
	s.root = s.deleteNode(s.root, key, &removed)
	if removed {
		s.size = before - 1
	}
	return removed
}

// get returns the value associated with key.
func (s *orderedSet[K, V]) get(key K) (V, bool) {
	n := s.root
	for n != nil {
		switch {
		case s.less(key, n.key):
			n = n.left
		case s.less(n.key, key):
			n = n.right
		default:
			return n.value, true
		}
	}
	var zero V
	return zero, false
}

// contains reports whether key is present.
func (s *orderedSet[K, V]) contains(key K) bool {
	_, ok := s.get(key)
	return ok
}

// len returns the number of stored keys.
func (s *orderedSet[K, V]) len() int { return s.size }

// first returns the smallest key and its value.
func (s *orderedSet[K, V]) first() (K, V, bool) {
	if s.root == nil {
		var zk K
		var zv V
		return zk, zv, false
	}
	n := minNode(s.root)
	return n.key, n.value, true
}

func maxNode[K comparable, V any](n *avlNode[K, V]) *avlNode[K, V] {
	for n.right != nil {
		n = n.right
	}
	return n
}

// last returns the largest key and its value.
func (s *orderedSet[K, V]) last() (K, V, bool) {
	if s.root == nil {
		var zk K
		var zv V
		return zk, zv, false
	}
	n := maxNode(s.root)
	return n.key, n.value, true
}

// firstAfter returns the smallest key strictly greater than key.
func (s *orderedSet[K, V]) firstAfter(key K) (K, V, bool) {
	var best *avlNode[K, V]
	n := s.root
	for n != nil {
		if s.less(key, n.key) {
			best = n
			n = n.left
		} else {
			n = n.right
		}
	}
	if best == nil {
		var zk K
		var zv V
		return zk, zv, false
	}
	return best.key, best.value, true
}

// appendAscending collects up to limit values in ascending key order using a
// stack-based bounded range walk. When hasAfter is true, the root-to-cursor
// path is pruned so keys <= after are never visited; otherwise traversal
// starts at the minimum key. Cost is O(log n + limit), independent of how far
// the cursor sits into the set. A non-positive limit returns out unchanged.
func (s *orderedSet[K, V]) appendAscending(out []V, after K, hasAfter bool, limit int) []V {
	if limit <= 0 {
		return out
	}
	// Stack holds the in-order frontier: the nodes whose own keys have not
	// been emitted yet, in ascending resolution order.
	var stack []*avlNode[K, V]
	n := s.root
	if hasAfter {
		// Push only nodes that may contain keys > after.
		for n != nil {
			if s.less(after, n.key) {
				stack = append(stack, n)
				n = n.left
			} else {
				n = n.right
			}
		}
	} else {
		for n != nil {
			stack = append(stack, n)
			n = n.left
		}
	}
	remaining := limit
	for remaining > 0 && len(stack) > 0 {
		n = stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		out = append(out, n.value)
		remaining--
		// Descend into the right subtree along its left spine.
		c := n.right
		for c != nil {
			stack = append(stack, c)
			c = c.left
		}
	}
	return out
}

// deleteRange removes every key with lo <= key <= hi and calls onDelete for
// each removed value. Whole subtrees fully outside the range are skipped
// entirely, so cost is O(log n + k log n) for k removed keys.
func (s *orderedSet[K, V]) deleteRange(lo, hi K, onDelete func(V)) {
	var removed int
	var prune func(n *avlNode[K, V]) *avlNode[K, V]
	prune = func(n *avlNode[K, V]) *avlNode[K, V] {
		if n == nil {
			return nil
		}
		if s.less(n.key, lo) {
			n.right = prune(n.right) // left subtree is entirely < lo
			return rebalance(n)
		}
		if s.less(hi, n.key) {
			n.left = prune(n.left) // right subtree is entirely > hi
			return rebalance(n)
		}
		// n.key is inside [lo, hi]: remove n like a standard BST delete and
		// keep pruning the children (which may hold both in- and out-of-range
		// keys).
		onDelete(n.value)
		removed++
		left := prune(n.left)
		right := prune(n.right)
		switch {
		case left == nil:
			return right
		case right == nil:
			return left
		default:
			succ := minNode(right)
			n.key = succ.key
			n.value = succ.value
			n.left = left
			n.right = s.removeMin(right)
			return rebalance(n)
		}
	}
	s.root = prune(s.root)
	s.size -= removed
}

// ascKey is the composite per-address ordering key (block height, in-block
// position). Both fields together uniquely identify an indexed tx within a
// block.
type ascKey struct {
	height uint64
	index  uint64
}

func lessAscKey(a, b ascKey) bool {
	if a.height != b.height {
		return a.height < b.height
	}
	return a.index < b.index
}
