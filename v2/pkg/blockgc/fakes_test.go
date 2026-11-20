package blockgc

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrPoolExhausted mirrors ipam.ErrNoBlock for the in-memory test pool.
var ErrPoolExhausted = fmt.Errorf("pool exhausted: out of blocks")

// FakeClock is a manually controlled Clock.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock creates a FakeClock at a fixed base time.
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t}
}

// Now implements Clock.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// FakeStore is an in-memory Store.
type FakeStore struct {
	mu      sync.Mutex
	blocks  map[string]*BlockState
	delLogs []string
}

// NewFakeStore creates an empty FakeStore.
func NewFakeStore() *FakeStore {
	return &FakeStore{blocks: make(map[string]*BlockState)}
}

// Add inserts a block (test helper).
func (s *FakeStore) Add(b *BlockState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *b
	s.blocks[b.Name] = &cp
}

// Get returns a copy of the stored block, or nil.
func (s *FakeStore) Get(name string) *BlockState {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.blocks[name]
	if !ok {
		return nil
	}
	cp := *b
	return &cp
}

// ListBlocks implements Store.
func (s *FakeStore) ListBlocks(_ context.Context) ([]*BlockState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.blocks))
	for n := range s.blocks {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]*BlockState, 0, len(names))
	for _, n := range names {
		cp := *s.blocks[n]
		out = append(out, &cp)
	}
	return out, nil
}

// MarkPending implements Store.
func (s *FakeStore) MarkPending(_ context.Context, name, nodeName, nodeUID string, reclaimAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.blocks[name]
	if !ok {
		return fmt.Errorf("block %s not found", name)
	}
	if b.NodeName == "" {
		// Idempotent replay: already pending, do not reset reclaim-at.
		return nil
	}
	b.NodeName = ""
	b.ReclaimNode = nodeName
	b.ReclaimUID = nodeUID
	b.ReclaimAt = reclaimAt
	return nil
}

// Restore implements Store.
func (s *FakeStore) Restore(_ context.Context, name, nodeName, nodeUID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.blocks[name]
	if !ok {
		return fmt.Errorf("block %s not found", name)
	}
	if b.NodeName != "" {
		// Idempotent replay: already restored.
		return nil
	}
	b.NodeName = nodeName
	b.NodeUID = nodeUID
	b.ReclaimNode = ""
	b.ReclaimUID = ""
	b.ReclaimAt = time.Time{}
	return nil
}

// SetNodeUID implements Store.
func (s *FakeStore) SetNodeUID(_ context.Context, name, uid string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.blocks[name]
	if !ok {
		return fmt.Errorf("block %s not found", name)
	}
	if b.NodeUID == uid {
		return nil
	}
	b.NodeUID = uid
	return nil
}

// DeleteBlock implements Store. Idempotent.
func (s *FakeStore) DeleteBlock(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.blocks[name]; !ok {
		// NotFound is success: deleting twice equals deleting once.
		return nil
	}
	delete(s.blocks, name)
	s.delLogs = append(s.delLogs, name)
	return nil
}

// DeleteCount returns how many distinct deletions happened.
func (s *FakeStore) DeleteCount(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, d := range s.delLogs {
		if d == name {
			n++
		}
	}
	return n
}

// FakeNodes is an in-memory NodeLister.
type FakeNodes struct {
	mu    sync.Mutex
	nodes map[string]NodeInfo
}

// NewFakeNodes creates a FakeNodes.
func NewFakeNodes() *FakeNodes {
	return &FakeNodes{nodes: make(map[string]NodeInfo)}
}

// Set adds or replaces a node.
func (f *FakeNodes) Set(n NodeInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes[n.Name] = n
}

// Remove deletes a node.
func (f *FakeNodes) Remove(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.nodes, name)
}

// ListNodes implements NodeLister.
func (f *FakeNodes) ListNodes(_ context.Context) (map[string]NodeInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]NodeInfo, len(f.nodes))
	for k, v := range f.nodes {
		out[k] = v
	}
	return out, nil
}

// FakePods is an in-memory PodLister.
type FakePods struct {
	mu   sync.Mutex
	pods []PodInfo
}

// NewFakePods creates a FakePods.
func NewFakePods() *FakePods {
	return &FakePods{}
}

// Set replaces the whole pod list (test helper).
func (f *FakePods) Set(pods ...PodInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pods = append([]PodInfo(nil), pods...)
}

// ListPods implements PodLister.
func (f *FakePods) ListPods(_ context.Context) ([]PodInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]PodInfo(nil), f.pods...), nil
}

// memPool is an in-memory replica of the pool allocation semantics of
// pkg/ipam/pool.go: the smallest free index is chosen under a lock, and any
// block still present in the store (including PendingReclaim blocks) counts
// as occupied.
type memPool struct {
	mu      sync.Mutex
	pool    string
	total   int
	store   *FakeStore
	nextSeq int
}

func newMemPool(pool string, total int, store *FakeStore) *memPool {
	return &memPool{pool: pool, total: total, store: store}
}

func blockIndex(name string) (int, error) {
	i := strings.LastIndex(name, "-")
	if i < 0 {
		return 0, fmt.Errorf("bad block name %q", name)
	}
	return strconv.Atoi(name[i+1:])
}

// Allocate gives the node the smallest free block index, or
// ErrPoolExhausted. Same state + same call order always yields the same
// index.
func (p *memPool) Allocate(node string) (*BlockState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	blocks, err := p.store.ListBlocks(context.Background())
	if err != nil {
		return nil, err
	}
	used := make(map[int]bool)
	for _, b := range blocks {
		if b.Pool != p.pool {
			continue
		}
		idx, err := blockIndex(b.Name)
		if err != nil {
			return nil, err
		}
		used[idx] = true
	}

	for idx := 0; idx < p.total; idx++ {
		if used[idx] {
			continue
		}
		b := &BlockState{
			Name:     fmt.Sprintf("%s-%d", p.pool, idx),
			Pool:     p.pool,
			NodeName: node,
			NodeUID:  node + "-uid",
			IPv4:     fmt.Sprintf("10.0.%d.0/31", idx),
		}
		p.store.Add(b)
		p.nextSeq++
		return b, nil
	}
	return nil, ErrPoolExhausted
}

// assertSingleOwnership fails the test if any block index is owned by more
// than one owner at the same time.
func assertSingleOwnership(t testTB, store *FakeStore) {
	t.Helper()
	blocks, err := store.ListBlocks(context.Background())
	if err != nil {
		t.Fatalf("list blocks: %v", err)
	}
	seen := make(map[int]string)
	for _, b := range blocks {
		idx, err := blockIndex(b.Name)
		if err != nil {
			t.Fatalf("bad block name: %v", err)
		}
		owner := b.Owner()
		if prev, ok := seen[idx]; ok && prev != owner {
			t.Fatalf("block index %d has two owners at once: %q and %q", idx, prev, owner)
		}
		seen[idx] = owner
	}
}

// testTB is the subset of testing.T used by helpers.
type testTB interface {
	Helper()
	Fatalf(format string, args ...any)
	Errorf(format string, args ...any)
}
