package blockgc

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
)

func newTestCollector(t *testing.T, grace time.Duration) (*Collector, *FakeStore, *FakeNodes, *FakePods, *FakeClock) {
	t.Helper()
	base := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	store := NewFakeStore()
	nodes := NewFakeNodes()
	pods := NewFakePods()
	clock := NewFakeClock(base)
	c := NewCollector(store, nodes, pods, clock, grace)
	return c, store, nodes, pods, clock
}

func allocatedBlock(pool, node string, idx int, uid string) *BlockState {
	return &BlockState{
		Name:     blockName(pool, idx),
		Pool:     pool,
		NodeName: node,
		NodeUID:  uid,
		IPv4:     "10.0.0.0/31",
	}
}

func blockName(pool string, idx int) string {
	return pool + "-" + itoa(idx)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

// Scenario 1: normal node deletion with no pods.
// Allocated -> PendingReclaim -> (grace elapsed) deleted; blocks are not
// allocatable while pending.
func TestSweep_NormalDeleteNoPods(t *testing.T) {
	ctx := context.Background()
	c, store, nodes, _, clock := newTestCollector(t, 5*time.Minute)
	const grace = 5 * time.Minute

	nodes.Set(NodeInfo{Name: "node-a", UID: "uid-a"})
	store.Add(allocatedBlock("p", "node-a", 0, "uid-a"))
	pool := newMemPool("p", 3, store)

	// Before deletion: node present, block stays allocated.
	if res, err := c.Sweep(ctx); err != nil || res.MarkedPending != 0 {
		t.Fatalf("unexpected sweep on live node: %+v, err=%v", res, err)
	}
	assertSingleOwnership(t, store)

	// Node disappears.
	nodes.Remove("node-a")
	res, err := c.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.MarkedPending != 1 || res.Deleted != 0 {
		t.Fatalf("want 1 marked pending, 0 deleted, got %+v", res)
	}
	b := store.Get("p-0")
	if b == nil || b.NodeName != "" || b.ReclaimNode != "node-a" || b.ReclaimUID != "uid-a" {
		t.Fatalf("block not in PendingReclaim: %+v", b)
	}
	if !b.ReclaimAt.Equal(clock.Now().Add(grace)) {
		t.Fatalf("reclaim-at %v, want %v", b.ReclaimAt, clock.Now().Add(grace))
	}
	assertSingleOwnership(t, store)

	// Still within grace: allocation must not pick the pending block.
	if _, err := pool.Allocate("node-b"); err != nil {
		t.Fatalf("allocate during grace: %v", err)
	}
	if got := store.Get("p-1"); got == nil || got.NodeName != "node-b" {
		t.Fatalf("node-b must get p-1, not the pending p-0")
	}
	if store.Get("p-0").NodeName != "" {
		t.Fatalf("pending block must not be reassigned")
	}
	assertSingleOwnership(t, store)

	// Just before grace expiry: still held.
	clock.Advance(grace - time.Second)
	if res, err := c.Sweep(ctx); err != nil || res.Deleted != 0 || res.Held != 1 {
		t.Fatalf("block must be held before grace: %+v err=%v", res, err)
	}

	// Grace elapsed: deleted.
	clock.Advance(time.Second)
	if res, err := c.Sweep(ctx); err != nil || res.Deleted != 1 {
		t.Fatalf("block must be deleted after grace: %+v err=%v", res, err)
	}
	if store.Get("p-0") != nil {
		t.Fatalf("p-0 should be gone")
	}
	assertSingleOwnership(t, store)

	// Now the freed index is the smallest and gets reused.
	blk, err := pool.Allocate("node-b")
	if err != nil {
		t.Fatalf("reallocate freed block: %v", err)
	}
	if blk.Name != "p-0" || blk.NodeName != "node-b" {
		t.Fatalf("freed block should be reusable as p-0, got %+v", blk)
	}
}

// Scenario 2: node deleted while a running pod still uses its addresses.
func TestSweep_DeleteWithRunningPod(t *testing.T) {
	ctx := context.Background()
	c, store, nodes, pods, clock := newTestCollector(t, 5*time.Minute)

	nodes.Set(NodeInfo{Name: "node-a", UID: "uid-a"})
	store.Add(allocatedBlock("p", "node-a", 0, "uid-a"))
	nodes.Remove("node-a")

	if _, err := c.Sweep(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	clock.Advance(6 * time.Minute)

	// A live pod still references the dead node.
	pods.Set(PodInfo{NodeName: "node-a", PodIPs: []string{"10.0.0.1"}, Phase: corev1.PodRunning})
	if res, err := c.Sweep(ctx); err != nil || res.Deleted != 0 {
		t.Fatalf("must not delete with live pod: %+v err=%v", res, err)
	}
	if store.Get("p-0") == nil {
		t.Fatalf("block wrongly deleted while pod present")
	}

	// Terminal / terminating pods must not block reclamation.
	terminating := time.Now()
	pods.Set(
		PodInfo{NodeName: "node-a", PodIPs: []string{"10.0.0.1"}, Phase: corev1.PodSucceeded},
		PodInfo{NodeName: "node-a", PodIPs: []string{"10.0.0.2"}, Phase: corev1.PodRunning, DeletionTimestamp: &terminating},
		PodInfo{NodeName: "node-a", HostNetwork: true, PodIPs: []string{"10.0.0.3"}, Phase: corev1.PodRunning},
		PodInfo{NodeName: "node-a", Phase: corev1.PodRunning}, // no IP yet
	)
	if res, err := c.Sweep(ctx); err != nil || res.Deleted != 1 {
		t.Fatalf("only live non-terminal pods block deletion: %+v err=%v", res, err)
	}

	// Once all pods are gone, a subsequent sweep has nothing left to do.
	pods.Set()
	if res, err := c.Sweep(ctx); err != nil || res.Deleted != 0 {
		t.Fatalf("replay after deletion must be a no-op: %+v err=%v", res, err)
	}
}

// Scenario 3: same-named node returns. Same UID inherits, different UID does not.
func TestSweep_SameNameNodeReturns(t *testing.T) {
	ctx := context.Background()
	c, store, nodes, _, clock := newTestCollector(t, 5*time.Minute)

	nodes.Set(NodeInfo{Name: "node-a", UID: "uid-a"})
	store.Add(allocatedBlock("p", "node-a", 0, "uid-a"))

	// Disappearance -> pending.
	nodes.Remove("node-a")
	if _, err := c.Sweep(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	clock.Advance(2 * time.Minute)

	// Same name, different UID (new machine): no inheritance even after grace.
	nodes.Set(NodeInfo{Name: "node-a", UID: "uid-b"})
	clock.Advance(10 * time.Minute)
	if res, err := c.Sweep(ctx); err != nil || res.Restored != 0 || res.Deleted != 0 {
		t.Fatalf("different UID must not inherit and must block deletion: %+v err=%v", res, err)
	}
	b := store.Get("p-0")
	if b == nil || b.NodeName != "" {
		t.Fatalf("old block must stay PendingReclaim with new same-named node: %+v", b)
	}

	// New node can only get a fresh block, never the old one.
	pool := newMemPool("p", 3, store)
	blk, err := pool.Allocate("node-a")
	if err != nil {
		t.Fatalf("new node allocation: %v", err)
	}
	if blk.Name == "p-0" {
		t.Fatalf("new node must not receive the pending old block")
	}
	assertSingleOwnership(t, store)

	// New node disappears again and grace passes: old block eventually reclaimed.
	nodes.Remove("node-a")
	clock.Advance(10 * time.Minute)
	if res, err := c.Sweep(ctx); err != nil || res.Deleted != 1 {
		t.Fatalf("old block should be reclaimed once no node and no pods: %+v err=%v", res, err)
	}

	// Same UID return: inheritance restores Allocated and clears the wait.
	store2 := NewFakeStore()
	nodes2 := NewFakeNodes()
	pods2 := NewFakePods()
	clock2 := NewFakeClock(clock.Now())
	c2 := NewCollector(store2, nodes2, pods2, clock2, 5*time.Minute)
	nodes2.Set(NodeInfo{Name: "node-a", UID: "uid-a"})
	store2.Add(allocatedBlock("p", "node-a", 2, "uid-a"))
	nodes2.Remove("node-a")
	if _, err := c2.Sweep(ctx); err != nil {
		t.Fatalf("sweep2: %v", err)
	}
	clock2.Advance(time.Minute)
	nodes2.Set(NodeInfo{Name: "node-a", UID: "uid-a"})
	if res, err := c2.Sweep(ctx); err != nil || res.Restored != 1 {
		t.Fatalf("same UID must restore block: %+v err=%v", res, err)
	}
	b2 := store2.Get("p-2")
	if b2 == nil || b2.NodeName != "node-a" || b2.ReclaimAt != (time.Time{}) || b2.ReclaimUID != "" {
		t.Fatalf("block not fully restored: %+v", b2)
	}
	assertSingleOwnership(t, store2)

	// Legacy block without recorded UID never inherits.
	legacy := allocatedBlock("p", "node-a", 3, "")
	store2.Add(legacy)
	nodes2.Remove("node-a")
	if _, err := c2.Sweep(ctx); err != nil {
		t.Fatalf("sweep3: %v", err)
	}
	nodes2.Set(NodeInfo{Name: "node-a", UID: "uid-z"})
	if res, err := c2.Sweep(ctx); err != nil || res.Restored != 0 {
		t.Fatalf("legacy block must not be inherited: %+v err=%v", res, err)
	}
}

// Scenario 4: ten-block pool, node A owns three; B cannot touch them during
// the wait, and may take them only after they return to free.
func TestSweep_TenBlockIsolation(t *testing.T) {
	ctx := context.Background()
	c, store, nodes, _, clock := newTestCollector(t, 5*time.Minute)
	pool := newMemPool("p", 10, store)

	nodes.Set(NodeInfo{Name: "jia", UID: "uid-jia"})
	nodes.Set(NodeInfo{Name: "yi", UID: "uid-yi"})

	// Jia takes three blocks.
	for i := 0; i < 3; i++ {
		if _, err := pool.Allocate("jia"); err != nil {
			t.Fatalf("jia alloc %d: %v", i, err)
		}
	}
	for i := 0; i < 3; i++ {
		b := store.Get(blockName("p", i))
		if b == nil || b.NodeName != "jia" {
			t.Fatalf("block %d not owned by jia", i)
		}
	}

	// Jia deleted: three blocks enter waiting.
	nodes.Remove("jia")
	if _, err := c.Sweep(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	for i := 0; i < 3; i++ {
		b := store.Get(blockName("p", i))
		if b == nil || b.NodeName != "" || b.ReclaimNode != "jia" {
			t.Fatalf("block %d must be pending for jia", i)
		}
	}
	assertSingleOwnership(t, store)

	// Yi scales out: may only get the other seven free blocks.
	for i := 0; i < 7; i++ {
		blk, err := pool.Allocate("yi")
		if err != nil {
			t.Fatalf("yi alloc %d: %v", i, err)
		}
		if blk.Name == "p-0" || blk.Name == "p-1" || blk.Name == "p-2" {
			t.Fatalf("yi must not receive jia's pending block %s", blk.Name)
		}
	}
	if _, err := pool.Allocate("yi"); err != ErrPoolExhausted {
		t.Fatalf("pool must be exhausted for yi while jia's blocks wait, got err=%v", err)
	}
	assertSingleOwnership(t, store)

	// Wait ends: jia's three blocks return to free.
	clock.Advance(6 * time.Minute)
	if res, err := c.Sweep(ctx); err != nil || res.Deleted != 3 {
		t.Fatalf("three jia blocks should be reclaimed: %+v err=%v", res, err)
	}

	// Yi can now apply and receive the returned blocks.
	for i := 0; i < 3; i++ {
		blk, err := pool.Allocate("yi")
		if err != nil {
			t.Fatalf("yi post-grace alloc %d: %v", i, err)
		}
		if blk.Name != "p-0" && blk.Name != "p-1" && blk.Name != "p-2" {
			t.Fatalf("yi must get a returned block, got %s", blk.Name)
		}
	}
	assertSingleOwnership(t, store)
}

// Scenario 5: two nodes contend for the last free block.
func TestAllocate_ContentionForLastBlock(t *testing.T) {
	store := NewFakeStore()
	pool := newMemPool("p", 2, store)

	if _, err := pool.Allocate("node-a"); err != nil {
		t.Fatalf("first alloc: %v", err)
	}

	// Sequential locking makes the winner deterministic: the first caller
	// wins, the second gets an explicit exhaustion error, never a busy block.
	blk, err := pool.Allocate("node-b")
	if err != nil {
		t.Fatalf("second alloc: %v", err)
	}
	if blk.Name != "p-1" {
		t.Fatalf("unexpected block %s", blk.Name)
	}
	if _, err := pool.Allocate("node-c"); err != ErrPoolExhausted {
		t.Fatalf("loser must get ErrPoolExhausted, got %v", err)
	}
	assertSingleOwnership(t, store)

	// Determinism: same state, same request order, same result on a fresh pool.
	store2 := NewFakeStore()
	pool2 := newMemPool("p", 2, store2)
	var names []string
	for _, n := range []string{"node-a", "node-b"} {
		b, err := pool2.Allocate(n)
		if err != nil {
			t.Fatalf("determinism alloc: %v", err)
		}
		names = append(names, b.Name)
	}
	want := []string{"p-0", "p-1"}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("non-deterministic allocation: got %v want %v", names, want)
		}
	}
}

// Scenario 6: node takes a second block as it grows.
func TestAllocate_SecondBlockForNode(t *testing.T) {
	store := NewFakeStore()
	pool := newMemPool("p", 4, store)

	first, err := pool.Allocate("node-a")
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := pool.Allocate("node-a")
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if first.Name == second.Name {
		t.Fatalf("second block must differ")
	}
	if second.Name != "p-1" {
		t.Fatalf("want p-1, got %s", second.Name)
	}
	for _, b := range []string{"p-0", "p-1"} {
		if got := store.Get(b); got == nil || got.NodeName != "node-a" {
			t.Fatalf("%s not owned by node-a", b)
		}
	}
}

// Scenario 8 (clock boundaries): assertions immediately before and after the
// grace timestamp.
func TestSweep_GraceBoundary(t *testing.T) {
	ctx := context.Background()
	const grace = 5 * time.Minute
	c, store, nodes, _, clock := newTestCollector(t, grace)

	nodes.Set(NodeInfo{Name: "node-a", UID: "uid-a"})
	store.Add(allocatedBlock("p", "node-a", 0, "uid-a"))
	nodes.Remove("node-a")
	if _, err := c.Sweep(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	clock.Advance(grace)
	// Exactly at reclaim-at: now.Before(reclaimAt) is false, so deletable.
	if res, err := c.Sweep(ctx); err != nil || res.Deleted != 1 {
		t.Fatalf("at grace boundary block must be deletable: %+v err=%v", res, err)
	}

	// Second block: one nanosecond before boundary stays held.
	store.Add(allocatedBlock("p", "node-a", 1, "uid-a"))
	if _, err := c.Sweep(ctx); err != nil {
		t.Fatalf("sweep2: %v", err)
	}
	clock.Advance(grace - time.Nanosecond)
	if res, err := c.Sweep(ctx); err != nil || res.Held != 1 || res.Deleted != 0 {
		t.Fatalf("one ns before grace must hold: %+v err=%v", res, err)
	}
	clock.Advance(time.Nanosecond)
	if res, err := c.Sweep(ctx); err != nil || res.Deleted != 1 {
		t.Fatalf("at grace must delete: %+v err=%v", res, err)
	}
}

// Scenario 9: idempotency and simulated restart (collector rebuilt from store).
func TestSweep_IdempotentAndRestartSafe(t *testing.T) {
	ctx := context.Background()
	c, store, nodes, pods, clock := newTestCollector(t, 5*time.Minute)

	nodes.Set(NodeInfo{Name: "node-a", UID: "uid-a"})
	store.Add(allocatedBlock("p", "node-a", 0, "uid-a"))
	nodes.Remove("node-a")

	first, err := c.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	reclaimAt := store.Get("p-0").ReclaimAt

	// Replaying the same transition must not reset reclaim-at nor double-mark.
	second, err := c.Sweep(ctx)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if second.MarkedPending != 0 {
		t.Fatalf("replay must not mark again: %+v", second)
	}
	if got := store.Get("p-0").ReclaimAt; !got.Equal(reclaimAt) {
		t.Fatalf("reclaim-at changed on replay: %v vs %v", got, reclaimAt)
	}
	if first.MarkedPending != 1 {
		t.Fatalf("first sweep should mark once")
	}

	// Simulate controller restart: new collector over the same store.
	c2 := NewCollector(store, nodes, pods, clock, 5*time.Minute)
	clock.Advance(6 * time.Minute)
	if res, err := c2.Sweep(ctx); err != nil || res.Deleted != 1 {
		t.Fatalf("restarted collector must converge: %+v err=%v", res, err)
	}

	// Double delete is equivalent to single delete.
	if err := store.DeleteBlock(ctx, "p-0"); err != nil {
		t.Fatalf("second delete must be a no-op: %v", err)
	}
	if n := store.DeleteCount("p-0"); n != 1 {
		t.Fatalf("block must be deleted exactly once, got %d", n)
	}
}

// Scenario 10: cordon / eviction (Node object still present) never reclaims.
func TestSweep_NodeStillPresentIsNotDeletion(t *testing.T) {
	ctx := context.Background()
	c, store, nodes, _, clock := newTestCollector(t, time.Minute)

	node := NodeInfo{Name: "node-a", UID: "uid-a"}
	nodes.Set(node)
	store.Add(allocatedBlock("p", "node-a", 0, "uid-a"))

	// Cordon / eviction do not remove the Node object. Sweep repeatedly;
	// nothing must happen regardless of elapsed time.
	for i := 0; i < 5; i++ {
		clock.Advance(10 * time.Minute)
		if res, err := c.Sweep(ctx); err != nil || res.MarkedPending != 0 || res.Deleted != 0 {
			t.Fatalf("live node blocks must never be reclaimed: %+v err=%v", res, err)
		}
		if b := store.Get("p-0"); b == nil || b.NodeName != "node-a" {
			t.Fatalf("block lost while node still exists")
		}
	}
}

// Grace period 0: a block is still persisted as PendingReclaim and can only
// be deleted on a LATER sweep — reclamation never happens within the same
// sweep that first observes the node disappearance.
func TestSweep_ZeroGraceStillCrossesSweep(t *testing.T) {
	ctx := context.Background()
	c, store, nodes, _, _ := newTestCollector(t, 0)

	nodes.Set(NodeInfo{Name: "node-a", UID: "uid-a"})
	store.Add(allocatedBlock("p", "node-a", 0, "uid-a"))
	nodes.Remove("node-a")

	// First sweep: marked pending, never deleted in the same pass.
	res, err := c.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.MarkedPending != 1 || res.Deleted != 0 {
		t.Fatalf("zero grace must still mark pending first: %+v", res)
	}
	if store.Get("p-0") == nil {
		t.Fatalf("block must survive the discovery sweep even with zero grace")
	}

	// Second sweep (no clock advance needed): now deletable.
	res, err = c.Sweep(ctx)
	if err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	if res.Deleted != 1 || store.Get("p-0") != nil {
		t.Fatalf("block should be reclaimed on the next sweep, res=%+v", res)
	}
}

// Reserved migration blocks are excluded from node-deletion reclamation.
func TestSweep_ReservedBlocksIgnored(t *testing.T) {
	ctx := context.Background()
	c, store, nodes, _, _ := newTestCollector(t, time.Minute)

	store.Add(&BlockState{Name: "p-9", Pool: "p", Reserved: true})
	nodes.Remove("anything")
	if res, err := c.Sweep(ctx); err != nil || res.MarkedPending != 0 || res.Deleted != 0 {
		t.Fatalf("reserved block must be untouched: %+v err=%v", res, err)
	}
	if store.Get("p-9") == nil {
		t.Fatalf("reserved block deleted")
	}
}

// UID backfill on legacy blocks while the node still exists.
func TestSweep_BackfillsNodeUID(t *testing.T) {
	ctx := context.Background()
	c, store, nodes, _, _ := newTestCollector(t, time.Minute)

	nodes.Set(NodeInfo{Name: "node-a", UID: "uid-a"})
	store.Add(allocatedBlock("p", "node-a", 0, "")) // legacy, no UID

	res, err := c.Sweep(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if res.BackfilledUID != 1 {
		t.Fatalf("want 1 backfill, got %+v", res)
	}
	if got := store.Get("p-0").NodeUID; got != "uid-a" {
		t.Fatalf("uid not backfilled: %q", got)
	}
	// Idempotent: no repeated backfill.
	res, _ = c.Sweep(ctx)
	if res.BackfilledUID != 0 {
		t.Fatalf("backfill must not repeat: %+v", res)
	}
}

// Pod list failure semantics: when pods cannot be listed, nothing is deleted
// even if the grace period has elapsed. (Exercised via an erroring lister.)
func TestSweep_PodListFailureBlocksDeletion(t *testing.T) {
	ctx := context.Background()
	store := NewFakeStore()
	nodes := NewFakeNodes()
	clock := NewFakeClock(time.Now())
	failing := &errorPods{}
	c := NewCollector(store, nodes, failing, clock, time.Minute)

	nodes.Set(NodeInfo{Name: "node-a", UID: "uid-a"})
	store.Add(allocatedBlock("p", "node-a", 0, "uid-a"))
	nodes.Remove("node-a")
	store.MarkPending(ctx, "p-0", "node-a", "uid-a", clock.Now().Add(-time.Minute))

	if _, err := c.Sweep(ctx); err == nil {
		t.Fatalf("pod list error must abort the sweep")
	}
	if store.Get("p-0") == nil {
		t.Fatalf("block must survive a failed pod listing")
	}
}

type errorPods struct{}

func (errorPods) ListPods(_ context.Context) ([]PodInfo, error) {
	return nil, errPodList
}

var errPodList = &sentinelErr{"pod list failed"}

type sentinelErr struct{ msg string }

func (e *sentinelErr) Error() string { return e.msg }
