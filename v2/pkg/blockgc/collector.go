// Package blockgc implements the delayed reclamation of AddressBlocks whose
// owning Node has disappeared.
//
// The collector is deliberately storage-agnostic: all external state is
// accessed through small interfaces so that the whole state machine can be
// exercised with in-memory fakes and a controllable clock.  The state of a
// block is derived solely from its labels/annotations (see
// docs/address-block-gc.md), which makes every Sweep idempotent and safe to
// replay after controller restarts.
package blockgc

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Block states. See docs/address-block-gc.md section 2.
const (
	// StateAllocated means the block has a node label and the node exists.
	StateAllocated = "Allocated"
	// StatePendingReclaim means the node disappeared; the block is kept for
	// the grace period and cannot be handed out to another node.
	StatePendingReclaim = "PendingReclaim"
	// StateReserved blocks are migration leftovers and are never reclaimed.
	StateReserved = "Reserved"
)

// BlockState is the storage-neutral projection of an AddressBlock.
type BlockState struct {
	// Name is the AddressBlock object name.
	Name string
	// Pool is the pool label value.
	Pool string
	// NodeName is the node label value. Empty when the block is in
	// PendingReclaim (the label has been removed).
	NodeName string
	// NodeUID is the AnnNodeUID annotation, empty for legacy blocks.
	NodeUID string
	// Reserved mirrors the coil.cybozu.com/reserved label.
	Reserved bool
	// ReclaimAt is zero unless the block is in PendingReclaim.
	ReclaimAt time.Time
	// ReclaimNode / ReclaimUID carry the identity of the disappeared node
	// while the block is in PendingReclaim.
	ReclaimNode string
	ReclaimUID  string
	// IPv4 / IPv6 are the block CIDRs (either may be empty).
	IPv4 string
	IPv6 string
}

// Owner returns the current owner identity of the block: either the live
// node name, or "pending-reclaim:<original node name>".  It is used by tests
// to assert that an address range never has two owners at once.
func (b *BlockState) Owner() string {
	if b.Reserved {
		return "reserved"
	}
	if b.NodeName != "" {
		return b.NodeName
	}
	return "pending-reclaim:" + b.ReclaimNode
}

// NodeInfo is the storage-neutral projection of a Node.
type NodeInfo struct {
	Name string
	UID  string
}

// PodInfo is the storage-neutral projection of a Pod, reduced to the fields
// needed for the occupancy decision.
type PodInfo struct {
	NodeName          string
	HostNetwork       bool
	PodIPs            []string
	Phase             corev1.PodPhase
	DeletionTimestamp *time.Time
}

// occupiesBlock reports whether the pod still holds an address carved from
// one of the node's blocks.  Terminating and terminal pods are releasing
// (or have released) their IP and must not block reclamation forever.
func (p PodInfo) occupiesBlock() bool {
	if p.HostNetwork {
		return false
	}
	if len(p.PodIPs) == 0 {
		return false
	}
	if p.DeletionTimestamp != nil {
		return false
	}
	if p.Phase == corev1.PodSucceeded || p.Phase == corev1.PodFailed {
		return false
	}
	return true
}

// Clock allows tests to advance time deterministically.
type Clock interface {
	Now() time.Time
}

// SystemClock is the production Clock.
type SystemClock struct{}

// Now implements Clock.
func (SystemClock) Now() time.Time { return time.Now() }

// Store abstracts AddressBlock persistence.
type Store interface {
	// ListBlocks returns all AddressBlocks.
	ListBlocks(ctx context.Context) ([]*BlockState, error)
	// MarkPending moves an Allocated block to PendingReclaim: the node label
	// is removed and the reclaim annotations are written in one atomic
	// mutation.  reclaimUID may be empty for legacy blocks.
	MarkPending(ctx context.Context, name, nodeName, nodeUID string, reclaimAt time.Time) error
	// Restore moves a PendingReclaim block back to Allocated for a returned
	// same-named, same-UID node.
	Restore(ctx context.Context, name, nodeName, nodeUID string) error
	// SetNodeUID backfills the node-uid annotation on a legacy block whose
	// node still exists.
	SetNodeUID(ctx context.Context, name, uid string) error
	// DeleteBlock removes the finalizer and deletes the block. It must be
	// idempotent: deleting an already-gone block is not an error.
	DeleteBlock(ctx context.Context, name string) error
}

// NodeLister lists existing nodes keyed by name.
type NodeLister interface {
	ListNodes(ctx context.Context) (map[string]NodeInfo, error)
}

// PodLister lists all pods.
type PodLister interface {
	ListPods(ctx context.Context) ([]PodInfo, error)
}

// SweepResult summarizes what one Sweep did, mainly for logs and tests.
type SweepResult struct {
	MarkedPending int
	Restored      int
	Deleted       int
	// Held counts PendingReclaim blocks that were not deleted this round
	// (grace not elapsed, pods still present, or a different-UID same-named
	// node returned).
	Held          int
	BackfilledUID int
}

// Collector runs one reclamation sweep per Sweep call.
type Collector struct {
	store       Store
	nodes       NodeLister
	pods        PodLister
	clock       Clock
	gracePeriod time.Duration
}

// NewCollector creates a Collector.  gracePeriod == 0 means a block becomes
// deletable on the first sweep whose grace timestamp is reached; the block
// still passes through persisted PendingReclaim, so reclamation never
// happens within the same sweep that observes the node disappearance.
func NewCollector(store Store, nodes NodeLister, pods PodLister, clock Clock, gracePeriod time.Duration) *Collector {
	if clock == nil {
		clock = SystemClock{}
	}
	return &Collector{
		store:       store,
		nodes:       nodes,
		pods:        pods,
		clock:       clock,
		gracePeriod: gracePeriod,
	}
}

// Sweep performs one full reclamation pass. It is a pure function of the
// current storage state and the clock, so replaying it (also after a process
// restart) converges to the same result without side effects duplicated.
func (c *Collector) Sweep(ctx context.Context) (SweepResult, error) {
	var res SweepResult

	nodes, err := c.nodes.ListNodes(ctx)
	if err != nil {
		return res, err
	}

	pods, err := c.pods.ListPods(ctx)
	if err != nil {
		return res, err
	}
	occupying := make(map[string]int)
	for _, p := range pods {
		if p.occupiesBlock() {
			occupying[p.NodeName]++
		}
	}

	blocks, err := c.store.ListBlocks(ctx)
	if err != nil {
		return res, err
	}

	now := c.clock.Now()
	for _, b := range blocks {
		if b.Reserved {
			continue
		}

		if b.NodeName != "" {
			// StateAllocated.
			n, ok := nodes[b.NodeName]
			if ok {
				// Backfill node UID on legacy blocks while the node exists.
				if b.NodeUID == "" && n.UID != "" {
					if err := c.store.SetNodeUID(ctx, b.Name, n.UID); err != nil {
						return res, err
					}
					res.BackfilledUID++
				}
				continue
			}

			// Node disappeared -> PendingReclaim (T2).
			if err := c.store.MarkPending(ctx, b.Name, b.NodeName, b.NodeUID, now.Add(c.gracePeriod)); err != nil {
				return res, err
			}
			res.MarkedPending++
			continue
		}

		// StatePendingReclaim.
		res.Held++
		if n, ok := nodes[b.ReclaimNode]; ok {
			// A same-named node returned. Inheritance requires the same UID.
			// Legacy blocks without a recorded UID never inherit (T4).
			if b.ReclaimUID != "" && n.UID == b.ReclaimUID {
				if err := c.store.Restore(ctx, b.Name, b.ReclaimNode, n.UID); err != nil {
					return res, err
				}
				res.Restored++
				res.Held--
			}
			continue
		}

		if now.Before(b.ReclaimAt) {
			// Grace period not yet elapsed.
			continue
		}
		if occupying[b.ReclaimNode] > 0 {
			// Grace elapsed but pods still reference the dead node (T6).
			continue
		}

		if err := c.store.DeleteBlock(ctx, b.Name); err != nil {
			return res, err
		}
		res.Deleted++
		res.Held--
	}

	return res, nil
}
