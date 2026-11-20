package runners

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	coilv2 "github.com/cybozu-go/coil/v2/api/v2"
	"github.com/cybozu-go/coil/v2/pkg/blockgc"
	"github.com/cybozu-go/coil/v2/pkg/constants"
)

// NewGarbageCollector creates a manager.Runnable to reclaim AddressBlocks
// of deleted nodes.
//
// Reclamation is delayed: a block whose Node has disappeared first enters
// the persisted PendingReclaim state and is only deleted after gracePeriod
// elapses and no Pod still references the node.  A same-named node inherits
// the block only if its UID matches.  See docs/address-block-gc.md.
func NewGarbageCollector(mgr manager.Manager, log logr.Logger, interval, gracePeriod time.Duration) manager.Runnable {
	return &garbageCollector{
		Client:      mgr.GetClient(),
		apiReader:   mgr.GetAPIReader(),
		log:         log,
		interval:    interval,
		gracePeriod: gracePeriod,
	}
}

type garbageCollector struct {
	client.Client
	apiReader   client.Reader
	log         logr.Logger
	interval    time.Duration
	gracePeriod time.Duration
}

// +kubebuilder:rbac:groups=coil.cybozu.com,resources=addressblocks,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list

var _ manager.LeaderElectionRunnable = &garbageCollector{}

// NeedLeaderElection implements manager.LeaderElectionRunnable
func (*garbageCollector) NeedLeaderElection() bool {
	return true
}

// Start starts this runner.  This implements manager.Runnable
func (gc *garbageCollector) Start(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()

	collector := blockgc.NewCollector(
		&k8sBlockStore{gc: gc},
		&k8sNodeLister{gc: gc},
		&k8sPodLister{gc: gc},
		blockgc.SystemClock{},
		gc.gracePeriod,
	)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			res, err := collector.Sweep(context.Background())
			if err != nil {
				return err
			}
			gc.log.Info("garbage collection finished",
				"markedPending", res.MarkedPending,
				"restored", res.Restored,
				"deleted", res.Deleted,
				"held", res.Held,
				"backfilledUID", res.BackfilledUID,
			)
			timer.Reset(gc.interval)
		}
	}
}

// --- blockgc.NodeLister ---

type k8sNodeLister struct{ gc *garbageCollector }

func (l *k8sNodeLister) ListNodes(ctx context.Context) (map[string]blockgc.NodeInfo, error) {
	nodes := &corev1.NodeList{}
	if err := l.gc.apiReader.List(ctx, nodes); err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}
	out := make(map[string]blockgc.NodeInfo, len(nodes.Items))
	for _, n := range nodes.Items {
		out[n.Name] = blockgc.NodeInfo{Name: n.Name, UID: string(n.UID)}
	}
	return out, nil
}

// --- blockgc.PodLister ---

type k8sPodLister struct{ gc *garbageCollector }

func (l *k8sPodLister) ListPods(ctx context.Context) ([]blockgc.PodInfo, error) {
	pods := &corev1.PodList{}
	if err := l.gc.apiReader.List(ctx, pods); err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}
	out := make([]blockgc.PodInfo, 0, len(pods.Items))
	for i := range pods.Items {
		p := &pods.Items[i]
		info := blockgc.PodInfo{
			NodeName:    p.Spec.NodeName,
			HostNetwork: p.Spec.HostNetwork,
			Phase:       p.Status.Phase,
		}
		for _, ip := range p.Status.PodIPs {
			info.PodIPs = append(info.PodIPs, ip.IP)
		}
		if p.DeletionTimestamp != nil {
			t := p.DeletionTimestamp.Time
			info.DeletionTimestamp = &t
		}
		out = append(out, info)
	}
	return out, nil
}

// --- blockgc.Store ---

type k8sBlockStore struct{ gc *garbageCollector }

func (s *k8sBlockStore) ListBlocks(ctx context.Context) ([]*blockgc.BlockState, error) {
	blocks := &coilv2.AddressBlockList{}
	// Use the API reader: the garbage collector registers no informer for
	// AddressBlocks, so the manager cache would serve empty/stale lists.
	if err := s.gc.apiReader.List(ctx, blocks); err != nil {
		return nil, fmt.Errorf("failed to list address blocks: %w", err)
	}
	out := make([]*blockgc.BlockState, 0, len(blocks.Items))
	for i := range blocks.Items {
		b := &blocks.Items[i]
		st := &blockgc.BlockState{
			Name:     b.Name,
			Pool:     b.Labels[constants.LabelPool],
			NodeName: b.Labels[constants.LabelNode],
			NodeUID:  b.Annotations[constants.AnnNodeUID],
			Reserved: b.Labels[constants.LabelReserved] == "true",
		}
		if b.IPv4 != nil {
			st.IPv4 = *b.IPv4
		}
		if b.IPv6 != nil {
			st.IPv6 = *b.IPv6
		}
		if v := b.Annotations[constants.AnnReclaimAt]; v != "" {
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return nil, fmt.Errorf("block %s has invalid %s annotation: %w", b.Name, constants.AnnReclaimAt, err)
			}
			st.ReclaimAt = t
			st.ReclaimNode = b.Annotations[constants.AnnReclaimNode]
			st.ReclaimUID = b.Annotations[constants.AnnReclaimUID]
		}
		out = append(out, st)
	}
	return out, nil
}

// mutateBlock retries conflicts while applying fn to the fresh block.
func (s *k8sBlockStore) mutateBlock(ctx context.Context, name string, fn func(*coilv2.AddressBlock)) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		b := &coilv2.AddressBlock{}
		if err := s.gc.apiReader.Get(ctx, client.ObjectKey{Name: name}, b); err != nil {
			return client.IgnoreNotFound(err)
		}
		fn(b)
		return s.gc.Update(ctx, b)
	})
}

func (s *k8sBlockStore) MarkPending(ctx context.Context, name, nodeName, nodeUID string, reclaimAt time.Time) error {
	err := s.mutateBlock(ctx, name, func(b *coilv2.AddressBlock) {
		// Idempotent: already pending. Keep the original reclaim-at.
		if _, ok := b.Annotations[constants.AnnReclaimAt]; ok {
			return
		}
		delete(b.Labels, constants.LabelNode)
		if b.Annotations == nil {
			b.Annotations = make(map[string]string)
		}
		b.Annotations[constants.AnnReclaimNode] = nodeName
		b.Annotations[constants.AnnReclaimUID] = nodeUID
		b.Annotations[constants.AnnReclaimAt] = reclaimAt.UTC().Format(time.RFC3339)
	})
	if err != nil {
		return fmt.Errorf("failed to mark block %s pending: %w", name, err)
	}
	s.gc.log.Info("marked block for delayed reclamation", "block", name, "node", nodeName, "reclaimAt", reclaimAt)
	return nil
}

func (s *k8sBlockStore) Restore(ctx context.Context, name, nodeName, nodeUID string) error {
	err := s.mutateBlock(ctx, name, func(b *coilv2.AddressBlock) {
		// Idempotent: already restored (node label present, no reclaim-at).
		if _, ok := b.Annotations[constants.AnnReclaimAt]; !ok {
			return
		}
		if b.Labels == nil {
			b.Labels = make(map[string]string)
		}
		b.Labels[constants.LabelNode] = nodeName
		if b.Annotations == nil {
			b.Annotations = make(map[string]string)
		}
		b.Annotations[constants.AnnNodeUID] = nodeUID
		delete(b.Annotations, constants.AnnReclaimNode)
		delete(b.Annotations, constants.AnnReclaimUID)
		delete(b.Annotations, constants.AnnReclaimAt)
	})
	if err != nil {
		return fmt.Errorf("failed to restore block %s: %w", name, err)
	}
	s.gc.log.Info("restored block for returned same-named node", "block", name, "node", nodeName)
	return nil
}

func (s *k8sBlockStore) SetNodeUID(ctx context.Context, name, uid string) error {
	err := s.mutateBlock(ctx, name, func(b *coilv2.AddressBlock) {
		if b.Annotations == nil {
			b.Annotations = make(map[string]string)
		}
		if b.Annotations[constants.AnnNodeUID] == uid {
			return
		}
		b.Annotations[constants.AnnNodeUID] = uid
	})
	if err != nil {
		return fmt.Errorf("failed to set node UID on %s: %w", name, err)
	}
	return nil
}

func (s *k8sBlockStore) DeleteBlock(ctx context.Context, name string) error {
	// remove finalizer
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		b := &coilv2.AddressBlock{}
		err := s.gc.apiReader.Get(ctx, client.ObjectKey{Name: name}, b)
		if err != nil {
			return client.IgnoreNotFound(err)
		}
		if !controllerutil.ContainsFinalizer(b, constants.FinCoil) {
			return nil
		}
		controllerutil.RemoveFinalizer(b, constants.FinCoil)
		return s.gc.Update(ctx, b)
	})
	if err != nil {
		return fmt.Errorf("failed to remove finalizer from %s: %w", name, err)
	}

	// delete ignoring notfound error, so double reclamation equals one.
	b := &coilv2.AddressBlock{}
	b.Name = name
	if err := client.IgnoreNotFound(s.gc.Delete(ctx, b)); err != nil {
		return fmt.Errorf("failed to delete block %s: %w", name, err)
	}
	s.gc.log.Info("reclaimed an orphan block after grace period", "block", name)
	return nil
}
