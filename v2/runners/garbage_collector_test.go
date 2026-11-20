package runners

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	coilv2 "github.com/cybozu-go/coil/v2/api/v2"
	"github.com/cybozu-go/coil/v2/pkg/constants"
)

var (
	errBlockDeleted     = errors.New("block was deleted")
	errStillAllocated   = errors.New("block still has node label")
	errReclaimAtMissing = errors.New("block has no reclaim-at annotation yet")
)

var _ = Describe("Garbage collector", func() {
	ctx := context.Background()
	var cancel context.CancelFunc

	const (
		gcInterval    = 500 * time.Millisecond
		gcGracePeriod = 2 * time.Second
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.TODO())
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:         scheme,
			LeaderElection: false,
			Metrics: metricsserver.Options{
				BindAddress: "0",
			},
		})
		Expect(err).ToNot(HaveOccurred())

		gc := NewGarbageCollector(mgr, ctrl.Log.WithName("garbage collector"), gcInterval, gcGracePeriod)
		err = mgr.Add(gc)
		Expect(err).ToNot(HaveOccurred())

		go func() {
			err := mgr.Start(ctx)
			if err != nil {
				panic(err)
			}
		}()
	})

	AfterEach(func() {
		deleteAllAddressBlocks()
		cancel()
		pods := &corev1.PodList{}
		Expect(k8sClient.List(context.Background(), pods)).To(Succeed())
		for i := range pods.Items {
			_ = k8sClient.Delete(context.Background(), &pods.Items[i])
		}
		Expect(k8sClient.DeleteAllOf(context.Background(), &coilv2.BlockRequest{})).To(Succeed())
		time.Sleep(10 * time.Millisecond)
	})

	getBlock := func(name string) *coilv2.AddressBlock {
		b := &coilv2.AddressBlock{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name}, b)
		if err != nil {
			return nil
		}
		return b
	}

	It("delays reclamation through PendingReclaim and only frees blocks after the grace period", func() {
		block := &coilv2.AddressBlock{}
		block.Name = "default-0"
		block.Index = 0
		block.Labels = map[string]string{
			constants.LabelPool: "default",
			constants.LabelNode: "ghost-node",
		}
		Expect(k8sClient.Create(ctx, block)).To(Succeed())

		// First sweep: node label removed, reclaim annotations written, block kept.
		Eventually(func() error {
			b := getBlock("default-0")
			if b == nil {
				return errBlockDeleted
			}
			if _, ok := b.Labels[constants.LabelNode]; ok {
				return errStillAllocated
			}
			if b.Annotations[constants.AnnReclaimAt] == "" {
				return errReclaimAtMissing
			}
			return nil
		}, "3s", "100ms").Should(Succeed())

		b := getBlock("default-0")
		Expect(b).NotTo(BeNil())
		Expect(b.Labels).NotTo(HaveKey(constants.LabelNode))
		Expect(b.Annotations[constants.AnnReclaimNode]).To(Equal("ghost-node"))
		reclaimAt := b.Annotations[constants.AnnReclaimAt]
		Expect(reclaimAt).NotTo(BeEmpty())

		// It must survive beyond one scan interval while inside the grace window.
		Consistently(func() *coilv2.AddressBlock {
			return getBlock("default-0")
		}, gcGracePeriod-gcInterval, gcInterval/2).ShouldNot(BeNil())

		// After the grace period the block is deleted.
		Eventually(func() *coilv2.AddressBlock {
			return getBlock("default-0")
		}, "5s", "200ms").Should(BeNil())
	})

	It("keeps blocks of a node that still exists, including cordoned nodes", func() {
		block := &coilv2.AddressBlock{}
		block.Name = "default-1"
		block.Index = 1
		block.Labels = map[string]string{
			constants.LabelPool: "default",
			constants.LabelNode: "node1",
		}
		Expect(k8sClient.Create(ctx, block)).To(Succeed())

		// node1 exists in the envtest suite (see suite_test.go).
		node := &corev1.Node{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "node1"}, node)).To(Succeed())
		node.Spec.Unschedulable = true // cordon
		Expect(k8sClient.Update(ctx, node)).To(Succeed())

		Consistently(func() string {
			b := getBlock("default-1")
			if b == nil {
				return "deleted"
			}
			return b.Labels[constants.LabelNode]
		}, gcGracePeriod*2, gcInterval).Should(Equal("node1"))
	})

	It("does not reclaim while a running pod still references the dead node", func() {
		block := &coilv2.AddressBlock{}
		block.Name = "default-2"
		block.Index = 2
		block.Labels = map[string]string{
			constants.LabelPool: "default",
			constants.LabelNode: "pod-ghost",
		}
		Expect(k8sClient.Create(ctx, block)).To(Succeed())

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "lingering", Namespace: "default"},
			Spec: corev1.PodSpec{
				NodeName: "pod-ghost",
				Containers: []corev1.Container{
					{Name: "c", Image: "nginx"},
				},
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIPs = []corev1.PodIP{{IP: "10.9.9.9"}}
		pod.Status.Phase = corev1.PodRunning
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

		// Enters pending but is never deleted while the pod runs.
		Eventually(func() bool {
			b := getBlock("default-2")
			return b != nil && b.Labels[constants.LabelNode] == ""
		}, "3s", "100ms").Should(BeTrue())

		Consistently(func() *coilv2.AddressBlock {
			return getBlock("default-2")
		}, gcGracePeriod*2, gcInterval).ShouldNot(BeNil())

		// Pod removed -> block reclaimed on a later sweep.
		Expect(k8sClient.Delete(ctx, pod)).To(Succeed())
		Eventually(func() *coilv2.AddressBlock {
			return getBlock("default-2")
		}, "5s", "200ms").Should(BeNil())
	})

	It("restores a pending block only when a same-named node returns with the same UID", func() {
		// First node registration.
		nodeA := &corev1.Node{}
		nodeA.Name = "flaky-node"
		Expect(k8sClient.Create(ctx, nodeA)).To(Succeed())
		uidA := nodeA.UID
		Expect(uidA).NotTo(BeEmpty())

		block := &coilv2.AddressBlock{}
		block.Name = "default-3"
		block.Index = 3
		block.Labels = map[string]string{
			constants.LabelPool: "default",
			constants.LabelNode: "flaky-node",
		}
		block.Annotations = map[string]string{constants.AnnNodeUID: string(uidA)}
		Expect(k8sClient.Create(ctx, block)).To(Succeed())

		// Node disappears -> pending.
		Expect(k8sClient.Delete(ctx, nodeA)).To(Succeed())
		Eventually(func() map[string]string {
			b := getBlock("default-3")
			if b == nil {
				return nil
			}
			return b.Annotations
		}, "3s", "100ms").Should(HaveKey(constants.AnnReclaimAt))

		// Same-named node re-registers. envtest assigns a fresh UID.
		nodeB := &corev1.Node{}
		nodeB.Name = "flaky-node"
		Expect(k8sClient.Create(ctx, nodeB)).To(Succeed())
		Expect(nodeB.UID).NotTo(Equal(uidA))

		// Different UID: the block must NOT be restored.
		Consistently(func() string {
			b := getBlock("default-3")
			if b == nil {
				return "deleted"
			}
			return b.Labels[constants.LabelNode]
		}, gcGracePeriod*2, gcInterval).Should(Equal(""))

		// New node disappears, grace passes with no pods -> reclaimed.
		Expect(k8sClient.Delete(ctx, nodeB)).To(Succeed())
		Eventually(func() *coilv2.AddressBlock {
			return getBlock("default-3")
		}, "5s", "200ms").Should(BeNil())
	})
})
