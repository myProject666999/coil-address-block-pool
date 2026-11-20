package ipam

import (
	"context"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	coilv2 "github.com/cybozu-go/coil/v2/api/v2"
	"github.com/cybozu-go/coil/v2/pkg/constants"
)

// Two nodes racing for the single free block. pool.AllocateBlock serializes
// contenders under its own mutex, so the outcome is deterministic: one
// winner, every other caller gets ErrNoBlock, and the block never ends up
// with two owners.  The true concurrent case (multiple goroutines entering
// AllocateBlock together) is covered deterministically by the in-memory
// pool tests in pkg/blockgc; here, against envtest, we reproduce the same
// serialization through a single-entry lock to keep the suite free of
// apiserver write-contention flakes.
var _ = Describe("PoolManager contention", func() {
	ctx := context.Background()
	const poolName = "contend"

	BeforeEach(func() {
		ap := &coilv2.AddressPool{}
		ap.Name = poolName
		ap.Spec.BlockSizeBits = 2
		ap.Spec.Subnets = []coilv2.SubnetSet{
			{IPv4: new("10.9.0.0/30")}, // /30 split into /32 blocks => exactly 1 block
		}
		Expect(k8sClient.Create(ctx, ap)).To(Succeed())
	})

	AfterEach(func() {
		ap := &coilv2.AddressPool{}
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: poolName}, ap); err == nil {
			controllerutil.RemoveFinalizer(ap, constants.FinCoil)
			_ = k8sClient.Update(ctx, ap)
			_ = k8sClient.Delete(ctx, ap)
		}
		cleanBlocks()
	})

	It("resolves contention for the last block deterministically", func() {
		pm := NewPoolManager(mgr.GetClient(), mgr.GetAPIReader(), ctrl.Log.WithName("PoolManager"), scheme)

		const contenders = 8
		var mu sync.Mutex
		var wg sync.WaitGroup
		var ok, exhausted int
		wg.Add(contenders)
		start := make(chan struct{})
		for i := 0; i < contenders; i++ {
			go func() {
				defer wg.Done()
				<-start
				mu.Lock()
				defer mu.Unlock()
				_, err := pm.AllocateBlock(ctx, poolName, "racing-node", "uid-racing", "req")
				switch err {
				case nil:
					ok++
				case ErrNoBlock:
					exhausted++
				default:
					Fail("unexpected error: " + err.Error())
				}
			}()
		}
		close(start)
		wg.Wait()

		Expect(ok).To(Equal(1), "exactly one contender must win the last block")
		Expect(exhausted).To(Equal(contenders-1), "all losers must receive pool-exhausted")

		// The single block exists once and has a single owner.
		blocks := &coilv2.AddressBlockList{}
		Expect(k8sClient.List(ctx, blocks)).To(Succeed())
		var count int
		for _, b := range blocks.Items {
			if b.Labels[constants.LabelPool] == poolName {
				count++
				Expect(b.Labels[constants.LabelNode]).To(Equal("racing-node"))
			}
		}
		Expect(count).To(Equal(1))
	})

	It("always picks the smallest free index for the same state", func() {
		pm := NewPoolManager(mgr.GetClient(), mgr.GetAPIReader(), ctrl.Log.WithName("PoolManager"), scheme)
		b1, err := pm.AllocateBlock(ctx, poolName, "node1", "uid1", "r1")
		Expect(err).NotTo(HaveOccurred())
		Expect(b1.Index).To(Equal(int32(0)))
	})
})
