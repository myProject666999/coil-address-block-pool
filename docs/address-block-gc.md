# 节点删除后的地址块回收（Address Block Reclamation）设计

本文档是地址块延迟回收特性的评审依据，状态机、回收依据和故障语义以本文档为准；代码是该设计的实现，不作为状态转换的唯一载体。

## 1. 现状核对（改造前的实际行为）

### 1.1 分配链路

1. 节点上的 `coild` 在本节点已有地址块全部占满时，创建一个 `BlockRequest` CR（owner 为该 Node）。
2. `coil-ipam-controller` 的 `BlockRequestReconciler` 调用 `PoolManager.AllocateBlock`。
3. `pool.AllocateBlock`（`pkg/ipam/pool.go`）在 pool 级互斥锁内取 bitset 中**编号最小的空闲 index**（`NextClear(0)`），创建 `AddressBlock` CR：
   - 标签 `coil.cybozu.com/pool=<池名>`、`coil.cybozu.com/node=<节点名>`、`coil.cybozu.com/request=<BlockRequest UID>`；
   - 带 finalizer `coil.cybozu.com`，ownerReference 指向 `AddressPool`（**不**指向 Node）；
   - 在内存 bitset 中标记该 index 已占用。
4. 节点需要第二个块时重复同一流程，因此一个节点可持有多个块。
5. 块的归还（现有两条路径）：
   - 节点内：`coild` 发现块内所有 IP 均已释放时，移除 finalizer 并删除块（`nodePool.gc/free`）。
   - 节点删除：`runners/garbage_collector.go` 周期性扫描，凡 `node` 标签对应的 Node 对象不存在，**立即**移除 finalizer 并删除块，不检查 Pod、不等待、不区分同名节点。

### 1.2 现状的实际状态

| 实际情况 | 现状如何归类 | 风险 |
|---|---|---|
| 空闲（池中未切块的 index） | bitset 未置位，无 AddressBlock 对象 | — |
| 已分配给存活节点 | AddressBlock 存在，node 标签指向现存 Node | — |
| 已分配但 Node 对象已消失（旧 Pod 可能仍运行） | 下一轮 GC（默认间隔 1 小时，但一旦扫到就立刻）删除 | **IP 可能仍被旧节点上的 Pod 使用，块被重新切给新节点后地址冲突** |
| Node 暂时失联（对象仍在） | 不回收 | 正确 |
| 同名节点重新注册（云节点常见） | 仅凭名称关联；新 coild 会通过 node 标签把旧块当作自己的块继续使用 | 若机器其实没换，这是对的；若换了机器，旧 Pod 与新节点会并存于同一块 |
| Node 被标记不可调度（cordon）/驱逐 | Node 对象仍存在，不回收 | 正确，块仍归属该节点 |
| 控制器重启后重复扫描 | `deleteBlock` 先去 finalizer 再 Delete，NotFound 被忽略 | 幂等，但没有“等待中”这一持久状态 |

### 1.3 块与节点现有的关联方式

- 唯一关联是 AddressBlock 标签 `coil.cybozu.com/node=<节点名>`，**只按名称，不含 Node UID**。
- coild 的 `nodePool.syncBlock` 按 `(pool, nodeName)` 两个标签列块；coil-router 同样按 node 标签名查 Node 的 InternalIP，Node 名查不到时跳过路由。
- 因此“同名节点”在现有实现里无法区分新旧实例，这是本设计必须解决的点。

## 2. 地址块状态机

状态不新增 CRD 字段，完全由 AddressBlock 上已有的标签/注解推导，保证控制器重启后状态不丢失、可重复推导。

### 2.1 状态定义

| 状态 | 判定条件 | 含义 | 是否可被分配给新节点 |
|---|---|---|---|
| `Free`（空闲） | 不存在该 index 的 AddressBlock 对象，pool bitset 未置位 | 池中未切出的块 | 是（分配即创建块） |
| `Allocated`（已分配） | AddressBlock 存在，有 `coil.cybozu.com/node` 标签，且该 Node 现存 | 块归属某存活节点 | 否 |
| `PendingReclaim`（等待回收） | AddressBlock 存在，但 node 标签已被摘除，带 `coil.cybozu.com/reclaim-at`、`coil.cybozu.com/reclaim-node`、`coil.cybozu.com/reclaim-uid` 注解 | 原节点已确认消失，处于宽限等待；块仍占用池 index，**不会被切给别人** | 否 |
| `Reclaimed`（已回收） | 宽限到期且无占用证据，块被删除（回到 `Free`） | 等价于 Free，只是转换命名 | 是 |

另保留现有特例：带 `coil.cybozu.com/reserved=true` 的迁移保留块不参与节点删除回收（它们本就不属于某个存活节点的常规生命周期，由迁移流程处理）。

### 2.2 状态转换表

| # | 起始状态 | 事件/条件 | 目标状态 | 处理动作 | 合法性 |
|---|---|---|---|---|---|
| T1 | Free | 节点申请块（BlockRequest） | Allocated | 创建 AddressBlock，置 node 标签与 node-uid 注解，bitset 置位 | 合法 |
| T2 | Allocated | Node 对象确认不存在（扫描时 Node 列表无此名） | PendingReclaim | 摘除 node 标签，写入 reclaim-at=now+grace、reclaim-node=原名、reclaim-uid=原 UID；保留 pool 标签与 finalizer，块不删除 | 合法 |
| T3 | PendingReclaim | 同名 Node 重新出现，且 UID 与 reclaim-uid 相同 | Allocated | 恢复 node 标签（=reclaim-node），删除三个 reclaim-* 注解 | 合法（原机器/原控制面身份返回，继承原块） |
| T4 | PendingReclaim | 同名 Node 重新出现，但 UID 不同（或旧块无 UID 记录） | PendingReclaim（不变） | 不恢复标签；新节点视为新归属者，只能申请新块 | 合法但保持等待 |
| T5 | PendingReclaim | now ≥ reclaim-at 且无占用证据（见 §3） | Reclaimed→Free | 移除 finalizer 并删除块；pool 下次 SyncBlocks 后该 index 可重新分配 | 合法 |
| T6 | PendingReclaim | now ≥ reclaim-at 但仍有占用证据 | PendingReclaim（不变） | 不删除，下一轮重新评估（reclaim-at 不向后推，避免无限占用） | 合法 |
| T7 | Allocated | 节点内 coild 释放块内最后一个 IP（现有路径） | Free | coild 移除 finalizer 并删除块 | 合法（保持不变） |
| T8 | Allocated | Node 被 cordon / 驱逐 / 失联但对象仍在 | Allocated（不变） | 无动作 | 合法（明确不是删除） |
| X1 | Free | 回收/删除 | Free | — | **非法**：没有对象可回收，回收动作必须 NotFound 安全 |
| X2 | PendingReclaim | 直接分配给另一个节点 | — | **非法**：等待期内块仍占 index，分配器选不到它 |
| X3 | Allocated | 不经过 PendingReclaim 直接回收 | — | **非法**：本特性显式废除旧 GC 的“节点消失即删”路径 |
| X4 | PendingReclaim | 节点名不同的节点“继承” | — | **非法**：只允许同名且同 UID 的 T3 |
| X5 | 任意 | 控制器重启后重放同一事件 | 同态 | 所有动作按对象当前标签/注解判定，重复执行结果与执行一次相同（幂等） | 合法性要求 |

### 2.3 同名节点的处理结论

- **关联键**：新分配的块同时记录 node 名称（标签）与 Node UID（注解 `coil.cybozu.com/node-uid`）。名称仍用于 coild/router 的既有选择逻辑，UID 仅由控制器 GC 用于身份判定。
- **同名返回能否继承原块**：仅当返回节点的 UID 与块上记录的 UID 一致时允许（T3）。UID 不同说明是新机器/新实例，原块上的旧 Pod 地址可能仍在网络中生效，必须等满宽限期并通过占用检查后回收，新节点通过正常申请获得新块，绝不共享旧块。
- **历史块（无 node-uid 注解）**：节点仍存活时，GC 第一次扫描到即补写 node-uid；节点已消失的历史块进入 PendingReclaim 后，同名返回一律按 T4 处理（不继承）。宁可延迟回收，不冒险继承。

## 3. 占用证据：选 Pod 列表，并给出视图不一致时的裁决

### 3.1 两种候选证据

1. **Pod 列表**：API 中 `spec.nodeName=<节点名>` 且已拿到 Pod IP 的 Pod。Pod 对象独立于 Node 存活，节点对象删除后旧 Pod 在一段时间内仍可查询。
2. **地址分配记录**：coild 内存中的 `allocInfoMap`/allocator bitset（每个 IP 的占用位图）。它只存在于该节点上 coild 进程的内存里，节点消失后控制器**无法读取**；集群中没有等价的持久化逐 IP 分配 CR。

### 3.2 选择与原因

**控制器侧回收以 Pod 列表为唯一占用依据**，原因：

- 节点删除后 coild 不可达，地址分配记录这一证据源在需要做回收裁决时恰好不可用；只有 Pod 列表仍可从 API 获得。
- Pod 列表是集群级、可在控制器重启后重新 List 重建的证据，满足幂等与可重放要求。
- coild 节点内的回收路径继续使用自己的逐 IP 位图（更精确），两条路径不冲突：coild 只删自己节点上、自己确认无 IP 的块；控制器只处理“节点已不存在”的块。

判定为“仍占用”的 Pod 需同时满足：`spec.nodeName` 等于原节点名、非 hostNetwork、`status.podIPs` 非空、且无 deletionTimestamp。终止中（Succeeded/Failed/带 deletionTimestamp）的 Pod 不算占用——它们的 IP 已在释放流程中，等待它们只会无谓延长回收。

### 3.3 视图不一致时的裁决表

| 故障/不一致 | Pod 视图 | 块/节点视图 | 采用的裁决 | 原因 |
|---|---|---|---|---|
| apiserver List Pod 失败 | 不可得 | 可得 | **本轮不回收任何块**，下一轮重试 | 无法证明无占用，保守等待；只影响回收时效，不产生冲突 |
| apiserver List Node 失败 | 可得 | 不可得 | **本轮不回收**，直接返回错误重试 | 无法确认节点不存在，回收条件一不满足 |
| Pod 已被删除但旧容器仍在跑（kubelet 已死） | 无 Pod | 节点消失 | 宽限到期后回收 | Pod 证据已消失，宽限期覆盖 kubelet 上报滞后；残余风险由宽限时长兜底 |
| Pod 对象残留（终态对象未及时清掉） | 有终态 Pod | 节点消失 | 终态 Pod 不算占用，到期可回收 | 终态意味着 IP 已在释放 |
| 运行中 Pod 因控制器 cache 滞后未出现在 List 中 | 暂时无 Pod | 节点消失 | 宽限期内多轮扫描；到期当轮仍无 Pod 才回收 | 宽限期 + 周期扫描给 cache 重同步留足窗口 |
| 节点失联但 Node 对象仍在 | 任意 | Node 存在 | 不进入 PendingReclaim | 回收条件一（节点确认不存在）不满足 |
| 同名新节点已注册，旧 Pod 仍在 | 有旧 Pod | 新 Node（UID 不同） | T4 保持等待；旧 Pod 消失且到期后才回收旧块；新节点拿新块 | UID 隔离两个归属者，杜绝同块双归属 |

总原则：**证据冲突或缺失时一律偏向“不回收”**。错误地延迟回收只损失地址利用率，错误地提前回收会造成 IP 冲突，二者代价不对称。

## 4. 宽限期与可配置性

- 新增 `coil-ipam-controller` 启动参数 `--gc-pod-grace-period`（duration），默认 **5 分钟**。设为 0 时退化为“节点确认不存在且当轮无 Pod 即回收”，但仍经过 PendingReclaim 状态落盘（至少跨越一个扫描周期），不会回到旧版“同轮即删”的语义。
- 现有 `--gc-interval`（扫描间隔，默认 1h）保持不变。实际回收延迟 ≈ grace period + 最多一个扫描间隔；调短 gc-interval 可加快回收。
- 宽限起点持久化在块注解 `reclaim-at` 上，控制器重启不重置计时。
- 等待期内块仍占据池的 bitset index（对象不删，SyncBlocks 仍把它算已分配），因此分配器天然选不到等待中的块，无需改动分配路径即可实现隔离。

## 5. 分配确定性、争抢与池前缀变更

### 5.1 两个节点争抢最后一个块

- `pool.AllocateBlock` 持有 pool 级互斥锁，整个“选 index → Create AddressBlock → bitset 置位”在锁内完成。
- 选块规则固定为 `NextClear(0)`——编号最小的空闲 index。相同池状态、相同申请顺序必然得到相同块选择，可排障推演。
- 争抢结果：先拿到锁的申请得到最后一个块；后到者 bitset 已全满，收到 `ErrNoBlock`（"out of blocks"），BlockRequest 被置为 `Failed/out of blocks`。失败方绝不会拿到已占用块。

### 5.2 是否优先复用刚回收的块

复用策略保持“最小编号优先”，不做 LRU/刚回收优先。刚回收的块编号若最小就会被先选中（自然复用，地址利用率好），否则选更小的空洞。理由：确定性优先、实现无额外状态；等待期本身已经把“刚回收”与“旧节点仍可能占用”在时间上隔离开，回收完成即视为安全，无需再刻意冷却某个块。

### 5.3 池前缀扩大/缩小

- 扩大（仅允许向 `subnets` 追加子网，现有元素不可变、blockSizeBits 不可变）：新子网产生新的更高编号 index，已分配块的 index、CIDR、归属完全不变。webhook `validateUpdate` 已强制该规则，补测试锁定。
- 缩小（删除/修改既有子网）：webhook 一律拒绝（`unshrinkable`/`unchangeable`），因此“已分配块落到池外”的修改无法通过校验，补测试锁定拒绝行为。

## 6. 幂等性（控制器重启 / 事件重放）

GC 每一轮都是对 AddressBlock/Node/Pod 当前状态的纯推导，不依赖内存中的“上次处理到哪”：

- 摘除 node 标签与写 reclaim-* 注解是同一次 patch；重启后看到注解即知已进入等待，不会重设 reclaim-at（计时不重置）。
- T3 恢复按 UID 判定，重复执行结果相同。
- 删除块沿用现有“先移 finalizer（Conflict 重试，NotFound 视为成功）再 Delete（忽略 NotFound）”，回收两次与回收一次等价。
- 等待中的块仍有 pool 标签，pool 重建（重启后 SyncBlocks）时照常计入 bitset，不会被重复分配。

## 7. 与现有行为的差异、等待时间与利用率影响

| 维度 | 旧行为 | 新行为 |
|---|---|---|
| 节点消失后的动作 | 当轮扫描立即删块 | 先摘 node 标签进入 PendingReclaim，落盘 reclaim-at |
| 额外等待时间 | 0 | 默认 5 分钟宽限 + 最多 1 个 gc-interval 的扫描对齐延迟 |
| Pod 检查 | 无 | 到期当轮要求无占用 Pod，否则继续保留 |
| 同名节点 | 名称匹配即被新节点继承 | 必须 UID 相同才继承；不同则旧块走完等待回收，新节点拿新块 |
| 地址利用率 | 回收最快 | 等待期内每块暂不可用；按块粒度占用，最坏情况每消失一个节点暂留其全部块一个宽限周期，到期立即归还，长期利用率不受影响 |
| 冲突风险 | 高（旧 Pod 仍在用即可能重分配） | 显著降低：宽限 + Pod 证据 + UID 隔离三重防护 |
| cordon/驱逐 | 不回收 | 不回收（语义不变，测试显式区分） |

## 8. 测试策略

核心逻辑放在 `pkg/blockgc` 纯 Go 包，依赖全部以接口注入，使用：

- 内存块存储（fake AddressBlock store）
- 假 Node/Pod 列表（可自由构造同名、UID、终态、deletionTimestamp）
- 可控假时钟（手动推进，不依赖真实 sleep）
- 内存池 bitset 复制品，用于验证等待中的块不可被分配

### 8.1 测试与需求场景的对应

| 需求场景 | 纯 Go 测试（`pkg/blockgc`） | envtest 集成测试 |
|---|---|---|
| 正常删除且没有 Pod | `TestSweep_NormalDeleteNoPods`（含到期前后断言） | runners: "delays reclamation ..." |
| 删除时仍有 Pod | `TestSweep_DeleteWithRunningPod`（含终态/终止/hostNetwork/无 IP 四种不阻塞情形） | runners: "does not reclaim while a running pod ..." |
| 消失后同名返回（同/异 UID、历史无 UID 块） | `TestSweep_SameNameNodeReturns` | runners: "restores a pending block only when ... same UID" |
| 十块隔离（甲 3 块、乙只能领 7 块、期满后才能领） | `TestSweep_TenBlockIsolation` | — |
| 两节点争抢最后一块 + 确定性 | `TestAllocate_ContentionForLastBlock` | ipam: "resolves contention ... deterministically" |
| 领取第二个块（扩容） | `TestAllocate_SecondBlockForNode` | 既有 ipam 分配测试 |
| 扩大/缩小池前缀 | — | api/v2 webhook 测试（追加允许、删除/修改拒绝、扩大后旧块 index/CIDR 不变） |
| 宽限到期前/后 | `TestSweep_GraceBoundary`（精确到 1 纳秒边界） | runners 各用例的 Eventually/Consistently 时间窗 |
| 幂等与重启等价 | `TestSweep_IdempotentAndRestartSafe` | GC 循环本身按周期重放 |
| cordon/驱逐不算删除 | `TestSweep_NodeStillPresentIsNotDeletion` | runners: "keeps blocks ... including cordoned nodes" |
| Pod/Node List 故障 | `TestSweep_PodListFailureBlocksDeletion`（Pod 失败不回收） | — |
| reserved 迁移块不回收 | `TestSweep_ReservedBlocksIgnored` | — |
| 历史块 UID 回填 | `TestSweep_BackfillsNodeUID` | — |

每个纯 Go 用例都调用 `assertSingleOwnership`：以块 index 为键建立归属映射，任何时刻同一 index 不允许出现两个不同归属者（含 `pending-reclaim:<node>` 这一临时归属），即“任何地址都不会同时属于两个归属者”的程序化断言。

envtest 集成测试保留在 `runners`（使用真实 k8s 存储接口跑同一套 Collector 逻辑），作为对 fake 测试的交叉验证。

## 9. 实现要点与故障兜底（代码层面）

- 存储接口化：`blockgc.Store/NodeLister/PodLister/Clock` 四个接口，runners 包提供基于 controller-runtime client 的实现；生产用系统时钟，测试用假时钟。
- GC 的块列表与 Node/Pod 列表都走 `apiReader`（直连 apiserver），而不是 manager 缓存 client：GC runnable 不注册任何 informer，缓存对未 watch 的类型可能返回空/陈旧列表。
- 块状态完全由标签/注解推导，无内存状态机；`MarkPending/Restore/SetNodeUID` 均做幂等保护（重复进入同一状态不重置 `reclaim-at`、不重复计数），删除沿用“移 finalizer（Conflict 重试、NotFound 成功）+ Delete（忽略 NotFound）”。
- `BlockRequestReconciler` 分配前读取 Node UID 并写入块注解；Node 读取返回 NotFound 时不阻断分配，而以空 UID 继续（GC 之后可回填，或按 T4 保守拒绝同名继承）；其他读取错误才让请求重试。
- 新增 RBAC：`coil-ipam-controller` ClusterRole 增加 `pods` 的 get/list（两个 role yaml 同步）。
- 新增启动参数 `--gc-pod-grace-period`（默认 5m），与既有 `--gc-interval` 正交。
