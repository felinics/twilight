# Twilight Agent Artifact Core

状态：v1 设计规范。本文定义 Artifact binding、content reference 与 retention claim。claim 在 `Active` 与 `Released` 两态之间迁移。

本文定义 `agentcore/artifact`。文中的"必须""不得""应该"是协议约束；canonical JSON、JCS 与 domain-separated digest 使用 `agentcore/jsonstable` 和 `agentcore/es` 的通则。

## 1. 模型与范围

Artifact Core 只有三个模型：

```text
Ref：定位并验证不可变内容
Binding：稳定 BindingID 到 immutable Ref 的映射
RetentionClaim：owner 对一个 BindingSet 的 durable 保留事实
```

`BindingSet` 是 claim 的内容集合。`Active` claim 是 retention root；`Released` claim 结束对内容的保留。claim 由 Session Module Framework 的 `Writer` 在 Append owner fact 之前以 `Active` 状态建立（EXT-WRT-3），使已提交引用始终具有 retention root。Append 结果未知时，claim 保持 Active，重开后的日志核对确认 owner fact 已提交则继续保留，确认未提交则释放孤儿 claim（ART-RET-3）。这里的“孤儿 claim”只描述 retention claim 没有对应 owner fact，不表示 Executor 的 `AttachmentState=orphaned`。`Prepared` 保留给需要显式 in-flight 状态的部署（第 7 节）。Core 通过 `ClaimOwner` 表达引用所属的权威身份，owner 的领域语义由对应模块解释。Attachment 等 owner module 可以关联 `AttachmentID`、subject 与 `BindingID`，artifact 边界使用 BindingID。

**ART-SCP-1** Core 不得解释 `ClaimOwner`，不得要求某种数据库、文件系统或 provider 实现。参考实现与 conformance suite 见第 8 节。

**ART-SCP-2** Core 的范围：Ref、Binding、Resolver/Store/Promoter capability、两态 RetentionLedger、SchemeDefinition 与 provider binding registry。

## 2. identity 与 wire

```go
type WireVersion uint16
type Scheme string
type Authority string
type Key string
type BindingID string
type BindingDigest string
type RefWireIdentity string
type ClaimID string
type RefSetDigest string
type ProviderKindID string
type ProviderInstanceID string

type Durability string
const (
    Ephemeral Durability = "ephemeral"
    EventBound Durability = "event_bound"
    Pinned Durability = "pinned"
)

type Integrity struct { Algorithm, Value string }
type Ref struct {
    Scheme Scheme; Authority Authority; Key Key
    MediaType string
    SizeBytes *uint64
    Integrity *Integrity
    Durability Durability
    ExpiresAtUnixMilli *int64
}
```

**ART-ID-1** 所有 identity 必须非空、稳定，按 bytewise UTF-8 比较；精确 identity、整数和 digest 在 JSON 中为 string。Ref 不得含 credential、临时签名 URL 或进程 handle。

**ART-REF-1** `LocatorIdentity=(Scheme, Authority, Key)`；`RefWireIdentity` 是完整 Ref 的版本化 canonical wire encoding，`RefIdentity(Ref)` 必须由 WireCodec 实现。因此 `MediaType` 是 identity-bound：它进入 RefWireIdentity 和 BindingDigest；但它仍是来自内容声明的 untrusted metadata，resolver、materializer 和安全策略不得仅据它判定可执行性、解析器或权限。相同 locator 的 size（含 presence）和 integrity（含 presence）必须一致，否则 admission 和 resolve 失败。

**ART-REF-2** `cas` 必须带 integrity；`ExpiresAtUnixMilli` 仅允许 `Ephemeral`；`EventBound` 和 `Pinned` 不得过期。durability 顺序为 `Ephemeral < EventBound < Pinned`，promotion 只能产生同级或更高的新 Ref。

**ART-WIR-1** `WireVersion` 冻结字段、required/omitted、array order、unknown-field policy 和 digest preimage。v1 省略 optional empty field、拒绝 null 和未知 envelope field；`size_bytes`、`expires_at_unix_milli` 使用无前导零十进制 string。codec 必须提供：

```go
type WireCodec interface {
    Version() WireVersion
    RefIdentity(Ref) (RefWireIdentity, error)
    EncodeRef(Ref) (jsonstable.Value, error)
    DecodeRef(jsonstable.Value) (Ref, error)
    EncodeBinding(Binding) (jsonstable.Value, error)
    DecodeBinding(jsonstable.Value) (Binding, error)
    EncodeManifest(BindingManifest) (jsonstable.Value, error) // 第 7 节
    DecodeManifest(jsonstable.Value) (BindingManifest, error) // 第 7 节
}
```

## 3. Ref 与 Binding

```go
type Binding struct { ID BindingID; Ref Ref; Digest BindingDigest }
type Info struct { MediaType string; SizeBytes *uint64; Integrity *Integrity; Durability Durability }
type PutRequest struct { MediaType string; Reader io.Reader; Durability Durability }
type PromoteRequest struct { TargetScheme Scheme; TargetAuthority Authority; Durability Durability }

type Resolver interface {
    Stat(context.Context, Ref) (Info, error)
    Open(context.Context, Ref) (io.ReadCloser, Info, error)
}
type Store interface { Put(context.Context, PutRequest) (Ref, error) }
type Promoter interface { Promote(context.Context, Ref, PromoteRequest) (Ref, error) }
// ContentStore 是一个 Authority 的完整 capability 集合。
type ContentStore interface { Resolver; Store; Promoter }
// CopyPromoter 是跨 store 的 promotion：从 Source resolve，以目标 durability Put 进 Target，
// 校验两侧 integrity 一致后返回目标 Ref。
type CopyPromoter struct { Source Resolver; Target Store }
type BindingResolver interface { ResolveBinding(context.Context, BindingID) (Binding, error) }
type BindingStore interface {
    CreateBinding(context.Context, Binding) (Binding, error)
    LookupBinding(context.Context, BindingID) (Binding, bool, error)
}
```

**ART-BND-1** Binding immutable。`BindingDigest` 覆盖 versioned domain separator、BindingID 和完整 RefWireIdentity。相同 BindingID 只可重建逐字段相同的 Binding；其他值为 conflict。

**ART-BND-2** Resolver 必须验证返回 bytes 与声明的 size/integrity 一致，Ref 声明的 MediaType 与存储时的声明不一致同样以 `corrupt` 拒绝。Store 只有在 durable acknowledgement 后返回 Ref；同 immutable identity 和 bytes 的重复 Put 幂等，同 bytes 以另一 MediaType 重复 Put 为 `conflict`。`Ephemeral` Put 返回的 Ref 是否携带 `ExpiresAtUnixMilli` 及其时长由 Store 的部署参数决定；`EventBound` 与 `Pinned` 的 Ref 不携带。promotion 流程为 `resolve → promote → CreateBinding(target Ref)`，不得重写旧 Binding；promote 产生的 Ref 与源 Ref 指向同一内容（integrity 相等），durability 不低于源，且离开 `Ephemeral` 后不再过期。同一 store 内的 promote 只提升 durability；跨 store 的 promote 经 `CopyPromoter` 复制字节。Store 可配置单次 Put 的字节上限（`MaxBytes`）：长度恰为上限的内容正常存入，超过上限的 Put 以 `invalid` 拒绝且不写入；实现最多读取上限加一个字节即可判定，不得先缓冲整个输入，上限取 int64 最大值时不得溢出。

## 4. capability interfaces

**ART-CAP-1** Resolver、Store、Promoter 是 capability boundary：它们必须区分 `missing`、`expired`、`unauthorized`、`corrupt` 和 transient failure，并防护跨 `Authority` key confusion、path traversal、size amplification 与不安全 media-type trust。

**ART-CAP-2** `Scheme` 是 resolution contract；`Authority` 是逻辑 store instance；`Key` 由 scheme 解释。标准 scheme 为 `cas`（content digest）、`spill`（opaque temporary key）和 `workspace`（immutable revision + canonical path）。`cas` 的 Key 为 `<algorithm>:<hex>`，与 Ref 的 Integrity 逐字相等；Put 的幂等与 Resolver 的 `missing`/`corrupt` 判定都以它为准。一个 Resolver 只服务自己的 Authority：另一 Authority 的 Ref 返回 `unauthorized`，未实现的 scheme 返回 `unsupported`。自定义 scheme 使用 `ext:<module-id>/<name>`，发布后不得破坏其 key、integrity、durability 或 resolution contract。

## 5. retention ledger

```go
type ClaimOwner struct { Kind, Authority, Identity string }
type ClaimState string
const (
    ClaimActive ClaimState = "active"
    ClaimReleased ClaimState = "released"
    ClaimPrepared ClaimState = "prepared" // 两阶段部署使用（第 7 节）
)

// BindingSet is a canonical, resolved retention set.
type BindingSet struct { BindingIDs []BindingID; RefSetDigest RefSetDigest }
type BindingSetBuilder interface {
    Build(context.Context, []BindingID) (BindingSet, error)
}
type RetentionClaim struct {
    ID ClaimID; Owner ClaimOwner; BindingSet BindingSet; State ClaimState
}
type ClaimCursor struct { Watermark ClaimID; After ClaimID }
type ClaimPage struct { Items []RetentionClaim; Next *ClaimCursor }
// Identities 为 nil 选中该 (Kind, Authority) 下的全部 owner；Limit 为页大小，0 取实现默认值。
type ClaimOwnerQuery struct { Kind, Authority string; Identities []string; Limit int }

// RetentionLedger 自行持久化（Memory、文件或数据库），不依赖宿主事务。
type RetentionLedger interface {
    // Activate 建立或幂等确认一个 Active claim；返回即持久。
    Activate(context.Context, ClaimID, ClaimOwner, BindingSet) (RetentionClaim, error)
    LookupClaim(context.Context, ClaimID) (RetentionClaim, bool, error)
    ReleaseActive(context.Context, ClaimID) error
    ClaimsByOwner(context.Context, ClaimOwnerQuery, ClaimCursor) (ClaimPage, error)
}
// OwnerVerifier 由 owner 的宿主提供：owner fact 是否已持久存在。
// Session 部署中 owner 为 {Kind:"twilight/session/commit", Authority:SegmentID, Identity:CommitID}，
// 实现为对该段查找该 CommitID 的 commit（EXT-WRT-5）。
type OwnerVerifier interface {
    OwnerExists(context.Context, ClaimOwner) (bool, error)
}
```

**ART-RET-1** `BindingSetBuilder.Build(ctx, ids)` 是构造 BindingSet 的唯一算法：它将 ids canonicalize 为 sorted-unique `BindingID`，逐个通过 BindingResolver resolve，并计算覆盖 profile、WireVersion 和按 BindingID 排序的 `(BindingID, BindingDigest)` 的 `RefSetDigest`。`BindingSet` 必须同时携带这两个值，不能由调用者单独拼接 digest。ledger 必须以自己的 BindingResolver 重建并精确验证传入 set。

**ART-RET-2** claim 只接受 `EventBound` 或 `Pinned` Binding；`Ephemeral` 必须先 promote。ClaimID 必须由 owner fact identity 与 BindingSet 稳定、确定地派生，并永久绑定该 owner 与 set：`Activate` 对同 ID、同 owner、同 set 幂等，对任何其他组合 conflict。`Active` claim 是 GC root；GC 只忽略 `Released` claim。未知 scheme 必须保守保留。

| 操作 | 前置状态 | 结果 |
|---|---|---|
| Activate(new ID, owner, set) | 不存在 | Active |
| Activate(existing ID, exact owner/set) | Active | 幂等成功 |
| Activate(existing ID, exact owner/set) | Released | conflict |
| Activate(existing ID, other owner/set) | 任意 | conflict |
| ReleaseActive | Active，且 owner retention 已结束或 owner 不存在 | Released |
| ReleaseActive | Released | 幂等成功 |
| ReleaseActive | 不存在 | not found |

**ART-RET-3** `ClaimsByOwner` 按 ClaimID 稳定排序枚举匹配 owner 的全部状态的 claim（按状态筛选由调用者做），使用 watermark cursor：零值 cursor 开始一次枚举，第一页把 `Watermark` 固定为当时最大的 ClaimID，之后的页只返回 `After` 之后、`Watermark` 之内的 claim，枚举期间新 Activate 的 claim 不进入本次结果；`Next` 为 nil 表示枚举结束。`Identities` 为 nil 匹配该 (Kind, Authority) 下全部 owner，显式列表只匹配其成员，空 owner identity 不匹配任何 claim；Kind 或 Authority 为空是 `invalid`。回收前核对：GC 在按 Active claim 计算 root 之前，对每个 Active claim 调用 `OwnerVerifier.OwnerExists`，不存在则 `ReleaseActive`；这一步清理 EXT-WRT-3 顺序下可能留下的孤儿 claim（claim 已 Active 而 owner commit 从未落盘）。核对只能在该 owner 的写入路径不可能仍在进行时执行：Session 部署中即该 Session 没有进行中的 `Writer.Commit`，在 `OpenWriter` 完成日志重建之后、接受第一个 Commit 之前对该 Session tip 段的 claim 核对一次，运行期的核对必须与 Writer 互斥。owner retention 结束的另一条路径是段回收：`Collect` 删段或截段时释放对应 commit 的 claim（SES-GC-3、EXT-WRT-9）。

结果未知而失效的 Writer 在 `OwnerExists` 返回其失效错误，核对流程保持 claim 原状态。宿主重开 Writer、重建完整提交索引后恢复核对。已释放的孤儿 claim 保持 Released；同一 owner 的提交重试通过新的 ClaimID 建立 retention root，Session Writer 的确定性派生规则见 EXT-WRT-5。

## 6. provider 与 scheme boundary

```go
type SchemeDefinition struct {
    Scheme Scheme; SupportedDurabilities []Durability
    ValidateRef func(Ref) error
}
type ProviderDescriptor struct {
    KindID ProviderKindID; Schemes []Scheme; ConfigSchema jsonstable.Value
}
type ProviderBinding struct {
    Scheme Scheme; Authority Authority; InstanceID ProviderInstanceID
}
type Registry struct { /* immutable indexes */ }
func BuildRegistry(schemes []SchemeDefinition, bindings []ProviderBinding) (*Registry, error)
func (r *Registry) Scheme(Scheme) (SchemeDefinition, bool)
func (r *Registry) Provider(Scheme, Authority) (ProviderBinding, bool)
func (r *Registry) Verify(Ref) error
func CASScheme() SchemeDefinition // 标准 cas 定义：全部 durability，Key 等于 Integrity
```

**ART-PRO-1** registry 在 startup 组合后 immutable；每个 Scheme 有唯一 definition（重复为 `conflict`），provider binding 只能指向已注册的 Scheme（否则 `invalid`），`(Scheme, Authority)` 唯一（重复为 `conflict`）。verified use（`Verify`）要求：Ref 自身合法、Scheme 已注册且支持该 durability、通过 scheme 自己的 `ValidateRef`、`(Scheme, Authority)` 有 provider binding（缺失为 `unavailable`）。未注册 Scheme 的 Ref 返回 `unsupported` 而非 `invalid`，使 GC 等调用者能按 ART-RET-2 保守保留它。provider config、secret、物理位置与迁移属于 adapter/Application。

## 7. archive 与 import/export

claim、ledger 与 binding 的归档、导入与导出格式。

```go
type BindingManifest struct {
    WireVersion WireVersion; Bindings []Binding; ActiveClaims []RetentionClaim
}
type InspectionStatus string
const (
    InspectionAccepted InspectionStatus = "accepted"
    InspectionUnknownScheme InspectionStatus = "unknown_scheme"
    InspectionInvalid InspectionStatus = "invalid"
)
type InspectionResult struct { Status InspectionStatus; Detail string }
type VerifiedImportResult struct { Bindings []BindingID; Claims []ClaimID }
```

**ART-ARC-1** manifest 是精确 canonical wire：Bindings 按 BindingID 严格递增、ActiveClaims 按 ClaimID 严格递增，且只可携带 `Active` claims。inspection 可以无损接受未知 scheme record，但不创建可用 Binding 或 claim；verified import 要求 codec、排序、Binding digest、RefSetDigest、scheme/provider 和 object policy 全部通过。

**ART-ARC-2** `ImportActiveClaims` 是 all-or-nothing validation boundary：先验证所有 referenced Binding、durability、digest、owner、ClaimID 和 state，再全部写入或失败。它不接受 Prepared 或 Released records；逐字段相同 active record 幂等，同 identity 的不同 record 为 conflict。导出按 `ClaimsByOwner` 的完整 cursor 枚举 closure。

**ART-PRO-2** adapter 改变物理实现时必须保持 locator resolution 不变，并以 generation/fence 防止旧位置在新位置验证可恢复前回收。具体 filesystem、DB 与迁移步骤由 adapter/Application 负责。

**两阶段 claim** 当 ledger 与 owner fact 不在同一事务域时，`Prepare(ClaimID, Owner, Set)` 建立 `Prepared` claim 作为 in-flight GC root，`Activate` 在 owner fact 确认后转为 Active，`AbortPrepared` 只在 owner operation 已 terminally aborted 的 durable evidence 下转为 Released；reconciler 扫描 `PreparedClaims`，对 NotFound、unknown 或 transient failure 保留 Prepared。

多 package coordination、quarantine 操作流程不属于本规范。

## 8. errors 与 conformance

```go
type ErrorCode string
const (
    ErrInvalid ErrorCode = "invalid"; ErrNotFound ErrorCode = "not_found"
    ErrConflict ErrorCode = "conflict"; ErrUnauthorized ErrorCode = "unauthorized"
    ErrExpired ErrorCode = "expired"; ErrCorrupt ErrorCode = "corrupt"
    ErrUnsupported ErrorCode = "unsupported"; ErrUnavailable ErrorCode = "unavailable"
)
type Error struct { Code ErrorCode; Operation string; Identity string; Detail string }
func (Error) Error() string
```

实现必须以可判别 `ErrorCode` 返回预期失败；`Detail` 不得承载 provider secret。

conformance suite 为 `agentcore/artifact/artifacttest`，以 `BindingStore`、`RetentionLedger`（经 `BindingSetBuilder` 验证 set）与按 Authority 构造 `ContentStore` 的工厂为参数；每个 adapter 以自己的工厂运行同一套断言。它必须验证：

- **ART-ID-1、ART-REF-1、ART-REF-2、ART-WIR-1**：canonical round-trip、拒绝歧义 wire、identity-bound/untrusted MediaType、locator/integrity 和 durability；
- **ART-BND-1、ART-BND-2、ART-CAP-1、ART-CAP-2**：Binding conflict 与 digest 校验、cas Key 等于 Integrity、Put 幂等与 MediaType conflict、resolver 对 size/integrity/MediaType 的校验、`missing`/`expired`/`unauthorized`/`corrupt`/`unsupported` 分类、Put 字节上限（恰为上限存入、超出一字节为 `invalid`、最大上限不溢出）、同 store 与跨 store 的 promotion（不降级、清除过期、不重写旧 Binding）；
- **ART-RET-1、ART-RET-2、ART-RET-3**：BindingSetBuilder/ledger 独立重算与精确验证、RefSetDigest、不可复用 released claim、两态状态表、`Activate` 返回即持久且幂等、owner 不存在的 Active claim 被回收前核对释放而 owner 存在的不受影响、cursor pagination、Active GC protection；
- **ART-PRO-1**：immutable registry 与 provider-instance isolation。
