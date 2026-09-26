# Twilight Agent Session Module Framework

状态：v1 设计规范。本文定义 Session Module Framework。写入串行与幂等重放由进程内的 `Writer` 承担，kernel 只提供追加日志（[agent-session.md](agent-session.md)）。

本文定义建立在 `agentcore/session` 与 `agentcore/artifact` 之上的 Session Module Framework。实现分两个包：`agentcore/session/extension` 承载声明的词汇、`Registry` 与投影引擎，`agentcore/session/writer` 承载写入路径；文中的"必须""不得""应该"是协议约束；JSON canonicalization 与 digest 遵循 `agentcore/jsonstable`、`agentcore/es`。

## 1. 范围与依赖

```text
agentcore/artifact  ←  Session Module Framework  →  agentcore/session
                                      ↑
                          first-party modules: chatlog、turn、run
```

Framework 负责：typed event codec 与按事件类型的 payload 版本；Binding admission；进程内的写入串行、幂等重放与 claim 顺序（`Writer`）；pure projection 与投影缓存。first-party Source 为 `twilight`，Module 为 `chatlog`、`run`、`attempt`、`turn`。

**EXT-SCP-1** 一个 Session 在一个进程内恰有一个 `Writer`，它持有 kernel 的 `session.Handle`（所有权句柄）。全部写入经 `Writer.Commit`：Run 的 Runtime、Turn 的 Coordinator、接管恢复都是它的调用方。模块读取投影经 `ProjectionReader`。

**EXT-SCP-2** 模块集合由组装代码在启动时传入 `BuildRegistry`，运行期不变；本层不 import 任何模块包。first-party 恰为四个 module（`chatlog`、`run`、`attempt`、`turn`）；application module 与它们同构、经装配开口注册，见第 8 节。

**EXT-SCP-4** 本层的两个包平级：`agentcore/session/writer` 依赖 `agentcore/session/extension`，反向不得。依赖由 import 表达，不由目录嵌套表达——一个 import `extension` 的包是它的兄弟而不是子包，first-party module（`agentcore/session/chatlog`、`agentcore/session/run`）与 adapter（`agentcore/session/filestore`）同理。声明（event、module、projection）与 `Registry` 同居前者所依赖的那一层，是因为 `ModuleDescriptor` 声明 `ProjectionDefinition`、而 `ProjectionDefinition.Apply` 消费带模块身份的 `DecodedEvent`——两者互相引用，只有同包才不成环。

**EXT-SCP-3** 模块间依赖单向、固定，以 `Requires` 声明并由 Registry 校验（EXT-REG-4）。v1 四个模块的声明：

| 模块 | Requires |
|---|---|
| `chatlog` | `run`（`run_created`、`model_step_completed`、`tool_step_opened`、`tool_call_completed`、`tool_call_answered`、`tool_call_failed` v1）：assistant 与 tool_result 条目由这些事实折叠 |
| `run` | 无 |
| `attempt` | 无 |
| `turn` | `attempt`（`twilight/attempt/started`）、`run`（`twilight/run/run_ended`）、`chatlog`（`input_delivered`） |

## 2. Registry 与版本

```go
type SourceID = session.SourceID   // 模块身份类型在 kernel 声明，供 Extensions 按模块分槽（SES-WIR-5）
type ModuleID = session.ModuleID
type ProjectionID string
type ProjectionVersion uint16
// PayloadVersion 是一个事件类型 codec 的版本，也是该类型每个 payload 携带的 `v`（SES-VER-1、EXT-REG-2）。
type PayloadVersion uint16

const SourceTwilight SourceID = "twilight"

// ModuleKey 是模块在 Registry 中的身份：(Source, ID) 二元组。
type ModuleKey = session.ModuleKey // { Source SourceID; ID ModuleID }，wire 上为 "source/id"

type EventDefinition struct {
    Type session.EventType
    Codecs map[PayloadVersion]PayloadCodec // 该事件发布过的每个版本的 codec，每个都解码到当前内存类型（EXT-REG-2）
    Version PayloadVersion                 // Encode 写入的版本；零值取 Codecs 的最大键，须在 Codecs 中
    Bindings []BindingReferenceDefinition
    // Ignorable 为真的事件被解码不出它的投影跳过（EXT-PRJ-2）；wire 上不携带该标记。
    Ignorable bool
    Stream string // 该事件所属的流 domain，须是本模块 Streams 中声明的一个（EXT-STR-1）
}
type ModuleDescriptor struct {
    Source SourceID
    ID ModuleID
    Requires []ModuleRequirement
    Streams []StreamDefinition // 本模块拥有的流 domain（EXT-STR-1）
    Events []EventDefinition
    Projections []ProjectionDefinition
}
type ModuleRequirement struct {
    Source SourceID // 必填：依赖以 (Source, Module) 指认
    Module ModuleID
    Events []session.EventType // 被依赖的事件类型（EXT-REG-4）；版本不参与握手
}
type Registry struct { /* immutable indexes */ }
func BuildRegistry(modules ...ModuleDescriptor) (*Registry, error)
func ModulePrefix(source SourceID, id ModuleID) session.EventType // <source>/<module>/
func (r *Registry) LookupEvent(session.EventType) (ModuleKey, EventDefinition, bool)
func (r *Registry) ModuleOf(session.EventType) (ModuleKey, bool) // 按 <source>/<module>/ 前缀
func (r *Registry) Encode(session.EventType, any) (jsonstable.Value, error) // 以该类型 Version 的 codec 编码并写入 v
func (r *Registry) Decode(session.Event) (DecodedEvent, error)
```

**EXT-REG-1** EventType 为 `<Source>/<ModuleID>/<local-name>`。Source 与 ModuleID 是非空、不含 `/` 的合法 UTF-8 段；模块身份是 `(Source, ID)` 二元组，同一 Registry 中该二元组、EventType、ProjectionID 均唯一（同名 ModuleID 可在不同 Source 下共存）。`twilight` Source 保留给本仓库的 first-party 模块，application module 必须使用自己的 Source。`BuildRegistry` 校验每个 EventDefinition 的 Type 前缀等于其模块的 `<Source>/<ID>/`，构建后只读。

**EXT-REG-2** payload 版本属于事件类型（SES-VER-1）。payload object 第一层携带整数字段 `v`，即写入时该类型的 `EventDefinition.Version`：`Encode(type, value)` 以 `Codecs[Version]` 编码并写入 `v = Version`；`Decode` 读 `v` 并选择 `Codecs[v]`，没有 codec 的 `v` 为 Unknown 并保留 raw payload（EXT-REG-3）。`Version` 零值取 `Codecs` 的最大键；声明的 `Version` 没有 codec 时 `BuildRegistry` 失败。同一类型的每个 codec 都解码到模块的当前内存类型（旧版本在 codec 内 upcast），旧版本的 codec 永久保留、永不修改，旧事件不迁移。模块各自演进：一个模块升版本不要求其他模块或 application 模块做任何事。

**EXT-REG-3** `Decode` 对未注册的 EventType 或未注册的 `v` 返回 `DecodedEvent{Unknown:true}` 并保留原始 payload。投影对 Unknown 的处置见 EXT-PRJ-2。

**EXT-REG-4** 模块间依赖由 `Requires` 声明，构建时校验：被依赖模块已注册、依赖图无环、`Requires.Events` 中的每个事件确由被依赖模块拥有、投影消费的 EventType 属于本模块或 `Requires` 中的模块（否则 `ErrInvalid`）。版本不参与握手：依赖方经 `DecodedEvent.Value` 得到的是被依赖模块 codec upcast 后的当前类型（EXT-REG-2）。`Requires` 只表达事件消费依赖；接口实现（如 Runtime 的 `frozen.Store`）是构造参数，不进入 `Requires`。

## 3. event codec

```go
type PayloadCodec interface {
    Encode(value any) (jsonstable.Value, error) // 不含 v；Registry 加入
    Decode(wire jsonstable.Value) (any, error)
    Validate(value any) error
}
type DecodedEvent struct {
    Stream session.StreamRef
    Event session.Event
    Module ModuleKey
    Version PayloadVersion // 该事件的 v
    Value any
    Unknown bool
}

type StreamKey func(value any) (string, error) // 从模块的 typed 事件值提取它所属流的 ID
type StreamDefinition struct {
    Domain string                 // 流 domain，整个 Registry 内唯一；kernel 不命名任何 domain
    Key StreamKey                 // nil 为单例流，batch 的 stream ID 必须为空；非 nil 为键控流，Key(value) 即 batch 的 stream ID
    Lineage session.StreamLineage // fork 之后子如何读该 domain 的流（SES-FRK-5）
}
func (StreamDefinition) Ref(id string) session.StreamRef                        // 该 domain 下的一条流坐标
func (*Registry) LookupStream(domain string) (ModuleKey, StreamDefinition, bool) // domain 的拥有者与声明
```

**EXT-COD-1** codec、Validate、Binding extraction 必须纯、确定、无 IO。Decode wire-first。Encode/Decode 拒绝 nil、typed nil、kind mismatch、未知 kind 与非 canonical value。有效值满足 `Encode → Decode → Encode` 的 canonical round-trip；该性质是模块的测试义务（每个注册事件类型一条往返断言），Registry 的 Encode 不在运行期重验。

**EXT-COD-2** 已提交事件的 payload 保持原始 canonical bytes。`v` 由 Registry 在 Encode 后加入、Decode 前取出；payload 的其他第一层字段不得命名为 `v`。

**EXT-STR-1（流 domain 由模块声明）** 逻辑流的 domain 由模块在 `ModuleDescriptor.Streams` 中声明，每个 domain 恰由一个模块拥有，kernel 不命名任何 domain（SES-WIR-1 只校验 `StreamRef` 的形状）。`StreamDefinition` 给出 `Domain`、`Key` 与 `Lineage`：`Key` 为 nil 是单例流，batch 的 stream ID 必须为空；非 nil 是键控流，`Key(value)` 以模块的 typed 事件值算出该事件所属流的 ID，必须等于 batch 的 stream ID。流的归属因此是模块 Go 值的属性，不与 payload 的字段名耦合，payload 的编码形状可以独立于流拓扑变化。每个事件类型通过 `EventDefinition.Stream` 命名本模块声明的一个 domain。`BuildRegistry` 验证声明：domain 为空或含 `/`、同一模块内或跨模块重复声明、`Lineage` 缺失或未知、事件未命名 domain、事件命名的 domain 未由本模块声明，均为装配错误；`Registry.LookupStream(domain)` 返回 domain 的拥有者与声明。Writer 在 encode 时按声明校验每个 batch：事件的 domain 与 batch 的 domain 不同、单例流的 batch 带 ID、键控流的 batch 无 ID、`Key(value)` 出错、为空或不等于 batch 的 stream ID，均拒绝整个 group。kernel 保持 payload 不透明，校验只在 writer 层执行。写侧强制后，投影按 EventType 折叠即不可能跨 stream 读到外来事件，fold 侧无需再查。第一方模块的声明：chatlog 为单例 domain `chatlog`（`LineageSession`）；turn 为 domain `turn`（Key 为 payload 的 TurnID，`LineageSession`）；attempt 为 domain `attempt`（Key 为 TurnID，`LineageSession`）；run 为 domain `run`（Key 为 `runmod.Event.RunID`，`LineageSegment`）。

## 4. Binding reference declaration 与 admission

```go
type Cardinality struct { Min uint32; Max *uint32 }
type BindingExtractor interface {
    BindingIDs(value any) ([]artifact.BindingID, error) // appearance order
}
type BindingReferenceDefinition struct {
    Extractor BindingExtractor
    Cardinality Cardinality
    AllowedSchemes []artifact.Scheme
    RequiredDurability artifact.Durability
}
```

**EXT-REF-1** 声明以 `Extractor` 提取 typed value 内的全部 Artifact 引用，保留 appearance order，随后 group 才 sorted-unique。Extractor 随 EventDefinition 声明，本层不维护提取器注册表，也不提供路径式（JSONPointer）提取。

**EXT-REF-2** `BuildRegistry` 验证 cardinality、Extractor 非 nil 与 scheme/durability 声明；最低 durability 至少为 `EventBound`。admission 解析每个 Binding，验证 Scheme、最低 durability、resolvability；任何违反拒绝整个 group，不作任何写入。声明只表示该事件的 payload **可能**含引用：不含引用时不经过 admission，因此从不引用 artifact 的宿主无需配置 `Admission`。反之，payload 含引用而对应的 `Bindings` 或 `Ledger` 为 nil 属宿主配置错误，`Commit` 返回 error 而非 `CommitInvalid`，以免配置失败被读成对 group 的判定。该检查不放在 `OpenWriter`：一个事件类型是否真的携带引用要到 payload 解码后才可知，在装配期按声明强制会连带拒绝纯文本部署。

## 5. Writer：进程内的写入串行与幂等

```go
type TypedEvent struct {
    Type session.EventType
    RecordedAtUnixMilli int64
    Value any
}
type TypedBatch struct {
    Stream session.StreamRef // 该 batch 写入的 stream（EXT-STR-1）
    Events []TypedEvent
}
type SemanticGroup struct {
    CommitID session.CommitID
    Batches []TypedBatch
}
// View 是 Commit 回调内可读的一致视图：head、tip 段、提交历史、投影状态。
type View interface {
    Head() session.Head
    Epoch() session.Epoch
    Header() session.SegmentHeader                                // tip 段的 header
    Committed(session.CommitID) bool                              // 只问是否已提交，不读 commit
    LookupCommit(session.CommitID) (session.Commit, bool, error) // 还要该 commit 的全部 batch
    StreamHead(session.StreamRef) (session.StreamSeq, bool)     // tip 段是否写过该逻辑流，及下一条的 StreamSeq（kernel 索引，SES-FRK-5）
    Projection(ProjectionID, ProjectionVersion) (any, error)      // 折叠到当前 head 的状态
}
type CommitFn func(View) (*SemanticGroup, error) // nil 表示不写

type CommitOutcome string
const (
    CommitApplied CommitOutcome = "applied"
    CommitAlreadyApplied CommitOutcome = "already_applied" // CommitID 已在 ledger 中（EXT-WRT-2）
    CommitConflict CommitOutcome = "conflict"             // kernel 以 ErrConflict 拒绝了 Append
    CommitInvalid CommitOutcome = "invalid"
    CommitNoop CommitOutcome = "noop"
)
type CommitResult struct {
    Outcome CommitOutcome
    Commit session.Commit // Applied 为本次写入的 commit，AlreadyApplied 为原 commit
    Claim *artifact.RetentionClaim
    Detail string
}
```

`Outcome` 承载语义结果，`error` 只表示基础设施失败：`CommitInvalid` 与 `CommitConflict` 是回答而非失败，因此以 nil error 返回。调用方必须按 `Outcome` 分支，只判断 `err != nil` 会把"group 被拒"读成写入成功。

```go
// Admission 提供 Binding admission 与 claim ledger。
type Admission struct {
    Bindings artifact.BindingResolver
    Ledger   artifact.RetentionLedger
}

type Writer interface {
    SessionID() session.SessionID
    Epoch() session.Epoch
    Header() session.SegmentHeader   // tip 段的 header
    Commit(context.Context, CommitFn) (CommitResult, error)
    Projections() ProjectionReader   // 读取本 Writer 维护的投影
    OwnerExists(context.Context, artifact.ClaimOwner) (bool, error)
    Close(context.Context) error
}
func OpenWriter(ctx, store session.Store, registry *Registry, admission Admission, sid session.SessionID, opts session.OpenOptions) (Writer, error)
```

**EXT-WRT-1** `OpenWriter` 先读 Session tip 段的 header，随后调 `store.Open` 取得所有权，重建每个已注册投影的当前状态与 head，核对 tip 段的 claim。投影的起始状态按 EXT-PRJ-3/5 取自缓存条目或 `Initial`；缓存条目未覆盖的部分（没有可用条目时即整条日志）被读出来 fold，读完即释放。Writer 不保留日志，也不保留提交历史索引——CommitID 的索引是 kernel 的（SES-REP-3/4），命中时才取该组的行；因此 Writer 的常驻内存只随已注册投影数增长，与日志长度无关。之后 `Commit` 在 Writer 的互斥区内执行：调 fn 得到 group，以每个事件类型的写入版本做 codec（EXT-REG-2），做 admission、claim，`session.Handle.Append`，再把新行折进投影，并按缓存策略刷新条目（EXT-PRJ-6/7）。fn 只能通过 `View` 读，且必须是纯函数：不做外部 IO（模型调用、网络、读远端存储），需要外部数据的调用方在 `Commit` 之前取得并作为闭包值带入。互斥区是全 Session 的串行点，fn 内的 IO 会把它的延迟施加给该 Session 的全部提交方，其失败也无法与"决策失败"区分。admission 与 claim 是互斥区内唯一的 IO，它们是边界的组成部分（EXT-REF-2、EXT-WRT-3）。fn 返回 nil 记 `Noop`。Writer 是并发的唯一入口：Run 的 worker、Coordinator、恢复流程都经它串行，kernel 不再需要临界区回调。

**EXT-WRT-2** 幂等：fn 返回的 group 若 CommitID 已提交（`View.Committed`，命中才取行，fork 的继承前缀计入），返回 `AlreadyApplied` 与原 commit，不重建事件、不写入、不做 admission 与 claim。Writer 不比对两次提交的内容：CommitID 由写者派生，一个 ID 对应一个操作（SES-APP-4）；identity 有意不覆盖内容的 command 族（同一 effect 的两次结算、同一 Turn 的两次 Settle）在内容不同时同样得到 `AlreadyApplied`，以先落下的 commit 为准，调用方从投影读取实际生效的结果。`unit.Work` 只携带 CommitID 与 Parts：CommitID 命中时不准备任何 Part。重放判定与 payload 字节无关。

**EXT-WRT-3** claim 顺序：group 含 Binding 时，Writer 在 `Append` 之前调用 `ledger.Activate(claimID, owner, set)`，让 ledger 中的引用从提交开始就具有 retention root。`Append` 确认写入前拒绝时，Writer 调用 `ledger.ReleaseActive(claimID)` 尽力回收；结果未知、ownership 丢失或 CommitID 冲突时保留 Active claim。`OpenWriter` 重建日志后核对 owner commit：已提交的 claim 保持 Active，未提交的孤儿 claim 被释放（ART-RET-3）。

**EXT-WRT-4** Writer 在两种情况下进入失效状态，本次与之后的 `Commit` 都返回同一错误：(a) `Append` 返回 `ErrOwnershipLost`——Session 级 fencing 在进程内的表现，调用方必须放弃该 Session 的执行，Runtime 与 Loop 对它的处理见 RUN-CMT-6；(b) `Append` 返回结果未知的错误（kernel 的 `ErrHandleFailed`、IO 错误或其他非验证性错误）——Writer 以 `ErrUnknownOutcome` 失效，因为它的 head 与投影状态可能已落后于日志一组，继续提交会给临时行赋 kernel 已用过的 Seq。(b) 的失效限于该实例：宿主经 `Writers` 再次请求即得到重开的 Writer（EXT-WRT-6），`OpenWriter` 从日志重建，同一 group 的重放由 kernel 的索引回答（落盘则 `AlreadyApplied`，未落盘则 `Applied`）。只有保证未写入的错误不致失效：kernel 的验证拒绝（`ErrInvalid`、`ErrNotFound`）与写入开始前的 ctx 错误；`ErrConflict` 按 EXT-WRT-2 报告为 `Conflict`。

**EXT-WRT-11（心跳）** `OpenWriter` 在 `OpenOptions.LeaseDuration` 非零时启动心跳：每 `LeaseDuration/3` 调一次 `Handle.Renew`（SES-OWN-1），`Close` 停止并等待它退出。Renew 返回 `ErrOwnershipLost` 时 Writer 进入失效状态，与被围栏的 Append 同样处理（EXT-WRT-4 (a)）；其他 Renew 失败在下一拍重试，因为租约在过期或被接管之前一直有效。心跳属于 Writer 而不是宿主：持有 Writer 的进程就是租约的持有者，不需要第二处生命周期。

**EXT-WRT-8（fork 不持有 claim）** `writer.Fork(store, registry, ForkRequest{Parent, At, Child})` 只是以 `Fork{Session: Parent, Seq: At}` 调用 `Store.Create`（SES-FRK-1），不建立任何 claim，也不解码父前缀。继承前缀引用的内容由持有这些 commit 的段的 commit claim 保留（EXT-WRT-5）：段活多久，claim 就活多久，而段的存活由 lineage 可达性决定（SES-GC-2），子 Session 作为一个到达它的根即足以保留它。fork 因此是 O(1)，与前缀长度和 registry 是否认识前缀中的事件类型无关。

**EXT-WRT-9（删除与回收）** `writer.Delete(store, sid)` 只调 `Store.Delete` 撤根（SES-GC-1），不触碰 claim；根已不存在不是错误。`writer.Collect(store, admission)` 先调 `Store.Collect` 回收存储（SES-GC-2），再按 `CollectReport` 释放 claim（SES-GC-3）：整段删除的段，释放 `{Kind: commit, Authority: 该段}` scope 下的全部 Active claim；被截断的段，释放 `Dropped` 列出的那些 CommitID 的 claim。先存储后 claim：两步之间失败只泄漏 claim，不会释放仍被 commit 引用的内容；泄漏的 claim 按该段的 scope 手工释放。宿主必须先关闭该 Session 的 Writer。

**EXT-WRT-5** commit claim 以持有该 commit 的段为 owner：`ClaimOwner = {Kind:"twilight/session/commit", Authority:SegmentID, Identity:CommitID}`，首个 ClaimID 为 `Digest("twilight/session-extension/claim", SegmentID, CommitID, RefSetDigest)`，域分隔的版本是本包自己的派生版本常量，预映像不含 SessionID 也不含 kernel 的 `ProtocolVersion`（SES-VER-3）：段是 commit 的 canonical 拥有者，Session fork 或推进 tip 段后仍读到同一条 claim。Writer 以其 tip 段为 Authority 激活 claim，`OpenWriter` 的核对也只扫描 tip 段的 scope：只有 tip 段可能持有本 Writer 留下的孤儿 claim，继承段的 commit 在成为继承段之前都已封印。重放先完整执行 binding admission 与 BindingSet 构建，再查找该 claim。已有记录的 owner、BindingIDs 和 RefSetDigest 必须完全相同。Active claim 由 `Activate` 幂等复用；Released claim 保持终态，Writer 派生后继 `Digest("twilight/session-extension/claim-successor", ReleasedClaimID)` 并重复查找，直到复用 Active claim 或建立新的 retention root。该链允许同一 CommitID 在孤儿回收后继续重试；任一记录的身份或集合冲突都拒绝本次提交。

```go
// Writers 是宿主维护的 SessionID → Writer 映射；模块（run 的 Runtime、turn 的 Coordinator）经它取得 Writer。
type Writers interface {
    Writer(context.Context, session.SessionID) (Writer, error)
}
// WritersConfig 是部署给出的投影缓存；两者都可缺省，缺省即每次从日志开头全折且不写。
type WritersConfig struct {
    Cache       ProjectionCache // 折叠结果的存放处（EXT-PRJ-3）
    CachePolicy CachePolicy     // 刷新哪个投影、何时刷新；nil 即 CacheEvery(DefaultCacheEvery)，部署调 n 即可改区间
    Observers   []CommitObserver // 已应用 group 的最佳努力通知
}
func NewWriters(store session.Store, registry *Registry, admission Admission, opts session.OpenOptions, cfg WritersConfig) Writers
// CloseWriter 关闭并忘记一个 Session 的 Writer；CloseWriters 关闭并忘记全部 Writer。两者只作用于 NewWriters 的值。
func CloseWriter(ctx context.Context, ws Writers, sid session.SessionID) error
func CloseWriters(ctx context.Context, ws Writers) error
```

**EXT-WRT-6** 一个进程对同一 Session 只打开一个 Writer，`Writers` 负责这一唯一性：首次请求时 `OpenWriter`，之后返回同一实例。Writer 因 `ErrOwnershipLost` 失效后，`Writers` 对该 Session 的每次请求都返回该错误，直到宿主调用 `CloseWriter` 忘记它：重开会从当前 owner 手中取回 Session，这一决定属于宿主。Writer 因结果未知失效（EXT-WRT-4 (b)）或被 Close 后，`Writers` 在下一次请求时关闭并忘记旧实例，以 `OpenWriter` 打开新实例返回。模块不自行调用 `OpenWriter`。

```go
// CommitObserver 看到 Writer 应用的每个 group：持久之后、按提交顺序、带已封装的 commit。
type CommitObserver interface {
    Committed(ctx context.Context, sid session.SessionID, commit session.Commit)
}
// WritersConfig.Observers []CommitObserver
```

**EXT-WRT-7** 提交观察。`WritersConfig.Observers` 在每个 `CommitApplied` 之后收到该组的封装行；被拒、重放（AlreadyApplied/Conflict）与 Noop 不通知。通知在互斥区之外执行——观察者不延长事务边界——但按提交全序串行：Writer 在释放互斥锁之前取得通知锁，后一个 Commit 可以立即进入临界区，它的通知却排在前一个之后。观察是派生工作、尽力而为：观察者 panic 被捕获，不影响 Commit 的结果与返回。它是一个 Session 全部观察的唯一源头：Loop 事件、Turn 生命周期、chatlog 条目都是已应用组里的行；宿主在其上派生事件流（OBS-1），不再有第二条观察通道。

## 6. pure projection 与缓存

```go
type ProjectionDefinition struct {
    ID ProjectionID; Version ProjectionVersion
    Consumes []session.EventType
    Initial func() (any, error)
    Apply func(any, DecodedEvent) (any, error)
    StateCodec PayloadCodec
    Inherits InheritPolicy // 从继承 commit 折叠哪些流，nil 按各 domain 声明的 Lineage（EXT-PRJ-8）
    Authoritative bool     // 折叠失败拒绝 commit（EXT-PRJ-9）
}
type ProjectionReader interface {
    // through 是该状态覆盖的 ledger head：Next 为下一未折叠 commit 的 Seq。
    Load(ctx, sid session.SessionID, id ProjectionID, v ProjectionVersion) (state any, through session.Head, err error)
}
// ProjectionCache 是可选的派生缓存，随时可删；Memory 实现由本层提供。
type ProjectionCache interface {
    Load(ctx, sid, id, v) (state jsonstable.Value, through session.Head, ok bool, err error)
    Save(ctx, sid, id, v, state jsonstable.Value, through session.Head) error
}
// ProjectionCacheProvider 由能把缓存落盘的 Store adapter 实现，组装层据此选中它。
type ProjectionCacheProvider interface{ ProjectionCache() ProjectionCache }
// CachePolicy 决定 Writer 刷新哪个投影的缓存条目；cached 是该条目的 through，
// 或缓存中无条目时的零值 Head。它只约束写入，从不约束读取。
// closing 为真表示这是 Close 前的最后一次询问。
type CachePolicy func(id ProjectionID, v ProjectionVersion, head, cached session.Head, closing bool) bool
// CacheEvery 在 head 落后 cached 满 n 个 commit 时刷新，并在 Close 时无条件刷新；n <= 0 取 DefaultCacheEvery。
func CacheEvery(n session.CommitSeq) CachePolicy
// Exclude 拒绝被点名的投影，其余交给 p；组装层用它让宿主自己刷新的投影不被 Writer 抢占。
func (p CachePolicy) Exclude(ids ...ProjectionID) CachePolicy
func NewProjectionReader(store session.Store, registry *Registry, cache ProjectionCache) ProjectionReader
```

**EXT-PRJ-1** Initial、Apply、StateCodec 必须 pure。Fold 以 commit 为单位：一个 commit 内任一 event 的 Apply 失败，不发布该 commit 的部分状态。commit 边界由 kernel 给出：`ReadCommits` 逐个返回完整 commit（SES-APP-2 从不暴露不完整 commit），不依赖事件内的任何标记；`Consumes` 过滤（EXT-PRJ-2）只决定哪些事件进入 Apply，不改变 commit 边界。

**EXT-PRJ-2** 投影只处理 `Consumes` 中的 EventType。其他 EventType 按归属处理：属于本模块或 `Requires` 模块（EXT-REG-4 的范围）且 `Decode` 为 Unknown 的事件，`Ignorable` 为真则跳过，否则 Fold 失败；范围之外的模块的事件一律跳过。写入者对纯信息性事件声明 `Ignorable`（EXT-REG），默认不可忽略：忘记声明只会导致多拒绝，不会导致静默丢失。读取时以范围内模块的前缀作为 `Types` 过滤。

**EXT-PRJ-3** 缓存条目记录 `through`（`session.Head`）：`Next` 为下一个未折叠 commit 的 CommitSeq。复用条件是 **commit 对齐**：`through.Next-1` 必须是日志中一个 commit 的 Seq，且 `StateCodec.Decode` 成功；否则从 `Initial` 重折。历史不可变（SES-APP-5），位置因此足以指认该 commit。commit 对齐是 EXT-PRJ-1 的直接后果：状态只在 commit 边界上发布，不存在半应用的 commit。该判定只有一个实现（`CommitAt(commit, through)`），Writer 与 Store reader 共用，因此同一条目在两条路径上的判定相同。第二个谓词 `OwnBoundary(header, through)` 判定 `through` 是否越过 tip 段的 seed（`through.Next > LedgerSeed(header).Next`）：条目记录的状态是在记录时 tip 的继承策略下折出的，fork 使此前的全部 commit 变为继承 commit，因此落在继承 commit 或继承边界上的条目不被复用，折叠从 `Initial` 重来，直到 tip 持有自己的 commit；Writer 与 reader 同样共用该谓词。相应地，没有自身 commit 的 tip（空 fork）在 `Close` 时不写任何条目。缓存是派生数据：条目缺失、无法解码或超出日志长度都只导致重折，不产生错误。缓存按 SessionID 归属：fork 首次打开时折叠继承前缀，此后以自己的条目续折；。

**EXT-PRJ-4** `Writer.Projections()` 返回的 reader 从 Writer 内存状态生成独立副本；`View.Projection` 同样通过该投影的 `StateCodec` encode/decode 交付调用方拥有的副本，包括嵌套 map、slice 与指针。独立进程的观察者用 `NewProjectionReader` 从 Store 读，两者对同一 head 给出相同状态。Writer 在 `Append` 之前折叠一个临时 commit（Seq 为当前 head 的 Next，带 CommitID 与全部 batch），reader 折叠 kernel 存储的 commit；kernel 在 Append 内只赋 Seq，Apply 只见到 `DecodedEvent`（stream、事件与解码值），因此两条路径向 Apply 交付相同输入。

**EXT-PRJ-5** `OpenWriter` 的重建分两步：先为每个投影判定缓存条目（EXT-PRJ-3），判定只读条目所指的那一个 commit（经 CommitIndex 的字节区间，SES-REP-5），得到每个投影的续折起点；再从全部起点中最小的那个开始读日志一次，各投影从各自起点续折，没有可用条目的投影从 `Initial` 全折。干净 Close 后每个条目都在 head，读取为空；崩溃后读取量不超过 `CacheEvery(n)` 的 n 个 commit。篡改或过期的条目只让该投影多折一次，绝不影响正确性，也绝不让 `OpenWriter` 失败。这次读取只服务于投影，不服务于提交历史——Writer 不保留日志，也不建 CommitID 索引（EXT-WRT-1）。

**EXT-PRJ-6** 写入与读取的权限不对称：`WritersConfig.CachePolicy` 只决定 Writer 写哪个投影的条目；读取一律尝试缓存中的条目，不论谁写的。某个投影的条目由它的宿主在语义检查点上写入时（run 的 machine projection 经 `SnapshotPolicy`，见 RUN-CMT-2），组装层用 `CachePolicy.Exclude` 把它排除，Writer 便只读不写，绝不会把条目落在检查点之间。

**EXT-PRJ-7** 缓存是派生数据，写入尽力而为：`Save` 失败只让下次多折，不影响 Commit 结果。刷新在 Writer 的互斥区之外执行：策略判定与状态快照在区内完成（状态按 EXT-PRJ-1 不可变，快照即引用），编码与 `Save` 在解锁后进行，因此缓存 IO 不延长事务边界，与后续提交也没有顺序约束。区间是部署参数而非常量：`CacheEvery(n)` 的 `n` 由部署给出，`n <= 0` 才取 `DefaultCacheEvery`，且必须能在不改代码的情况下调整：宿主层把它暴露为 `Ports.CacheEvery`（APP-MEM-2），换值即换代价，不必重编译。间距给出可依赖的代价上界：任何时刻条目落后 head 不超过 `n` 个 commit，续折不超过 `n` 行；`Close` 不例外，`CacheEvery(n)` 在关闭时仍按间距判定，因为大投影（chatlog surface 随历史增长）在每次关闭时整体写一次，累计写量随历史长度平方增长，而按活动工作获取所有权的部署（APP-ACT）每 Turn 都关闭。需要干净 Close 后零续折的部署以 `CachePolicy.AtClose()` 包装策略。`Save` 单调：条目的 `through` 只增不减，晚到的旧写入被丢弃（刷新在互斥区外，两次刷新可能乱序到达），三个实现（内存、filestore、Postgres 的条件 UPSERT）均如此。被 `Exclude` 的投影在关闭时也不会被写入。

**EXT-PRJ-8（继承策略）** `ProjectionDefinition.Inherits` 是按逻辑流判定的谓词 `InheritPolicy func(session.StreamRef) bool`，声明投影从 fork 继承前缀中折叠哪些流。nil 为按声明的 lineage 继承：继承 commit（`Seq <= header.Parent.Seq`）只折叠 domain 声明为 `LineageSession` 的流批次，`LineageSegment` domain 的批次跳过（经 `Registry.LookupStream` 查声明）；`InheritAll`：继承 commit 的全部批次都折叠；`InheritStreams(domains...)` 折叠列出的 domain。fork 语义由此随 domain 的声明进入投影，Registry 不含任何 domain 的特判。tip 段的 commit 总是全部折叠。`Registry.FoldFrom(scope, state, commits, header)` 以 Session 的 tip header 判定继承边界，`Writer.rebuild` 与 `ProjectionReader` 都经它折叠；`Fold` 等价于无 Parent 的 `FoldFrom`，用于只含 tip commit 的折叠（provisional group）。默认值使执行状态投影（`twilight/run` 的 Machine）不把父的 Run 当作子的执行（SES-FRK-5）；chatlog 的 Surface/Context 与 turn 的 Surface 声明 `InheritAll`，因为它们的语义内容（assistant、tool_result、attempt 结算）来自 run 事实。app module 不声明时得到默认值。

**EXT-PRJ-9（authoritative 与 derived）** 投影是 `Facts -> View` 的读模型，不是写入校验器：写入时的不变量由 unit of work 的各 Part 在同一 View 上判定（SES-ATM）。`ProjectionDefinition.Authoritative` 标出命令在 Writer 的 View 上据以规划、且其 fold 守护自身流不变量的投影——`twilight/run/machine`、`twilight/turn/surface`、`twilight/chatlog/surface` 与 `context`——它们对 provisional commit 的 fold 失败使 commit 为 `invalid`，这表示 Part 与投影不一致的缺陷，而不是业务拒绝。其余投影是 derived read model：fold 失败不阻止事实落盘，该投影在本 Writer 生命周期内标记为不健康（状态停在最后一次成功的 commit，`View.Projection` / `Writers.Projections()` 读取返回 `ErrProjectionUnhealthy`，缓存不再为它刷新）。`OpenWriter` 的重建同样：derived 投影折不过 log 时折到最后一个成功的 commit 并标记不健康，Session 照常打开；只有 authoritative 投影折不过 log 才使 `OpenWriter` 失败。一个 extension 的缺陷因此既不能让 Session 不可写，也不能让它下次打不开。`Authoritative` 是能力而不是自我声明：它取决于模块如何进入 registry。`BuildRegistry(protocol, modules...)` 的模块全部由调用方担保为可信 core；`BuildRegistryWithExtensions(protocol, core, extensions)` 中的 extension 不得声明 `Authoritative`，也不得使用 `SourceTwilight`，因此 descriptor 无法为自己伪造第一方身份。Authority 以 first-party 三模块为 core、`Ports.Modules` 为 extensions 构建。chatlog 的 Context 投影保持 authoritative，因为 `chatlog.Commands.Compact` 在提交临界区内读它计算 base digest。

## 7. errors 与 conformance

```go
type ErrorCode string
const (
    ErrInvalid ErrorCode = "invalid"; ErrUnknownEvent ErrorCode = "unknown_event"
    ErrCodec ErrorCode = "codec"; ErrBinding ErrorCode = "binding"
    ErrConflict ErrorCode = "conflict"; ErrOwnershipLost ErrorCode = "ownership_lost"
    ErrProjectionUnhealthy ErrorCode = "projection_unhealthy" // derived 投影折叠失败后的读取（EXT-PRJ-9）
    ErrUnknownOutcome ErrorCode = "unknown_outcome" // Append 结果未知，Writer 已失效（EXT-WRT-4）
)
```

v1 conformance 必须验证：

- **EXT-REG-1 至 4**：immutable Registry、Encode 写入类型的 `Version`（缺省为最大 codec 键、声明值无 codec 时构建失败）、按 payload 的 `v` 选择 codec、多版本 codec 共存、Unknown 保留 raw payload、`Requires` 缺失或成环被拒绝、`Requires.Events` 指向非被依赖模块的事件被拒绝、投影消费范围外事件被拒绝；Source 段非法（空、含 `/`、非 UTF-8）被拒绝、`(Source, ID)` 重复被拒绝、同名 ModuleID 在不同 Source 下共存且各自前缀可解析；
- **EXT-COD-1/2**：wire-first、`v` 保留字段；canonical round-trip 由各模块的测试覆盖；
- **EXT-REF-1/2**：Extractor 全量提取、cardinality、scheme/durability admission、拒绝时无写入；
- **EXT-WRT-7**：每个 applied group 恰通知一次、行与日志一致、顺序与 Seq 一致（含并发提交）；被拒与重放不通知；观察者 panic 不影响 Commit；
- **EXT-WRT-1 至 5**：OpenWriter 后投影等于全量 fold 且 Writer 不保留日志（重开后常驻内存不随日志长度增长）；同 CommitID 重放 AlreadyApplied、不同内容 Conflict、两者无写入；并发调用方串行且各自看到前一次的结果；claim 先于 append，已确认写入前拒绝时释放 claim，结果未知时保持 Active 至重开核对；`ErrOwnershipLost` 后 Writer 失效；Append 在底层持久化之后返回错误时 Writer 以 `ErrUnknownOutcome` 失效、重开后同一 group 为 `AlreadyApplied`，claim 保持 Active，后续 Seq 连续、链完整；Append 在写入之前发生结果未知的错误时，重开核对释放孤儿 claim；同一提交经历多次写入前失败与重开后，通过后继 claim 成功提交且仅保留一个 Active root；owner 或 BindingSet 冲突时拒绝派生后继；
- **EXT-PRJ-1 至 4**：pure fold、组边界、Consumes 与范围外跳过、Ignorable 与非 Ignorable 的 Unknown、缓存复用条件、Writer 内投影与 Store 读取一致；修改 `Projections().Load` 或 `View.Projection` 返回的嵌套状态后，后续读取与提交仍保持原事实流的投影。
- **EXT-STR-1**：未声明的 domain、他模块的 domain、重复的 domain、缺失或未知的 `Lineage` 使 `BuildRegistry` 失败；事件放入其他 domain 的 batch、单例流的 batch 带 ID、键控流的 batch 无 ID 或 `Key(value)` 与 batch 不符的 group 被 Writer 拒绝；
- **EXT-PRJ-8**：默认继承策略下继承 commit 中 `LineageSegment` domain 的批次不进入折叠、tip commit 全部折叠；`InheritAll` 折叠继承 commit 的全部批次；Writer 与 ProjectionReader 对同一 fork 折出相同状态；
- **EXT-PRJ-9**：非 authoritative 投影的 fold 失败不阻止 commit，该投影此后读到 `ErrProjectionUnhealthy`、不再写缓存，authoritative 投影不受影响；authoritative 投影的 fold 失败使 commit 为 `invalid`；
- **EXT-WRT-8/9**：Fork 之后唯一覆盖前缀内容的 claim 是父段的 commit claim，打开子不释放它；删除父后该 claim 仍 Active、子仍读到前缀；Collect 截断父段时只释放被截 commit 的 claim；删除全部到达者并 Collect 后 claim 释放；重复 Delete 返回 nil；
- **EXT-PRJ-5 至 7**：`AtClose` 下干净 Close 后重开不折任何 event，`CacheEvery(n)` 下 Close 不写落后不足 n 的条目且重开只折该尾部；晚到的旧 `Save` 不回退条目；落在继承 commit 或继承边界上的条目不被复用，没有自身 commit 的 tip 在 Close 时不写条目；条目只覆盖前缀时只折尾部；日志越界、落在组内、状态不可解码的条目一律回退为全折且不使 Open 失败；被策略排除的投影不被写入，但它已有的条目仍被复用；`CacheEvery(n)` 下条目落后不超过 n 行；未配置缓存时不写任何条目且行为不变。

## 8. Application module

Application 在自己的代码里定义 `ModuleDescriptor`（自有 Source 下的事件类型、codec、投影），经装配开口（`authority.Ports.Modules`，经 `app.Config.Modules` 传入）与 first-party 模块一起传入 `BuildRegistry`。app module 与 first-party 模块同构、同权：同一 Registry、同一 `Writer.Commit` 提交路径、同一投影框架。

**EXT-APP-1（承诺面）** app module 的 `Requires` 可依赖 first-party 模块的事件；四个 first-party 模块各事件解码后的当前类型即稳定消费面：first-party 为某事件发布新版本时，其 codec 把新旧版本都 upcast 到该类型，app module 不需要重新注册任何东西（EXT-REG-2）；app module 自己的事件同样按自己声明的版本编码，与 first-party 的版本无关。

**EXT-APP-2（隔离）** EXT-PRJ-2 的范围规则双向保护：first-party 投影对 app 模块（范围外）的事件一律跳过；app 投影对未列入其 `Requires` 的模块同样跳过。app module 未注册时，其历史事件对所有投影是范围外事件，按 EXT-REG-3 保留原始 payload、不参与折叠。

**EXT-APP-3（适用判据）** 需要"持久、可重放、参与投影"的事实才建 module；工具、模型、系统提示、prompt builder、观测 sink 走既有接口扩展点（宿主层的 AgentPreset 与 Executor、EventSink、Store adapter），不进 Session ledger。

模块以 Go 值直接传入 `BuildRegistry`；把多个 Source 的 ModuleDescriptor 与 artifact SchemeDefinition 组合为只读索引的通用 `Catalog` 不在本层的职责内。
