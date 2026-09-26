# Twilight Agent Session Protocol

状态：v1 设计规范（commit ledger）。本文定义 Session ES kernel。本文档随 feat/agent-runtime 分支首次发布；定稿前的行格式（一行一个 SessionEvent、按行 digest）从未对外发布，由 commit ledger 取代。

本文定义 Twilight Session 的 Event Sourcing kernel。文中的"必须""不得""应该"是协议约束。

## 0. 设计原则

Twilight Session 是一个单写者、可接管、幂等提交的 Event-Sourced Aggregate：Commit Ledger 是事实的唯一权威，Writer 提供语义事务与串行化，kernel 只提供原子 append、ownership/fencing 与持久化，Projection 与 Snapshot 全部是可重建的派生状态。下列原则贯穿本文与 [Session Module Framework](agent-session-extension.md)、[Decision](agent-decision.md)、[Run](agent-run.md)、[Turn](agent-turn.md)、[Chatlog](agent-session-chatlog.md)、[Artifact](agent-artifact.md)、[Runtime](agent-runtime.md) 各规范；每条给出承载它的条款。

```text
                  Session

      ┌── Canonical Commit Ledger ─┐
      │                            │
Command                            │
  ↓                                │
Writer.Commit                      │
  │                                │
  ├─ semantic serialization        │
  ├─ transaction boundary          │
  ├─ validation                    │
  ├─ idempotency                   │
  ↓                                │
Atomic Commit ─────────────────────┘
             │
             ↓
      Projections / Snapshots
             │
             ↓
       Runtime / Context / UI
```

1. **唯一权威历史。** `Session = append-only Commit Ledger`，`State = Fold(Events)`。Turn、Run、Chatlog 不各自持有 Session 的权威状态，它们的事实同在一条 ledger 上，按各模块声明的逻辑流分组（chatlog、turn/&lt;TurnID&gt;、run/&lt;RunID&gt;，EXT-STR-1），CommitSeq 是权威全序，StreamSeq 只是读优化（§3）。stream 之外还有三类有明确 owner 的持久数据：内容寻址的 artifact `cas` ContentStore 存内容本体；artifact 的 `RetentionLedger` 保存 claim；Executor 的 durable Execution Store 保存已接受 Assignment 的执行状态与结果。前两类中被 Session 引用的内容以 digest 锚定，模型请求本体作为 Assignment 内容从 authority 传递到 executor，Run 事实只记其 digest，恢复不依赖 authority 的短期本体（RUN-WIR-4、RUN-CMT-7）。Execution Store 是效果层的 authority，不是 Session 事实的第二份来源；Session 只通过 Assignment/Outcome 与它交互。claim 先于 Append 建立（EXT-WRT-3）。

2. **语义串行化。** 同一 Session 的全部写入（Turn、Run、恢复、Compaction）经进程内唯一的 `Writer.Commit`，形成一个确定的全序（EXT-SCP-1、EXT-WRT-1）。kernel 不承担并发控制（SES-SCP-2）。

3. **事务边界。** `read state → decide → validate → append` 在 Writer 的互斥区内完成，不可被另一个语义提交插入：`CommitFn` 经 `View` 读取的 head、提交历史与投影状态即写入时的状态，fn 自身不做外部 IO（EXT-WRT-1）。validate 有两层：Binding admission（EXT-REF-2）与投影预折叠——任一投影拒绝则不落盘（EXT-PRJ-1）。这条边界是进程内的；跨进程的隔离由第 5 条提供，两者合起来才是完整的隔离。

4. **原子 Commit。** 一个领域动作产生的多个 event 要么全部出现，要么全部不存在（SES-APP-1）；底层事务或 fsync 只是它的物理实现。崩溃只可能留下一个不完整的尾 Commit，`Open` 在确立 head 之前把它截掉，reader 在任何时刻都看不到不完整的 Commit（SES-APP-2）。

5. **Session 级 Ownership 与 Fencing。** `Handle + Epoch + Lease`：同一 Session 同一时刻至多一个有效写者；接管使 Epoch 加一并持久化，旧 Handle 的迟到写入被拒（SES-OWN-1/2）。所有权是 Session 级而非执行目标级：接管者对全部执行中的目标查询，并根据结果重连、延迟或处置（SES-OWN-3、RUN-CMT-7）。何时允许接管由租约回答：持有者按 `LeaseDuration` 续租（Writer 心跳，EXT-WRT-11），租约过期后另一个 Open 无需 `Takeover` 即可接管；对仍存活的租约强制接管是 kernel 之上的运维决定。安全性在任何情况下都由 Epoch 承担。

6. **幂等语义提交。** `CommitID` 命名操作：写者派生 CommitID，使同一 ID 只对应一个操作；已提交的 CommitID 再次提交为 `AlreadyApplied`，返回原 commit，不写入也不重建事件（EXT-WRT-2）。kernel 拒绝重复 CommitID 并提供该索引的读侧（SES-APP-3、SES-REP-3/4）。kernel 与 Writer 都不比对两次提交的内容：一个 ID 是否只对应一个操作，由派生 ID 的写者保证（SES-APP-4）。恢复与重放因此不会重复写事实。

7. **Projection 与 Snapshot 只是派生状态。** Projection 可重建，Snapshot（投影缓存）可丢弃；复用条件是 Commit 边界对齐（EXT-PRJ-3），篡改或过期的条目只让下次多折，绝不成为第二份 authority（EXT-PRJ-5/7）。owner 进程内的投影与观察者从 Store 折出的投影对同一 head 给出相同状态（EXT-PRJ-4）。

8. **最小化、payload-opaque 的 kernel。** kernel 只懂 Open/ownership、Append、Read、Seq、CommitID 索引（SES-SCP-1/3、第 4 至 6 节）。它不解释 payload，不知道 Turn、Run、Tool、Compaction 是什么；领域语义全部在 Module、Writer 与 Projection 层。

9. **历史不可变由 Store 保证。** adapter 端口只有 append 与 read，没有改写或删除单个 Commit 的操作；唯一约束 `(segment, seq)`、`(segment, commit_id)` 与 Lease 检查在同一事务内完成（SES-APP-5）。`(SegmentID, Seq)` 因此永久指向同一个 Commit，fork 边与投影缓存都只以位置引用它（SES-FRK-1、EXT-PRJ-3）。kernel 不在协议内计算 hash 链（SES-WIR-2）：存储层的完整性手段（事务、校验和、备份校验）属于部署，不属于 Agent Core。

10. **模块隔离与版本独立。** 事件按 `<source>/<module>/` 归属，`Requires` 图决定投影的消费范围：范围外事件跳过，范围内不可忽略的 Unknown 事件使折叠失败（EXT-REG-1/4、EXT-PRJ-2）。payload 版本 `v` 属于事件类型、由模块携带（SES-VER-1）；kernel 自身没有版本号：header 与 commit 的结构以字段的存在与否演进（SES-VER-2），段不携带任何模块层版本，kernel 不读取 Ext 中任何模块的值。application module 与 first-party 模块同构（EXT-APP）。

11. **事实只 canonical 一次。** 一个模型或工具结果在 ledger 上只有一份表达：Run 事实记录 digest，正文在 `frozen.Store`；对话条目与 Turn 结算是这些事实的纯投影，读取时经 materializer 取回正文（RUN-WIR-4、TRN-MAP-1、Chatlog 第 1 与第 8 节）。一次语义操作仍可以在一个 Commit 内写多个 domain 的事实（Run 的 `input_accepted` 与 Chatlog 的 `input_delivered`），它们是各自 domain 的真实事实，不是同一事实的两种表示。run/&lt;RunID&gt; 流因此是 canonical history 的一部分，不能独立于其他流回收；正文可以迁移到冷存储，不得丢弃。

12. **崩溃后果的封闭集合。** Session 崩溃只可能留下不完整尾 Commit（第 4 条）与孤儿 claim（回收前核对释放，ART-RET-3）；Executor 崩溃还可能留下需要查询的 durable execution record。Session 接管者询问执行目标后重连或处置；两者都不触发自动重试。崩溃恢复、语义重试、重新生成回答和外部效果未知的身份边界见 TRN-DUR-1 至 4；Execution Store 的恢复由效果层合同负责。

13. **读不需要所有权。** 任何进程可随时读完整 Commit 构成的前缀（SES-OWN-4）；观察者用 `NewProjectionReader` 从 Store 折叠，与 owner 一致（第 7 条）。

## 1. 范围

```text
Events = 一条 Session 的 commit ledger：有序的原子 Commit 日志，一个 Commit 内含若干按逻辑流分组的 batch
State  = Fold(Events)

Session lineage 树（第 8 节；每个根段下的节点为一棵树，全部根段为森林）：
  节点  = Segment：不可变的创建记录（SegmentHeader，不含任何 Session 身份）加它自己的只追加 commit，身份为 kernel 随机抽取的 SegmentID
  边    = LedgerRef：子 Segment 到父 Segment 某个 commit 的引用（SegmentHeader.Parent，每段至多一条）
  根    = SessionRecord：SessionID → 它追加到的 Segment（Tip）与 Session 自己的元数据
  路径  = Ancestry：从根段到该 Session tip 段的唯一 Segment 序列，及每段在拼接序列中贡献的区间

kernel 负责：Segment/LedgerRef/SessionRecord/Ancestry 的语义、Commit（Seq、CommitID、批次）、原子的 Commit 追加、根级写者独占、按 Ancestry 拼接的 CommitSeq 顺序读与流读、fork、tip 段推进、删除、可达性回收
adapter 负责：Backend——LedgerStore（存节点：段的创建记录与自身 commit）与 SessionStore（存根：记录与 Lease）
modules 负责：event ontology、typed codec、payload 版本、投影、投影缓存、幂等重放、并发串行
```

kernel 的 `session.Ledger` 实现 `Store`，只依赖 `Backend` 端口；Memory 与文件 adapter 只实现该端口。lineage 树由 Go 领域类型定义并由存储持久化；存储布局不定义它。

**SES-SCP-1** kernel 不解释 payload，不校验 payload 的 schema，不知道模块、commit 的语义、投影或 lease。它保证三件事：日志只能追加，已接受的 Commit 不被改写或删除；同一时刻一个 Session 至多一个有效写者；一次 `Append` 的整 Commit event 同时可见或同时不存在。

**SES-SCP-2** 并发不在 kernel 解决。一个 Session 的全部写入者（Run 的 worker、Turn 的 Coordinator、恢复流程）在进程内经同一个 `writer.Writer` 串行（EXT-WRT），它持有 kernel 的所有权句柄 `session.Handle`。kernel 只拒绝不持有有效所有权的 `Append`。

**SES-SCP-3** kernel 的范围是 Session lineage 树：header、Open/Append/ReadCommits/ReadStream、所有权与 epoch、fork、删除与可达性回收（第 8、9 节）。lineage 的单父不变量见 SES-LIN-1：多父 merge 被排除在模型之外；canonical import 不属于当前合同，若日后加入，它与 fork 一样只能新建根段或子段，不得为已有 Session 增加第二个父节点。

**SES-SCP-4** adapter 端口是 `Backend = LedgerStore + SessionStore + CreateSession`。`LedgerStore` 存节点：`Segment`、`ListSegments`、`ReadSegment`（只读该段自身的 commit）、`Locate`、`LookupCommit`、`StreamHead`（段内一个流在给定 Seq 之前的事件计数，SES-REP-3）、`Summarize`、`Index`、`PutIndex`（段的 CommitIndex 摘要、全量与重建写回，SES-REP-5）、只插入的 `Append(lease, segment, commit)`、`TruncateSegment`、`RemoveSegment`（只由 `Collect` 调用，SES-APP-5）。`SessionStore` 存根：`Record`、`ListRecords`、`Acquire`（所有权、租约与 torn tail 修复）、`Renew`、`Release`、`DeleteRecord`。两者共享一个一致性域，使 `Append` 能与 Lease 检查原子进行。adapter 不知道 fork、前缀与可达性；`Ledger` 在该端口之上一次实现 SES-FRK 与 SES-GC。conformance 以 `Store` 为参数运行，因此每个 adapter 得到同一套 lineage 语义。

## 2. 版本

kernel 没有 wire 版本号。header 与 commit 只有少数字段，新增可选字段走各模块的 `Ext` 槽（SES-WIR-5），JSON 解码容忍未知字段；版本只存在于 payload（SES-VER-1）。

**SES-VER-1** payload 的版本属于事件类型，由模块负责：每个 payload object 第一层携带整数字段 `v`，即写入时该事件类型 codec 的版本；读侧按 `(EventType, v)` 选 codec，模块为它发布过的每个版本永久保留 codec，并在 codec 内 upcast 到当前内存类型（EXT-REG-2）。kernel 不读取该字段。同一段、同一 Commit 内不同事件类型的 `v` 可以不同；段不携带任何模块层版本，kernel 把 metadata 作为不透明值原样保存，不读取其中任何键。

**SES-VER-2（kernel 结构不设版本）** header（`ID`、`Parent`、`CausationID`、`Ext`）与 commit（`Seq`、`CommitID`、`Batches`、`Ext`）的字段集合不带版本号。kernel 需要新的可选信息时，写入 `Ext` 的 `twilight/session` 槽，旧 reader 原样保留；改变既有字段含义的修改不属于本合同，需要新建 Store 或经外部迁移工具重写。没有"推进 tip 段"这类按版本切段的机制：段只因 fork 而产生。

**SES-VER-3（派生身份不嵌版本）** 凡是生命周期长于一个段的派生身份（ClaimID、CommitID、SessionID、模块的 command 与 fact identity），其预映像都不得包含任何 kernel 层版本；各层自己的预映像版本由该层的常量给出（EXT-WRT-5）。

## 3. wire types

```go
type SessionID string
type CommitID string
type EventType string
type CommitSeq uint64   // Commit 在 ledger 中的位置，从 0 连续递增，是权威全序
type StreamSeq uint64   // event 在其逻辑流内的位置，从 0 连续递增；读优化
type Epoch uint64       // 写者所有权代数，从 1 递增

type StreamRef struct { Domain string; ID string } // 逻辑流坐标。Domain 由拥有它的模块声明（EXT-STR-1），kernel 不命名任何 domain；ID 为空是单例流，非空是该 domain 下以 ID 区分的一条键控流
type StreamLineage string                          // 读流时穿越段边的方式，由流所属 domain 的模块声明（SES-FRK-5）
const (
    LineageSession StreamLineage = "session" // 继承前缀加自身段：该 domain 的流是 Session 的语义历史
    LineageSegment StreamLineage = "segment" // 只读 tip 段自身：该 domain 的流是写入它的那个段的历史
)

type SegmentID string                 // 段身份：kernel 创建段时抽取的 128 位随机数的 hex
type LedgerRef struct { Segment SegmentID; Seq CommitSeq } // lineage 树中的一个位置：某段的某个 Commit

type SourceID string; type ModuleID string
type ModuleKey struct { Source SourceID; ID ModuleID } // 模块身份，wire 上为 "source/id"（EXT-REG-1）
type RawValue []byte                                    // 一个模块的扩展值：kernel 原样保存、不解释的 JSON
type Extensions map[ModuleKey]RawValue                  // header 与 commit 的模块扩展槽（SES-WIR-5）

type SegmentHeader struct {          // 段的创建记录：lineage 树的节点，不含 Session 身份
    ID SegmentID
    Parent *LedgerRef                 // nil 为 root segment；非 nil 为该段唯一的父边，见第 8 节
    CausationID es.CausationID
    Ext Extensions                    // 按模块分槽的扩展值，缺省为空（SES-WIR-5）
}
type SessionRecord struct {           // 根：Session 身份与它追加到的段
    ID SessionID
    Tip SegmentID
    CreatedAtUnixMilli int64
}

type Event struct {
    Type EventType
    RecordedAtUnixMilli int64
    Payload jsonstable.Value
}

type StreamBatch struct {
    Stream StreamRef
    Events []Event // 非空
}

type Commit struct {
    Seq CommitSeq         // 在段内的位置；(SegmentID, Seq) 永久指向这一个 Commit（SES-APP-5）
    CommitID CommitID     // 产生该 commit 的操作的身份；重放按它判定（SES-APP-4）
    Batches []StreamBatch // 非空；同一 Commit 内每个流至多一个 batch
}
type Head struct { Next CommitSeq } // 空日志为 LedgerSeed(header)：根段 0，fork 产生的子段 Parent.Seq+1
```

**SES-WIR-1** identity 非空、稳定、有效 UTF-8。`CommitSeq` 从 `LedgerSeed(header).Next` 连续（根段从 0，fork 产生的子段从 `Parent.Seq+1`，见第 8 节）；一次 `Append` 持久化恰好一个 `Commit`，`CommitID` 在同一 ledger 内唯一。每个 batch 的流归因必须合法：`Domain` 非空且不含 `/`，`Domain` 与非空的 `ID` 都须是合法的身份字符串，kernel 只校验这一形状，domain 的归属与 ID 的绑定由模块层校验（EXT-STR-1）；同一 Commit 内同一流至多一个 batch，每个 batch 与每个 Commit 都非空。`Payload` 必须是 canonical JSON object（RFC 8785）：模块以 canonical 字节派生 command 与 fact 的身份，kernel 只校验形状。event 不携带事务元数据（无 Seq、Index、SourceSeqs、Ignorable）：事件的权威顺序由 CommitSeq 加上其在 batch 内的位置决定。

**SES-WIR-4（四种身份）** `SessionID` 是根（分支）身份；`SegmentID` 是历史节点身份，由 kernel 在创建段时随机抽取，不由任何字段派生；`CommitID` 是语义操作身份，在一个 Session 的拼接历史内唯一；`(SegmentID, Seq)` 是 Commit 的位置身份，历史不可变（SES-APP-5），因此永久指向同一个 Commit。段的创建记录与 commit 都不含 `SessionID`：段是 lineage 树的 canonical 对象，被根命名但不属于任何一个根。删除、重建、重命名 Session，或把段集合与根集合一起迁移到另一个 Store，都不改变任何段或 commit 的身份。

**SES-WIR-2（不含内容 hash）** header 与 commit 都不携带内容 digest，也不链接前一个 Commit：Commit 由 `(SegmentID, Seq)` 定位、由 `CommitID` 命名。历史不可变由 Store 的写入契约保证（SES-APP-5），不由协议层重算 hash 发现；存储损坏的检测与恢复属于存储层（事务、校验和、备份）。写者是谁由 `Append` 的 Lease 在 adapter 处核对（SES-OWN-2），不进入 Commit；出处在需要时是模块自己的事实。内容寻址只用于 artifact 与冻结正文（ART、RUN-WIR-4），不参与 ledger 的身份。

**SES-WIR-5（模块扩展槽）** `SegmentHeader.Ext` 是按模块分槽的扩展值：`Extensions = map[ModuleKey]RawValue`，键是模块身份（EXT-REG-1，wire 上为 `"source/id"`），值是该模块自己的 JSON。kernel 只校验键的形状与值为合法 JSON，不解释任何值，不认识某个模块的 reader 原样保留其条目（`RawValue` 逐字节往返）。每个模块只读写自己的键：spawn 在子段 header 的 `twilight/spawn` 槽记录 provenance（SPN-2）；kernel 自身若需要可选字段，使用 `twilight/session` 槽。header 没有其他调用方字段。缺省为空；空值、非法 JSON 或非法键为 `ErrInvalid`。`Create` 的幂等判定比较 Ext（SES-CRT-1）。commit 没有扩展槽：模块写入 commit 的一切都是事件。

## 4. 所有权

```go
type OpenOptions struct {
    // Takeover 为假时，租约仍存活的 Session 的 Open 返回 ErrOwned；为真时接管存活的租约：Epoch 加一，
    // 旧持有者被 fencing。租约已过期时无需 Takeover。
    Takeover bool
    Owner string                // 持有者标识，进入 Lease，只供诊断
    LeaseDuration time.Duration // Acquire 与每次 Renew 之后租约存活的时长；0 为直到 Release 才失效
    Clock func() time.Time      // 租约计时的时钟；nil 为 time.Now，夹具注入以推进时间
}
// Handle 是 kernel 的所有权句柄，由 Store.Open 返回；进程内的写入者是 writer.Writer，它持有一个 Handle。
type Proposal struct {
    CommitID CommitID     // 操作的身份，由写者派生（SES-APP-4）
    Batches []StreamBatch // 非空；调用方按批归因流
}
type Handle interface {
    SessionID() SessionID
    Epoch() Epoch
    Lease() Lease                  // 本句柄的租约：Epoch、Owner、到期时刻
    Renew(context.Context) error   // 把到期时刻推到 now + LeaseDuration；被接管的句柄得到 ErrOwnershipLost
    Head() Head
    Append(context.Context, Proposal) (Commit, error)
    Committed(CommitID) bool
    LookupCommit(CommitID) (Commit, bool, error)
    StreamHead(StreamRef) (StreamSeq, bool)                                    // SES-REP-3；只计 tip 段，与流的 lineage 无关（SES-FRK-5）；首问时读 backend，之后随 Append 累加
    Close(context.Context) error
}
type Store interface {
    Create(context.Context, CreateRequest) (SegmentHeader, error)   // 返回 tip 段的 header
    Header(context.Context, SessionID) (SegmentHeader, error)       // tip 段的 header
    Record(context.Context, SessionID) (SessionRecord, error)       // 根
    Open(context.Context, SessionID, OpenOptions) (Handle, error)
    ReadCommits(context.Context, CommitReadRequest) (CommitPage, error)
    ReadStream(context.Context, StreamReadRequest) (StreamPage, error)
}
```

**SES-CRT-1** `Create` 建立一个根与它的 tip 段：kernel 解析 `Fork`（SES-FRK-1）、抽取 128 位随机 `SegmentID`、写入 `SegmentHeader`，以 `Backend.CreateSession` 一步落下段与根。SegmentID 只由 kernel 抽取，调用方不能指定：可写节点的身份不对外开放，因此两个根不可能被构造成共用一个 tip（SES-FRK-4）；`CreateSession` 对已存在的 SegmentID 也返回 `ErrConflict`。对已存在的 SessionID，请求所决定的段字段（解析后的边、CausationID、Ext）都与现有 Session 相同则幂等返回现有 tip 的 header，否则 `ErrConflict`；幂等判定不比较 SegmentID，因为 ID 每次不同，也不比较 `CreatedAtUnixMilli`：它记录首次成功创建时调用方给出的时刻，超时后带新时钟重试的 Create 是重放，不是冲突。wire 夹具以 `NewLedger(be, WithSegmentIDSource(...))` 注入确定性 ID。

**SES-OWN-1** 同一 Session 同一时刻至多一个有效 Handle，有效性由租约定义：`Acquire` 记录 `Lease{Session, Epoch, Owner, UntilUnixMilli}`，`Until = now + LeaseDuration`（`LeaseDuration` 为 0 时 `Until` 为 0，表示直到 Release 才失效）；`Renew` 把 `Until` 推到 `now + LeaseDuration`，只对当前 Lease 生效，被接管的 Lease 得到 `ErrOwnershipLost`。`Open` 在租约存活（已持有且 `Until` 为 0 或晚于 now）且未声明 `Takeover` 时返回 `ErrOwned`；租约已过期时 Open 直接接管；声明 `Takeover` 的 Open 接管存活的租约。三种接管都使 Epoch 加一，安全性一律由 Epoch fencing（SES-OWN-2）承担：过期本身不终止所有权，未被接管的过期持有者仍可写入，被接管的持有者在下一次 `Append` 或 `Renew` 被围栏。时钟由 `OpenOptions.Clock` 给出，adapter 不自带时钟；这与 Execution Store 的 record 租约（RUN-EXE-6）形状相同。

**SES-OWN-2** 每次成功的 Open 使该 Session 的 `Epoch` 加一并持久化。`Append` 携带 Handle 的 Epoch；Store 对落后于当前持久化 Epoch 的调用返回 `ErrOwnershipLost`，不写入任何内容。这是 fencing：被接管的旧 Handle 的迟到写入不可能进入日志。

**SES-OWN-3** 所有权是 Session 级的，不是执行目标级的。一个进程取得 Session 的所有权即拥有其中全部执行；接管者读日志后对所有仍在执行中的目标做询问后处置（RUN-CMT-7）。kernel 不知道"执行中"是什么，这一步由 run 模块在 Writer 上完成。

**SES-OWN-4** `ReadCommits` 与 `ReadStream` 不需要所有权，任何进程可以随时读；读到的是完整 Commit 构成的前缀（SES-APP-2）。

**SES-OWN-5（租约读取）** 租约可以被任何进程读取，与所有权无关。`LeaseOf(sid)` 返回该 Session 当前持有的 `Lease`，ok 为 false 表示根存在但没有持有者（从未 Open，或已 Release）；根不存在为 `ErrNotFound`。`ListLeases()` 返回每个有持有者的根的 Lease，不按存活过滤：过期但未被接管的租约照样返回，`UntilUnixMilli` 由调用方对照自己的时钟判定。`ExpiredLeases(before, limit)` 返回 `UntilUnixMilli` 不为零且不晚于 before 的持有中租约，按到期时间升序，最多 limit 条（0 为全部）：这是 owner 副本池恢复死亡 owner 的 Session 所用的读取（APP-ACT-3），adapter 为它建索引，读取成本与页大小成正比而与 Session 总数无关。三者都是读取，不改变 Epoch，不触发接管；调用方据此做的决定（对过期租约的 Session 发起 `Open(Takeover)`、把命令路由到 `Owner` 所在进程）仍经 Open 与 Append 的规则裁决，读到的租约在下一刻可能已被替代。这是 controller 发现过期 Session 与 gateway 路由命令的依据（CLD-CTL-2、CLD-GWY-2）；形状与 Execution Store 的 `LeaseOf` 相同（RUN-EXE-6）。

## 5. append

**SES-APP-1** `Append(proposal)` 原子：整个 Commit 同时可见或同时不存在。Store 为 Commit 赋 `Seq`（从当前 `Head.Next` 起连续，空 ledger 的 head 为 `LedgerSeed(header)`），持久化，然后返回存储的 Commit。返回即持久（文件 adapter 每次 Append 一次 `fsync`；数据库 adapter 一个事务）。写入开始之后的任何失败（write、fsync、事务提交返回错误）使该 Commit 是否落盘对句柄成为未知：句柄进入失效状态，本次与之后的 `Append` 返回 `ErrHandleFailed`，不再写入；调用方 Close 并重开，`Open` 按磁盘实况决定该 Commit 是否存在（完整则接纳进索引，残缺则按 SES-APP-2 截断），随后的重放由 `Committed`/`LookupCommit` 回答。adapter 只能在写入开始之前返回 ctx 错误；写入开始后的中断按未知结果报告。

**SES-APP-2** 崩溃只可能留下一个不完整的尾 Commit：文件 adapter 打开时把末尾帧不完整且没有后续 Commit 的尾部截掉；数据库 adapter 由事务保证不会出现。截断必须发生在 `Head` 确立之前：否则 `Head.Next` 落在残 Commit 内部，下一次 `Append` 会把残 Commit 与后续 Commit 焊成一个。reader 在任何时刻都不会看到不完整的 Commit。

**SES-APP-4（CommitID 命名操作）** `CommitID` 由写者派生，同一 ID 只对应一个操作（Run 的 CommandID、Turn 的各命令 ID、chatlog 的 compaction ID 都由操作内容或坐标派生）。kernel 对已存在的 CommitID 拒绝 `Append`（`ErrConflict`）并提供索引读侧（SES-REP-3/4）；Writer 据此对重放返回 `AlreadyApplied` 与原 commit（EXT-WRT-2）。kernel 与 Writer 都不比对两次提交的内容：同 ID 不同内容不是可判定的冲突，先落下的 commit 生效，避免这种情形是派生 ID 的写者的责任（RUN-CMT-5、TRN-EVT-2）。

**SES-APP-3** kernel 拒绝：空 Commit、空 batch、同一 Commit 内重复的流、非法流归因、重复 `CommitID`、非 canonical 或非 object 的 payload、无效 identity、落后的 Epoch。拒绝不写入任何内容，返回 `ErrInvalid`（重复 CommitID 为 `ErrConflict`）。kernel 不返回"已应用"：幂等重放由 `writer.Writer` 按 CommitID 命中回答（EXT-WRT-2），原 Commit 经 `LookupCommit` 从 kernel 取（SES-REP-4）。

**SES-APP-5（只追加）** adapter 端口没有改写或删除单个 Commit 的操作：`Append` 只插入；`TruncateSegment` 与 `RemoveSegment` 只由 `Collect` 对没有任何根到达的段调用（SES-GC-2），存活 Session 的历史不被触碰。数据库 adapter 以唯一约束 `(segment, seq)`、`(segment, commit_id)` 与同一事务内的 Lease 检查实现；文件 adapter 以单次 write 加 fsync 的整行追加实现（SES-APP-1/2）。这是 ledger 一致性的第一层保证，kernel 不在其上另设 hash 链（SES-WIR-2）。

## 6. read

```go
type CommitReadRequest struct {
    SessionID SessionID
    From CommitSeq      // 起点，含
    Limit uint32        // 0 为不限
}
type CommitPage struct { Header SegmentHeader; Commits []Commit; Head Head; HasMore bool }  // Header 为 tip 段的
type StreamReadRequest struct {
    SessionID SessionID
    Stream StreamRef   // 只读该逻辑流的事件
    Lineage StreamLineage // 必填：按流所属 domain 声明的方式穿越段边，缺失为 ErrInvalid（SES-FRK-5）
    From StreamSeq      // 起点，含
    Limit uint32        // 0 为不限
}
type StreamPage struct { Header SegmentHeader; Stream StreamRef; Events []Event; Head Head; HasMore bool }
```

**SES-REP-1** `ReadCommits` 按 `CommitSeq` 递增返回 `From` 起的完整 Commit；fork 的序列是继承前缀加自身 commit（SES-FRK-2）。`From` 大于等于 `Head.Next` 时返回空页且 `HasMore` 为假，这对 `CommitSeq` 的全部值域成立：实现必须以 `CommitSeq` 比较起点，不得先把它转换为 `int` 再索引日志。Open 读取的 Session 元数据属于校验范围：header 的 `ID` 与所在节点不一致，或所有权记录无法解析时报 `ErrCorrupt`，不得报告为不存在；残尾 Commit 的截断见 SES-APP-2。读路径信任存储。

**SES-REP-2** `StreamSeq` 是流内位置，由 Store 按 CommitSeq 顺序在 `ReadStream` 请求的 lineage 所见的序列上数出，是读侧的优化：`ReadStream` 只返回该流的事件，但其顺序与从 `ReadCommits` 折叠出的流内顺序完全一致。它不是第二种排序。

**SES-REP-3** `Committed` 报告某个 `CommitID` 是否已在 ledger 中。`Append` 必须拒绝重复 `CommitID`（SES-APP-3），kernel 因此本来就持有这个索引；`Committed` 是该索引的读侧，只做索引查找，不触碰 commit 本体：tip 段与继承段同经 `Backend.Locate` 查各段的 CommitIndex（SES-REP-5），沿 Ancestry 逐段核对 Seq 落在该段贡献的区间内；tip 段的核对以句柄自己的 head 为界（`Seq < head.Next`），head 只由句柄自身的 `Append` 推进，因此句柄知道的恰为它在 Open 时读到与自己写下的 commit：被替代的句柄不会得知继任者的 commit，仍在 `Append` 处触到 Epoch 围栏（SES-OWN-2）。句柄不在内存中持有 tip 段的 CommitID 集合，`Open` 的代价与 tip 段长度无关（APP-ACT-5）。调用者（`writer.Writer`、Run 的重放判定）不必自己再维护一份同样的索引。`StreamHead(stream)` 是同一索引的另一读侧：报告该 ledger 是否写过某逻辑流及其下一个 `StreamSeq`，只计 tip 段自身的 commit（SES-FRK-5）；句柄第一次被问到某个流时经 `Backend.StreamHead(segment, stream, head.Next)` 读取一次并缓存（同样以自己的 head 为界），之后随自身 `Append` 累加。

**SES-REP-5（CommitIndex）** 每个段有一个 `CommitIndex`：按 Seq 顺序的条目 `{CommitID, Seq, Streams}` 加它覆盖到的 head `Through`。它是段的组成部分而不是 adapter 的私有缓存：adapter 在每次 `Append` 中把该 commit 的条目与 commit 一起落下，在 `TruncateSegment` 中一起截断。不变量是条目恰好覆盖该段 `[seed, head)` 的每个 commit。`Open` 经 `Backend.Summarize` 取索引摘要（条目数、首末 Seq、`Through`，`IndexSummary.Valid`）核对：`Through` 等于段的 head，条目数等于 head 到 seed 的距离，首末 Seq 落在 seed 与 head-1；Seq 在段内唯一，三者成立即连续。通过则句柄不读任何 commit，也不复制索引，此后按需经 `Locate` 与 `StreamHead` 查询它；不通过（索引缺失、崩溃后落后于 commit、被截短）则 kernel 以 `ReadSegment` 读该段自身 commit 重建索引并经 `PutIndex` 写回。索引损坏或缺失因此只导致一次重建，不导致错误。文件 adapter 以段目录下的 `index.jsonl` 持久化，每行是条目加该 commit 在 `log.jsonl` 中的字节区间，先写 commit 再写索引行，所以崩溃至多让索引落后一个 commit，加载时由日志尾部补齐；数据库 adapter 以 `(segment, commit_id)` 唯一索引回答 `Locate`，以每 commit 每流一行的流计数表回答 `StreamHead`，两者与 commit 同事务写入。`Index` 的全量读取只剩 `Collect` 在截段前取被删 commit 的名字这一处用途。`LookupCommit` 与 `ReadSegment` 按索引给出的字节区间读取，代价与段长度无关。

**SES-REP-4** `LookupCommit` 返回某个已提交的 Commit，未提交时 `ok=false`。句柄不持有它时从存储读取：文件 adapter 按 CommitIndex 记录的字节区间读该 Commit（SES-REP-5），代价与日志长度无关；内存 adapter 复制该 Commit。代价只落在命中，未命中是一次索引查找。这是幂等重放唯一需要的读取能力：重放不必读整条日志（EXT-WRT-2）。

## 7. errors 与 conformance

```go
type ErrorCode string
const (
    ErrInvalid ErrorCode = "invalid"; ErrNotFound ErrorCode = "not_found"
    ErrConflict ErrorCode = "conflict"; ErrCorrupt ErrorCode = "corrupt"
    ErrOwned ErrorCode = "owned"; ErrOwnershipLost ErrorCode = "ownership_lost"
    ErrHandleFailed ErrorCode = "handle_failed" // 前一次 Append 的持久结果未知；句柄已失效
    ErrUnsupported ErrorCode = "unsupported"
)
```

conformance 以 `Store` 为参数，每个 adapter 跑同一套，必须验证：

- **SES-WIR-1**：CommitSeq 连续、批次非空、同 Commit 内流唯一且归因合法、CommitID 唯一、payload canonical；
- **SES-OWN-1/2**：第二个 Open 返回 `ErrOwned`；Close 后可再 Open 且 Epoch 加一；声明 `Takeover` 的 Open 在所有权存续期间接管且 Epoch 加一；旧 Handle 的 Append 返回 `ErrOwnershipLost` 且不写入；带 `LeaseDuration` 的租约在存活期内拒绝无 Takeover 的 Open，Renew 延长存活期，过期后无 Takeover 的 Open 接管且 Epoch 加一，过期持有者的 Renew 与 Append 为 `ErrOwnershipLost`、其 Close 不释放新持有者；`LeaseDuration` 为 0 的租约不过期；
- **SES-OWN-5**：Open 前 `LeaseOf` 为 ok=false；Open 后返回该 Handle 的 Lease；Renew 后 `UntilUnixMilli` 更新；过期未接管时仍返回旧持有者且出现在 `ListLeases` 与 `ExpiredLeases`；存活期内不在 `ExpiredLeases` 中；`ExpiredLeases` 的 limit 生效；接管后返回新持有者；Close 后 ok=false 且不在 `ListLeases` 中；不存在的 Session 为 `ErrNotFound`；
- **SES-APP-1/2/3**：整 Commit 可见性；在 Commit 中途注入崩溃后打开，尾 Commit 不出现；拒绝项无写入；注入持久化失败后句柄返回 `ErrHandleFailed`，重开后已落盘的完整 Commit 在索引中、同 CommitID 的 Append 为 `ErrConflict`；
- **SES-REP-5**：索引与 commit 一致、落后一个、落后全部、缺失四种情形下 Open 成功且 Head、Committed、StreamHead、LookupCommit、重复 CommitID 的拒绝与 ReadCommits 都与从 commit 得到的答案相同；fork 子对继承 CommitID 的 Committed 经父段索引回答；
- **SES-REP-1/2**：顺序、From、Limit 截断、ReadStream 与折叠一致；`From` 取到 `CommitSeq` 最大值仍为空页；header 归属另一段或所有权记录无法解析时 Open 与 Header 报 `ErrCorrupt`；
- **SES-GC-1/2**：Delete 对持有中、未知的 Session 分别为 `ErrOwned`、`ErrNotFound`；删除后不可见、不可开、不可 fork、再次 Delete 为 `ErrNotFound`，同名 Session 立即可重建且得到新段；子仍读到已删除父的前缀；Collect 截掉最大 anchor 之后的自身 commit、整段删除不可达段、对存活 Session 无影响、幂等；
- **SES-WIR-4**：段 header 与 commit 不含 SessionID；删除后重建同名 Session 得到新的 SegmentID；
- **SES-WIR-5**：Ext 条目经 Store 往返后键与字节不变；空值、非法 JSON 或非法模块键为 `ErrInvalid`；
- **SES-FRK-1/2/3**：未知父、超出父 history 的 Seq、自身为父的 fork 被拒且不留根；相同 origin 重复 Create 幂等，不同 origin 为 `ErrConflict`；边指向贡献该 commit 的 Segment（在继承 commit 处 fork 的边直指持有它的祖先段）；空 fork 的 head 为 seed；`ReadCommits` 返回前缀加自身，`From`/`Limit` 跨越前缀边界计数；`ReadStream` 以 `LineageSession` 读取时返回前缀加自身且流内位置计入继承事件，以 `LineageSegment` 读取时只返回自身段的事件、父的同名流不受影响，未指定 lineage 为 `ErrInvalid`（SES-FRK-5）；首个自身 commit 的 Seq 为 `Seq+1`；继承的 CommitID 对 `Committed`/`LookupCommit` 可见、对 `Append` 为 `ErrConflict`；父在 fork 之后的追加对子不可见，反之亦然；fork 的 fork 读穿两层前缀；

kernel 的 wire 只有上述 header 字段、commit 字段与批次完整性规则，没有版本号（SES-VER-2）。

## 8. lineage 树与 fork

Session 的历史是 lineage 树上从根段到 tip 段的一条路径。节点是不可变的 commit 段（`Segment`），边是段到其父段某个 commit 的引用（`SegmentHeader.Parent`，类型 `LedgerRef`）；每个段至多一条父边（SES-LIN-1），因此每个根段下的节点构成一棵树，全部根段构成森林。Session 是指向自身 tip 段的根（`SessionRecord`）。fork 的单位是整条 ledger 的前缀 `Session @ CommitSeq N`：全部逻辑流到该 Commit 为止的事实。对话与 Turn 状态是 run 事实的投影（第 11 条），只复制其中部分流得不到完整的 canonical history，因此 fork 不复制任何 commit，而是新增一个节点和一条边。

```go
type SegmentID string                                       // = SegmentHeader.ID，kernel 随机抽取
type LedgerRef struct { Segment SegmentID; Seq CommitSeq }
type Segment struct { ID SegmentID; Header SegmentHeader }  // Header.Parent *LedgerRef 是边
type SessionRecord struct { ID SessionID; Tip SegmentID; CreatedAtUnixMilli int64 }
type Lease struct { Session SessionID; Epoch Epoch; Owner string; UntilUnixMilli int64 }
type ForkOrigin struct { Session SessionID; Seq CommitSeq }  // CreateRequest.Fork
type CreateRequest struct { SessionID; CreatedAtUnixMilli; Fork *ForkOrigin; CausationID; Ext }  // SegmentID 由 kernel 抽取，调用方不能命名节点
type Ancestry struct { Segments []AncestrySegment }          // 根段在前，tip 在后；每段带 From/Through
func LoadAncestry(ctx, LedgerStore, tip SegmentID) (*Ancestry, error)
func (*Ancestry) Read / Lookup / Contains / Owner(seq)
func LedgerSeed(SegmentHeader) Head        // 根段 0；子段 Parent.Seq+1
func Reachable(nodes map[SegmentID]Segment, roots []SessionRecord) map[SegmentID]CommitSeq
```

**SES-LIN-1（单父不变量）** 一个段至多一条父边：`SegmentHeader.Parent` 是单个可空引用，记录在段的创建记录中，创建后不可修改。一个 Session 的 `Ancestry` 是从根段到其 tip 段的唯一路径。建立边的操作只有 fork（SES-FRK-1），它只为新建的段设置父边：fork 新建子段并使其成为新根的 tip。已有段的父边不可修改，任何操作都不得为已有 Session 增加第二个父节点；多父 merge 被排除在模型之外，canonical import 若进入合同也只能新建根段或子段。因此 lineage 是森林，读路径只拼接一个父前缀（SES-FRK-2）、`Reachable` 沿唯一的 `Parent.Segment` 传递保留点（SES-GC-2）都依赖该不变量。Session 之间的其他关系不进入 lineage：spawn 子代理的派生来源记录在子段创建 `Metadata` 的 `twilight/spawn` 键下（SPN-2/3），以 fork 模式 spawn 的子 Session 只有 fork 点这一条父边，其 spawn 来源与 lineage 分开建模。

**SES-FRK-1（创建）** `Create` 携带 `Fork{Session, Seq}` 时建立 fork。`Ledger` 解析父 Session 的 `Ancestry`，找到贡献 commit `Seq` 的段（`Owner`），把边记为 `Parent = LedgerRef{Segment: 该段, Seq}`，然后以 `Backend.CreateSession` 一步落下新段与新根（SES-GC-4）。必须核对：父 Session 存活（否则 `ErrNotFound`）、`Seq` 在父的 history 内（否则 `ErrInvalid`）、父不是子自身；边只携带位置。任一不满足则不写根也不写段。相同 origin 的重复 Create 幂等并返回现有 tip 的 header、不同 origin 为 `ErrConflict`（SES-CRT-1）。父段只追加、已接受的 commit 不被改写（SES-APP-5），边一经建立永久有效；在继承 commit 处 fork，边直指持有该 commit 的祖先段，路径不会随 fork 层数增长。

**SES-FRK-2（读）** 段只存自身 commit，从 `LedgerSeed(header)` 起连续编号：首个自身 Commit 的 `Seq = Parent.Seq+1`。`Open`、`ReadCommits`、`ReadStream` 先加载根段的 `Ancestry`，再在这条显式路径上迭代：每段读取一次自己贡献的区间，不递归读 Store。`From`、`Limit`、`HasMore` 与 `StreamSeq` 都按拼接后的序列计数（`ReadStream` 按请求的 lineage 所见的序列，SES-FRK-5），`Head` 为 tip 段的 head。父在 fork 之后追加的 Commit 不属于子；子的 Commit 不属于父。

**SES-FRK-3（身份）** `Ancestry` 内的每个 CommitID 都是该 Session 的 CommitID：`Committed` 与 `LookupCommit` 对继承 commit 返回命中，`Append` 对它们返回 `ErrConflict`。重放判定只看 CommitID（EXT-WRT-2）：前缀 commit 经子重放为 `AlreadyApplied`。Session 级派生身份（RunID、Start/Retry/Settle 的 CommitID）在子中以子的 SessionID 派生，与父此后可能派生的同名身份不冲突。

**SES-FRK-4（所有权与恢复）** 所有权是根级的（`Lease{Session, Epoch}`），段不属于任何 Session：多个根可以经边共享同一历史段，但每个根有自己的 tip 段，两个根从不共用一个 tip，因此不同 Session 的写者从不向同一节点追加。`Append(lease, segment, commit)` 由 adapter 原子核对三件事：Lease 是该 Session 的当前 Lease、该 Session 的 `Tip == segment`、commit 的 `Seq` 等于该段的 head。子有独立的 Lease，打开子不需要父的所有权，父的写者也不受子影响。前缀中处于 Executing 的目标属于父的执行：子的接管处置以子的 AssignmentKey 询问 Executor，得到 `missing` 后按 RUN-CMT-7 处置（模型步撤回重规划、工具 call 记 Unknown），不接管父的 attempt。子引用的冻结正文与 artifact 由前缀段的 commit claim 保留，与段同寿（SES-GC-3）。

**SES-FRK-5（流的 lineage）** ledger 的 `Ancestry` 与流的语义继承分开定义：`Ancestry` 让子读到父的完整 Commit 前缀（完整性、provenance、CommitID 身份，SES-FRK-2/3），前缀中各个流对子的意义由拥有该流 domain 的模块声明（`StreamDefinition.Lineage`，EXT-STR-1），kernel 不为任何 domain 预设答案。`ReadStream` 按请求携带的 `Lineage` 读取：`LineageSession` 返回前缀加自身，流内位置计入继承事件；`LineageSegment` 只返回子自身段（`Seq > Parent.Seq`）的事件，流内位置从自身段起算；未指定为 `ErrInvalid`。`StreamHead` 与 lineage 无关，只计 tip 段自身。第一方模块的声明：`chatlog` 与 `turn/<TurnID>` 为 `LineageSession`，即会话与 Turn 的语义历史，子继承；`run/<RunID>` 为 `LineageSegment`，即写入它的那个段的执行历史，子不继承。投影按各自声明折叠继承 commit（EXT-PRJ-8）：默认只折叠 `LineageSession` domain 的批次，因此父在 fork 点仍处于 Executing 的 Run 在子的 `twilight/run` Machine 投影中不存在、子的接管处置不会把它当作自己的执行来恢复；以 run 事实为语义内容的投影（chatlog 的 assistant/tool_result、turn 的 attempt 结算）声明折叠全部批次。上层据此把继承的 attempt 视为已结算：其 Run 在子中 `ErrRunNotFound`，结算只在 surface 上。fork 点必须是语义静止点（父在该 commit 没有活动中的 Turn），由 Owner 核对（OWN-FRK-1）；kernel 的 `Create` 不核对。

## 9. 删除与回收

删除只是撤掉一个根；节点是否保留由可达性决定。

```go
func (Store) Delete(ctx, SessionID) error
func (Store) Collect(ctx) (CollectReport, error)
type CollectReport struct { Removed []SegmentID; Truncated map[SegmentID]CommitSeq; Dropped map[SegmentID][]CommitID }
```

**SES-GC-1（删除撤根）** `Delete(sid)` 删除 `SessionRecord`：此后 `Header`、`Open`、`ReadCommits`、`ReadStream`、以它为 origin 的 `Create` 都返回 `ErrNotFound`，第二次 `Delete` 为 `ErrNotFound`，被写者持有的 Session 为 `ErrOwned`。SessionID 立即可以重建，重建得到的是新根与新段，与旧段无关。段本身不动，也没有"已删除"状态：仍以它为前缀的 fork 继续经 `Ancestry` 读到它。

**SES-GC-2（可达性回收）** `Collect` 由 kernel 以 `Reachable(nodes, roots)` 计算每个段必须保留到的 CommitSeq：某个根的 tip 段保留全部自身 commit；只经边到达的段保留到到达它的最大 `Parent.Seq`，边沿 `Parent.Segment` 传递。未被任何根到达的段整段删除；被到达但无根的段截掉边之后的自身 commit。任何根 tip 的 commit 不被触碰，因此 `Collect` 可以在有写者打开时运行，且幂等。

**SES-GC-4（图变更的串行）** 改变根与节点集合的操作（`Create`、`Delete`、`Collect`）在 kernel 内互斥：`Create` 对父存活的核对与它的写入不会与回收该父或新节点的 `Collect` 交错；adapter 以 `CreateSession(Segment, SessionRecord)` 一步落下节点与根，不存在有根无段或有段无根的持久状态。`Append` 与读不取该锁：根 tip 的段从不被 `Collect` 触及。该互斥是进程内的：多个进程共享一个 Backend 时，根集合变更与可达性回收的互斥必须由该 Backend 的存储事务或 GC 权威提供，当前两个 adapter 都是单进程的。

**SES-GC-3（claim 与回收的分工）** 内容保留与 ledger 可达性同源：一个 commit 的 retention claim 以持有它的段为 owner（EXT-WRT-5），与该 commit 同生死。`Delete` 只撤根；`Collect` 计算可达性后删段或截段，并在 `CollectReport` 中报告整段删除的段（`Removed`）与截断时丢弃的 CommitID（`Dropped`），`writer.Collect` 据此释放对应 claim（EXT-WRT-9）。fork 不需要自己的 claim：子作为一个到达前缀段的根即保留了前缀（EXT-WRT-8）。

