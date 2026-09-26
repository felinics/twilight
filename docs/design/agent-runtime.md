# Twilight Agent Runtime

状态：v1 设计规范。本文定义 core 之上的运行时分层：`agentcore/owner`（Owner：组装与 Session 所有权；角色名见 README 术语规则）、`agentcore/driver`（Turn 驱动与恢复）、`agentcore/preset`、`agentcore/observe` 属于 Core；`agent/app`（组装与对话策略）、`agent/spawn`（子代理 Responder）、`agent/prompt` 与 `agent/context/compaction` 属于参考 agent，是建立在 Core 上的第一个具体 agent，不是 Core 协议（README 目录边界）。协议细节分别见 [Run](agent-run.md)、[Turn](agent-turn.md)、[Decision](agent-decision.md)、[Chatlog](agent-session-chatlog.md) 与 [Session](agent-session.md)。

运行时把 core 的三层——事实层（Store、Writer、Runtime、Coordinator）、决策层（AgentPreset 与 PromptBuilder 目录）、效果层（Executor 端口）——按角色端口组合成一个 Owner 进程；应用层在其上实现对话策略。两者都是部署中立的：Store 是内存、文件还是数据库，Executor 在本进程执行效果还是转发给远端 worker，驱动阻塞还是异步，都由端口的实现决定，运行时本身不含任何模型客户端、工具实现或执行环境。

## 1. 定位

**OWN-SCP-1** 运行时拥有的只有组合、所有权与策略：Owner 把端口接成 core 服务并发放 Session 所有权；driver 按 AgentPreset 组合 Loop 并驱动；app 拥有驱动的生命周期（何时驱动、驱动 goroutine 的归属、取消、结果组装）、输入路由、积压排空、compaction 时机、回复读取。协议语义（Turn、Run、事件、投影、恢复）全部在 core，运行时不新增任何事实类型。

**OWN-SCP-2** 下列都是部署选择，运行时只提供接口位置，不做决定：Store adapter；Executor 是进程内还是远端；驱动阻塞还是异步；何时对一个 Session 声明 `Takeover`；AgentPreset 注册表是进程内还是共享服务；观察者是进程内 sink 还是网络推送。

**OWN-SCP-3** 分离成立的判据：Owner 进程在没有任何模型客户端与工具实现的情况下能组装、注册 AgentPreset、Start Turn、把 Assignment 交给 Executor 并在 Outcome 到达时提交事实；effect 实现只存在于 Executor 一侧。

**OWN-SCP-4** Loop 运行在 Owner 进程里，executor 进程里没有 Loop。Loop 的决策内容——Machine 的 `Next`、PromptBuilder——是纯函数，属于决策层，身份进 AgentPreset 摘要；Loop 本身是把决策翻译成事实层提交与效果层 Assignment 的驱动器，读投影、经 Runtime 写事实、经 Executor 端口派发，因此不纯。换 PromptBuilder 不动 Loop，换驱动方式不动决策层。

角色对照：

| 角色 | 对应 |
|---|---|
| Session Store | `session.Store` 端口及其 adapter |
| Owner | `owner.Owner`：事实层 + 决策层 + driver，发放 `Handle` |
| Worker / Executor | `effect.Port`；实现是进程内 `LocalExecutor` 或远端客户端，效果实现只在这一侧，Worker 不接触 Store |
| Observer（只读方） | `extension.ProjectionReader` 按 SessionID 读取，不持 lease；`observe.Bus` 是 Owner 侧的实时流，两者对同一 head 一致（EXT-PRJ-4） |
| Workspace | 运行时只接收 opaque `TargetRef`，由 `TargetResolver` 按 effect 解析（RUN-LOP-9）；解析器实现、资源注册与 Workspace 管理在 core 之外（APP-TGT-1） |
| 存活判定 / 何时 Takeover | Session 租约（SES-OWN-1）：`Ports.Ownership.LeaseDuration` 给出租期，Writer 心跳续租（EXT-WRT-11），过期即可被下一个 Open 接管；对存活租约的强制 Takeover 仍由部署决定 |
| Agent Server（API、Auth、路由） | core 之外 |

## 2. Owner 与端口

```go
type Ports struct {
    Store          session.Store               // 必填；只有 durable 实现（JSONL filestore）
    Content        artifact.ContentStore       // 必填；冻结正文的 cas 存储（RUN-WIR-4），文件实现
    Artifacts      Artifacts                   // 可为零值
    Presets        preset.Registry             // nil → 内存注册表；只存决策身份
    Decisions      *decision.PromptBuilders    // 必填；Core 无默认目录，参考 agent 传 prompt.DefaultPromptBuilders()
    Executor       effect.Port                 // 必填：效果层端口（RUN-EXE-3）
    TargetResolver loop.TargetResolver         // application 的资源层按 effect 解析 opaque target（RUN-LOP-9）；nil 时每个 effect 无 target（APP-TGT-1）
    Observers      []writer.CommitObserver     // 提交观察（EXT-WRT-7）
    Modules        []extension.ModuleDescriptor
    Clock          func() time.Time
    Cache          extension.ProjectionCache   // nil → Store 能力或内存
    CacheEvery     session.CommitSeq
    Ownership      session.OpenOptions
    Fail           func(session.SessionID, error) // 调用之外的工作失败时
}
type Owner struct {
    Store; Writers; Registry; Admission; Runtime
    Turns   *turn.Coordinator     // turn.Commands + turn.Reader
    Driver  *driver.Driver
    Presets preset.Registry; Executor effect.Port; Frozen frozen.Store
    Projections extension.ProjectionReader // 经 Writer 读投影
    Content     chatlog.ContentResolver    // materializer（CHT-MAT-1）
    Chatlog     *chatlog.Commands
    History     turn.History
}
func New(Ports) (*Owner, error)
func (o *Owner) Open(ctx, sid) (*Handle, error)
func (o *Owner) CreateSession(ctx, sid, meta jsonstable.Value) error
```

**OWN-PRT-1** 端口按角色分组，每个字段是接口或 core 值类型；Owner 不知道拿到的是哪个实现，导出的字段是 core 服务与端口。缺省实现只在 nil 时选用，且都是进程内的。

**OWN-PRT-2** Executor 是唯一必填端口：没有效果层的 Owner 无法完成任何 Turn，而效果层的实现从不属于运行时。OWN-SCP-3 的判据以一个只记录 Assignment 并按脚本回送 Outcome 的 Executor 验收。

**OWN-PRT-3** 内容寻址只有一个端口：`Content` 是 artifact `cas` ContentStore，冻结正文是它在 `runmod.FrozenAuthority` 下的内容。Runtime 写入；authority 以 `runmod.NewContent` 建立 materializer（`Owner.Content`），供 prompt 构造、回复与 compaction transcript 读取（CHT-MAT-1）。Executor 不读它：Dispatch 携带内联请求（RUN-EXE-7）。重启或接管的进程必须拿到同一个 store，ledger 只有 digest。

## 3. 所有权与命令边界

```go
type Handle struct{ Recovered int }
func (h *Handle) ID() session.SessionID
func (h *Handle) Writer() writer.Writer
func (h *Handle) Close(ctx) error
```

**OWN-PRT-3（全部 port 为 durable）** Session Store、frozen 正文的 Content Store、BindingStore、RetentionLedger 与（组装 Worker 时的）Execution Ledger store 均为必填且均为 durable；dispatch ledger（`Ports.Processes`）durable，在 `Ports.MissingEffects` 为 `RedispatchMissing` 时必填（缺失时 `owner.New` 返回错误，RUN-EXE-15），为 `DisposeMissing` 时不使用：core 不提供任何随进程消失的 store 实现，`owner.New` 与 `app.Build` 对 nil port 返回错误而不回退。一方实现为：Session ledger 与 cas 正文在文件系统（`agentcore/session/filestore`，JSONL 段与 cas 文件），Binding、retention claim 与 execution record 在同一个 SQLite 文件（`agent/store/sqlite`）；另一方实现为全部合同在一个 Postgres 数据库（`agent/store/postgres`，CLD-STO-1）。这样崩溃重启后事实、正文、索引、claim 与 record 同时存在，Executing 目标的接管处置只依据 record（RUN-CMT-7）。投影缓存是可丢弃的派生数据，允许内存实现；preset 注册表在 Build 时重建，同样允许内存实现。reference agent 的测试使用 `t.TempDir()` 下的同一组实现（`filestoretest`、`sqlitetest`）；`agentcore` 的测试使用各合同 `xxxtest` 包中的参考实现（CLD-STO-0）。

**OWN-HDL-1** `Owner.Open(sid)` 发放对一个 Session 的执行能力，不是读取能力。Open 取得该 Session 的 Writer（本进程 epoch 下）、运行接管处置（DRV-3）并安装恢复监听；返回的 `Handle` 只承载这份能力与其生命周期：`ID`、`Writer`、`Close`。所有权按代（generation）记录，一代的状态为 opening / open / closing：同一 Owner 内一个 Session 同时只有一代，处于任一状态时 Open 都返回 `ErrSessionOpen`，因此一代的释放（停止恢复监听、关闭 Writer）完成之前新的一代不会取得 Writer；`Handle.Close` 只释放自己那一代——先在锁内把该代置为 closing，释放资源后再从表中删除——已释放或正在释放的 Handle 再 Close 为无操作，不会关闭替代它的一代；Close、DeleteSession、Owner.Close 与失败的 Open 都经同一条释放路径，接管处置失败时 Open 释放已取得的 Writer，失败的 Open 不留下所有权。SessionID 是持久身份；Handle 表示"本进程当前拥有它"。Handle 不带任何业务操作：Send、排空、fork、compaction、spawn 分别属于 app、domain 命令或效果层。

**OWN-HDL-2** 写侧要求 Handle，读侧只要 SessionID。core 的每个命令——`turn.Commands`（Start/Deliver/Retry/Stop/Settle）、`chatlog.Commands`（Submit/Withdraw/Compact）、`driver.Drive`、`SessionRunStore.Bind(w)` 与 `RecoverInterrupted`——以 `Handle.Writer()` 为参数；Loop 拿到的是绑定了该 Writer 的 `runtime.RunStore`（`loop.Run/Advance/Deliver(ctx, store, …)`），任何一层都不按 SessionID 重新取 Writer；命令校验请求所指 Session 与 Writer 的 Session 一致。命令路径上的读取——决定"要不要写"的读也属于命令路径：`driver.Drive` 判断 Turn 是否 active、恢复交付查找 Run 所属 Turn、Loop 经绑定的 `RunStore.Load(ctx, runID)` 读机器状态，都经 `w.Projections()` 读 Writer 的事务投影——是 owner 自己 epoch 下的视图，因此失去所有权的 owner 仍按自己的旧视图规划，在下一次提交被围栏（RUN-LOP-5），而不会读到新 owner 的状态后静默结束。三条路径因此是：公共查询经 `ProjectionReader(Store)`；命令规划经 `Writer.Projections()`；命令提交经同一 Writer。公共读取——`turn.ReadSurface`、`chatlog.ReadSurface`、`chatlog.ReadContext`、`SessionRunStore.Record`、`Coordinator.Status`、`Owner.Reply`、`Owner.Projection`——按 SessionID 经 `extension.NewProjectionReader(store, registry, cache)` 从 Store 折叠（EXT-PRJ-3/4），不经 `Writers`，不取得也不延续任何所有权；所有权只在 Open 时随 Writer 的打开转移。

**OWN-HDL-3** Writer 是同步点。跨域不变量由同一个 Session Writer 的原子 Commit 保证，不由 service 之间协调，协调者只有一个：`unit.Commit`。`Start` 是 Turn、attempt、chatlog、Run 四个 Part 的一个 unit（`turn/started`、`attempt/started`、`chatlog/input_delivered` 与 Run 创建事实）；`Deliver` 是 Run 的 `Command` Part（`AcceptInput`）与 chatlog 的 `DeliverInputs` Part 的一个 unit（TRN-DLV-2）。不存在"chatlog 成功、turn 失败、run 未创建"的中间状态。所有命令经同一 Writer 落盘，因此共享同一 epoch 与同一投影视图，过期 owner 由 Writer 围栏（SES-OWN）。

**OWN-HDL-4** driver 不拥有事实。它读已提交状态、决定执行、经 Loop/Runtime 提交、再读已提交状态；"提交持久状态"与"继续执行"之间不构成事务，崩溃边界允许落在两者之间：Start 已提交而未驱动时，持久事实已是 `TurnActive + ActiveRun`，新 owner Open 后 Drive 即继续同一 Run。需要原子的是 chatlog/turn/run 之间的事实转换；不需要原子的是提交之后的外部效果与驱动推进。

## 4. Session 生命周期

**OWN-FRK-1** `Fork` 先以 `turn.History.ActiveAt(parent, At)` 核对 fork 点是语义静止点：父在该 commit 有活动中的 Turn 时拒绝（`ErrInvalid`），不建子——子会继承一个执行属于父的 Turn（SES-FRK-5）。通过后以 `writer.Fork` 建立子 Session（SES-FRK-1、EXT-WRT-8），不打开它；调用方随后以 `OpenSession` 打开。子的执行状态投影不含父的 Run（EXT-PRJ-8），因此接管处置没有前缀遗留的 Executing 目标；继承的 Turn 在子的 surface 上已结算，其 Run 对子为 `ErrRunNotFound`。父不受影响，可以继续被驱动。

**OWN-FRK-2** `ForkBeforeTurn(parent, turnID, child)` 以 `turn.History.StartCommit` 找到携带该 Turn `twilight/turn/started` 的 Commit `k`，在 `k-1` 处 fork：子的对话止于该 Turn 的输入仍为 `submitted` 的状态。`Drain` 或 `Route` 把这些输入投递给新 Turn 即重新生成；`chatlog.Commands.Withdraw`（经子的 Handle）写 `input_withdrawn`（CHT-EVT-2，要求输入为 `submitted`）后再 `Send` 即编辑。`k = 0` 时没有可 fork 的前缀，返回 `ErrInvalid`；未知 Turn 返回 conflict。edit / retry / regenerate 三种动作因此都归到同一个 fork 原语加输入投递上（TRN 第 1 节）。fork 只复制 Session 的已提交事实：子 Session 不恢复父在 `k-1` 时刻的 workspace 状态，父 Session 在 `k` 之后的工具调用对 workspace 的效果不会被撤销（TRN-DUR-3、agent-workspace.md）。

**OWN-FRK-3** `DeleteSession` 先停止该 Session 的恢复监听并关闭其 Writer，再以 `writer.Delete` 撤根（EXT-WRT-9）；被另一进程持有的 Session 为 `ErrOwned`。以它为前缀的 fork 不受影响，前缀的 claim 随段保留。`Collect` 调 `writer.Collect` 回收无根可达的段并释放被回收 commit 的 claim（SES-GC-2/3）。

## 5. AgentPreset 注册（preset）

```go
// agentcore/preset
type Registry interface {
    Register(turn.PresetID, turn.AgentPreset) (turn.PresetRef, error)
    Resolve(turn.PresetRef) (turn.AgentPreset, error)
}
func NewMemory() *Memory
// agent/app
func NewPreset(model run.ModelRef, tools []loop.ExecutableTool, opts ...PresetOption) (turn.AgentPreset, error)
```

**PST-1** 注册表只存决策身份。`Register` 按 TRN-PST-2 校验、按 TRN-PST-1 计算摘要并返回 `PresetRef`；`Resolve` 在摘要匹配时返回 AgentPreset。它不持有模型客户端或工具实现：AgentPreset 里的工具只是 `PublicTool{Ref, Definition, Policy}`，实现由 Executor 一侧的目录提供。`app.NewPreset` 是本地便捷构造：从工具实现取冻结定义与响应策略进 AgentPreset，实现本身不进。

**PST-2** 注册表按完整 `PresetRef{ID, Digest}` 保存 immutable 版本。同一 ID 注册新摘要时保留旧版本，已有 Turn 继续解析其记录的版本。注册与 Resolve 均隔离可变字段；SystemPrompt 参与摘要。缺少指定版本时返回 `preset.ErrUnavailable`，Turn 保持原状态，等待宿主提供该版本（TRN-REC-2）。

## 6. 驱动（driver）

**DRV-1** `driver.Drive(ctx, w, turnID)`：读 `twilight/turn/surface`，Turn 为 `active` 时解析其 AgentPreset、取该 AgentPreset 的 Loop、驱动 `ActiveRun` 到下一个静止点（阻塞式 `Loop.Run`，即 Advance/Deliver 之上的封装，RUN-LOP），随后（或 Turn 非 active 时直接）调用 `Coordinator.Status` 组装响应（TRN-STA-1）。Drive 返回 `driver.DriveResult{TurnResponse, AlreadyDriving}`：Loop 报告同一 Run 已有本地驱动者时，Drive 转为成功响应并置 `AlreadyDriving`，`TurnResponse` 是读到的 Turn 状态；提交的输入由运行中的驱动者继续推进，调用方不经错误通道分辨这一情形。`AlreadyDriving` 是本进程的事实，不进入 Turn 的 `ResumeDisposition` 词汇表。驱动受调用方 ctx 约束：取消是调用方的决定，被取消的驱动使 Turn 保持 `active`，下次 Open 后再驱动即恢复。

**DRV-2** Loop 按 PresetRef 组合并缓存在 Driver 内：`Decisions.Resolve(preset)` 得到 prompt builder，与 preset 上的 Scheduling、MalformedRetries 及共享的 Executor 一起构成 `loop.New(executor, builder, loop.Settings{Scheduling, MalformedRetries, BeforePrepare})`；`Driver.Planner` 非空时 `BeforePrepare` 把 Loop 绑定的 Writer 交给它（RUN-LOP-10，APP-CKP-1）。每次驱动以 `Driver.Sink` 为 Loop 的 EventSink，进度帧由此到达 Bus（OBS-1）。一个 Run 属于一个 Turn、一个 Turn 只有一个 AgentPreset，因此同一 Run 的全部驱动落在同一个 Loop 上，Loop 的 already-driving 守卫成立（RUN-CMT-6）。

**DRV-4（Responder）** `Driver.Responders` 按 ToolRef 登记系统应答器：`Responder.Respond(ctx, w, WaitingCall{Request, ToolRef, Arguments})` 返回结算该 call 的 payload，或以错误拒绝。Driver 在两处询问：一次 Drive 以 `LoopWaiting` 结束时，对该 Run 每个 `Waiting(ExternalResponse)` 且有 Responder 的 call；Open 完成接管处置后，对该 Session 每个活动 Run 的这类 call。应答在 Session 的 recovery lifetime 下运行、经同一 Writer 以派生的 response CommandID 提交 `SubmitToolResponse` 或 `RejectToolCall`（重放为 already-applied），随后 Drive 所属 Turn；同一 ResponseID 同时只有一个应答在途，Owner 关闭时在途应答取消而 Wait 保留。没有 Responder 的 ExternalResponse call 留给应用层按 TRN-STA-2 应答。

**DRV-3** `Owner.Open(sid)` 经 `Writers` 取得 Writer，随后 `driver.Open(ctx, w)` 以该 Writer 安装恢复监听并调用 `SessionRunStore.RecoverInterrupted(ctx, w, reconciler)`（RUN-CMT-7），其中 `reconciler = &reconcile.Reconciler{Executions: executor, Watcher: watcher, Lifetime: lifetime, Deliver: deliver, Fail: fail}`；恢复监听持有 w，重连的 Outcome 经它结算。kept 目标向 Driver 的 `effect.Watcher` 登记（RUN-EXE-17）：Watcher 对 Executor 保持一条结算通知订阅并周期性重读，`ErrOutcomeNotReady` 表示执行仍在进行、继续等待；`ErrExecutionNotFound` 与 `effect.ErrOutcomeUnavailable`（`executor.ErrUnknownProvider` 与 `effect.ErrOutcomeCollected` 包装它，HTTP 以 410 承载）是确定答案，登记撤销并经 `Fail` 上报；其他读取失败不改变任何状态，下一次通知或周期性重读再试。目标保持 Executing，下一次 `RecoverInterrupted` 重新规划，记录已不存在则处置。Attach 握手受 Open 请求的 context 约束；Watcher 登记与交付使用该 Session 的 recovery lifetime，Driver 的 Loop 与 Reconciler 共用同一个 Watcher。Open 返回后请求取消仍允许恢复继续；再次 Open 会替换旧监听，`Handle.Close` 与 `Owner.Close` 取消各自拥有的监听。 `missing` 目标的处置策略来自 `Ports.MissingEffects`（`reconcile.MissingPolicy`，透传为 `Reconciler.Missing`）：`RedispatchMissing` 时 reconciler 带 dispatch ledger（`Ports.Processes`）与 `Redispatch` 端口（`loop.Redispatch` 在该 Writer 上重建 Assignment），对 `missing` 的目标在预算内重派而不处置（RUN-EXE-15）；`DisposeMissing`（零值）时只处置。

Attach 的 `active` / `terminal` 为 `keep`：保留 Executing 并等待实际 Outcome；`orphaned` 表示 record 存在但没有未过期的租约（持有者已死或从未持有；持有者活着时无论是哪个 Worker 都为 `active`），为 `defer`：保留 Executing 并同样等待 Outcome，executor 实现 `effect.Recoverer` 时 Reconciler 随即请求一次 `RecoverExecution`（RUN-EXE-6）；`missing` 为 `dispose`，才进入接管处置。Executor 重启后旧记录仍在其 Execution Store 中，按 record 返回状态。`deliver` 按 Outcome 的 RunID 查找 Turn，使用其 preset 的 Loop 结算并继续驱动；后台失败经 `Ports.Fail` 上报。

| Executor observation (`AttachmentState`) | Recovery disposition | Owner 行为 | API 观察 |
|---|---|---|---|
| `missing` | `missing` | 允许协议自动处置 | `recovery_required`，直到处置完成 |
| `active` | `active` | 保留 Executing，等待 Outcome | `observed` |
| `terminal` | `terminal` | 读取并结算 Outcome | `observed` |
| `orphaned` | `deferred` | 保留 Executing，请求一次 `RecoverExecution` 后等待 Outcome | `recovery_required` |

`RunStatus=active`、`TurnStatus=active` 与 `AttachmentState=active` 分属三个状态域；API 不直接暴露它们的内部枚举，而通过明确的 view 映射输出。

## 7. 应用层（app）

```go
// agent/app
func Build(Config) (*Application, error)
func (app *Application) OpenSession(ctx, sid, SessionOptions{Preset, NewTurnID, ResumeActive, Compact*}) (*Session, error)
func (app *Application) Fork(ctx, ForkRequest{Parent, At, Child}) (session.SegmentHeader, error)      // OWN-FRK-1
func (app *Application) ForkBeforeTurn(ctx, parent, turnID, child) (session.SegmentHeader, error)      // OWN-FRK-2
func (app *Application) DeleteSession(ctx, sid) error                                                 // OWN-FRK-3
func (app *Application) Collect(ctx) (session.CollectReport, error)
func (app *Application) Events(ctx, sid) <-chan Event                                                 // OBS-1
func (app *Application) EventsFrom(ctx, sid, from session.CommitSeq) (<-chan Event, error)          // OBS-2
type Result struct { TurnID; Status; Disposition; Reply string }
func (s *Session) Send(ctx, text string) ([]Result, error)                  // 提交 + 路由 + 同步驱动 + 结算后排空
func (s *Session) Submit(ctx, text string) (turn.TurnRef, error)            // 提交 + 路由，后台驱动，立即返回
func (s *Session) Route(ctx, inputs []run.AgentInput) (turn.TurnResponse, error)
func (s *Session) Drain(ctx) (turn.TurnResponse, bool, error)
func (s *Session) Resume(ctx) ([]Result, bool, error)
func (s *Session) Retry(ctx) ([]Result, bool, error)
func (s *Session) Status(ctx) (SessionStatus, error)
func (s *Session) Compact(ctx) (chatlog.CompactionID, bool, error)
func (s *Session) Handle() *owner.Handle
func (s *Session) Close(ctx) error
```

**APP-SES-1** `OpenSession` 依次：解析 Preset（PST-2）、确保 Session 存在（`Create` 对已存在的 Session 幂等，不比较 `CreatedAtUnixMilli`，SES-CRT-1）、`Owner.Open`（取得 Handle：Writer 与接管处置，OWN-HDL-1、DRV-3）；处置数暴露为 `Session.Recovered`。`ResumeActive` 为真时同步 Resume 仍在 `active` 的 Turn。此后 Session 的每个命令都经 `Handle.Writer()` 提交。

**APP-SES-2** `Send` 提交文本（`chatlog.Commands.Submit`）、Route 并阻塞到结算：首个 `Result` 是输入落入的 Turn，其后是本次调用在结算后从积压开启并结算的 Turn（Drain 的循环内化在 Session 里）。`Disposition` 为 `already_driving` 时该输入由运行中的驱动者推进，本次调用不再排空。`Reply` 为该 Turn 最后一条 assistant 的文本（`chatlog.LastAssistantText`），仅在 `finished` 时读取——回复是对话层概念，turn 层只报协议结果。

**APP-SES-3** 并发 `Send` 安全：写入由该 Session 的 Writer 串行化。路由竞态（两个 Send 同时判定 Start，或投递瞬间结算）表现为 `turn.ErrConflict`，Session 重试路由；重试次数由 `SessionOptions.RouteRetries` 限定（默认 `DefaultRouteRetries`=4），耗尽后返回 `app.ErrRouteContended`（瞬时答案，与"Turn 等待 Retry / Settle"的 `turn.ErrConflict` 区分），输入保持 submitted；重试前发现输入已被其他驱动者投递时，返回 `AlreadyDriving` 的 `Result`。结算后的排空由 `SessionOptions.DrainBudget`（默认 `DefaultDrainBudget`=64）限定，耗尽时返回已得到的 Results 与 `ErrDrainBudget`，剩余积压留给下一次调用。

**APP-SES-4** `Submit` 提交文本并提交其路由（Deliver 或 Start，同 APP-RTE-1 的提交半段），返回输入落入的 `TurnRef` 后立即返回；驱动、结算后排空与自动 compaction 在 Session 拥有的后台 goroutine 里进行，其 ctx 由 `Session` 持有、`Close` 取消并等待。进展与回复经 Events 观察；驱动失败经 `Config.Warn` 与事件流上的一条 `Event{Err}` 报告，不进 ledger。输入被运行中的驱动者接走（already_driving）时 Submit 直接返回该 Turn，不起驱动。`Send` 与 `Submit` 共用路由与结算逻辑，差别只在驱动是同步还是后台。`Wait` 阻塞到已启动的后台驱动全部结束而不取消它们。

**APP-RTE-1** 路由是 app 的策略：`app.Session.Route(ctx, inputs)` 先读 turn surface——存在 `active` 的 Turn 时 `Deliver`（输入进入该 Run 的下一步）；否则以新 TurnID 与 Session 的 AgentPreset `Start`；`attempt_failed` 的 Turn 使 Route 返回 conflict，不自动 Retry 或 Settle，那是调用方的决定。提交成功后进入 `driver.Drive`。输入在两种情形下都已先写入 `input_submitted`。

**APP-RTE-2** 排空是 app 的策略：`app.Session.Drain(ctx)` 读 chatlog surface，若存在 `submitted` 且未 delivered 的输入，按 CommitSeq 顺序取全部，经 Route 开新 Turn；否则返回 false。已提交而未投递的输入就是 inbox 的 next-turn 列表，不需要另一份持久结构。

**APP-INP-1** `chatlog.Commands.Submit(ctx, w, id, content)` 提交用户输入，`content` 对 core 是 opaque 的 canonical JSON，构造器为 `agent/input.Text`（DEC-INP-1）；`app.Session.Submit` 以 `chatlog.NewInputID` 铸造随机、跨重启无碰撞的 InputID，`app.Session.SubmitInput(ctx, id, text)` 接受调用方的 InputID 作为幂等键（inbox 的 submit 命令经此路径重放）。`StartRequest.Inputs[i].ID` 等于已 submitted 的 InputID，`Payload` 等于其 Content。

### 7.0 命令 inbox

**APP-INB-1（命令先落盘）** `agentcore/inbox.Store` 是 Session 的 durable 命令 inbox：`Enqueue(sid, Command{ID, Kind, Payload})` 返回即成为事实，按 Session 分配递增 Seq；同一 CommandID 重复 Enqueue 返回已存条目，不写入；同 ID 不同 Kind 或 Payload 为 `ErrCommandConflict`。`Pending(sid)` 按 Seq 返回未处理条目，`Resolve(sid, seq, Result)` 只对 pending 条目生效一次（否则 `ErrNotPending`），`Sessions(limit)` 按 SessionID 顺序返回有 pending 条目的 Session，最多 limit 个（0 为全部，CLD-CMD-4）；一页是"需要 owner 的 Session"的样本而非游标，未返回的在前面的被排空后出现。不持有 Session 的调用方（gateway、其他进程、尚未 Open 的本进程）经 `app.Application.Enqueue` 写入，`AwaitCommand` 等待 Result。没有配置 `Config.Inbox` 时这些入口返回 `ErrNoInbox`，不退化为无操作。CommandID 由调用方给出，app 不代生成。

**APP-INB-2（owner 应用与幂等）** 只有持有 Handle 的进程应用命令：`app.Session.ApplyPending` 按 Seq 顺序经该 Session 的 Writer 提交，提交成功后才 `Resolve`。命令集合与 payload 由 app 定义：`submit`（`SubmitCommand{InputID, Text}`，经 `SubmitInput` 提交并路由，后台驱动）、`stop`（`StopCommand{TurnID?, Reason}`，无 active Turn 或 TurnID 与 active Turn 不一致时 rejected）、`withdraw`（`WithdrawCommand{InputID, Reason}`）、`retry`（`RetryCommand{Reason}`，提交 Retry 后后台驱动）、`bind_workspace`（`BindWorkspaceCommand{WorkspaceID}`，APP-WSP-2）、`unbind_workspace`（`UnbindWorkspaceCommand{Reason}`）。应用结果两类：ledger 提交成功或已提交（`CommitAlreadyApplied`）为 applied；已提交状态不容许的命令（`turn.ErrConflict`、`chatlog.ErrNotSubmitted`、payload 不合法、未知 Kind）为 rejected，写入 Reason 后关闭。存储或 Writer 的瞬时失败、路由争用（`app.ErrRouteContended`，APP-SES-3）使命令保持 pending 并终止本轮，以保持顺序，下一轮重试。`stop` 建议携带 `TurnID`：不带时停止应用时刻的 active Turn。应用幂等：owner 在提交后、Resolve 前崩溃时，下一个 owner 重放命令，ledger 以 CommitID 判定已应用，再 Resolve。

**APP-INB-3（触发与生命周期）** `OpenSession` 在接管处置（RUN-CMT-7）之后、`ResumeActive` 之前先应用一遍 pending 命令，随后由该 Session 的 applier goroutine 在两种触发下继续应用：同进程 `Enqueue` 的唤醒（非阻塞信号），以及 `SessionOptions.InboxPoll` 的轮询（零值为 `DefaultInboxPoll`，即命令唤醒未到达时的最坏延迟）。applier 与后台驱动分开计数：`Wait` 只等驱动，`Close` 取消后等待 applier 退出。被替代的 owner 应用命令时 Append 被 Epoch 围栏拒绝（SES-OWN-2），视为瞬时失败，不 Resolve。

**APP-ACT-1（按活动工作获取所有权）** `Config.Activation` 配置后，Session 的所有权只为活动工作持有。命令经 `Enqueue` 到达本进程而本进程未持有该 Session 时，`Application.Activate` 以 `Activation.Preset` 与 `Activation.Options`（`ResumeActive` 置为 true）调用 `OpenSession`。Open 内部的 Acquire 是副本间唯一的裁决（SES-OWN-1）：另一副本持有存活租约为 `ErrOwned`，命令已 durable，由持有者在轮询中应用；租约已过期则本进程接管，接管处置（RUN-CMT-7）与 pending 命令的应用（APP-INB-3）在 Open 中完成。同一 Session 的并发激活在进程内合并为一次 Open。`Activation` 要求配置 `Config.Inbox`。

**APP-ACT-2（静止判定与释放）** `Activation.IdleRelease` 大于零时，每个已打开的 Session 有一个静止检查循环（周期为 IdleRelease/4，下限 10ms）。静止条件：无后台驱动与快照、自最近活动起已过 IdleRelease、turn 投影无 active Turn、inbox 无 pending 条目。活动时刻在 Open、后台驱动开始与结束、应用 pending 命令时更新。条件满足时调用 `Session.Close` 释放所有权（Release，不等待租约过期），随后重读 inbox：若期间有命令落入（其唤醒可能发给了已停止的 applier），立即重新激活。等待工具响应而无后台驱动的 Turn 是 active Turn，不释放。IdleRelease 为零时激活的 Session 保持打开直到 Close。

**APP-ACT-3（激活来源）** 三个来源：(1) `Enqueue` 写入 pending 条目且本进程未持有该 Session；(2) `Activation.Scan` 大于零时的周期扫描，候选为 `inbox.Sessions(limit)`（CLD-CMD-4）与 `ExpiredLeases(now, limit)`（SES-OWN-5；死亡 owner 遗留的 Session，接管后若无活动工作即由 APP-ACT-2 释放），limit 为 `Activation.ScanLimit`（零值为 `DefaultScanLimit`），每次扫描的读取成本与 Session 总数无关，未读到的候选留给下一次扫描或其他副本；(3) owner 命令面的 `open` 不带 preset 时。扫描与后台激活对 `ErrOwned`、`ErrSessionOpen` 与取消不告警。多副本部署要求 `Ownership.LeaseDuration` 大于零，否则崩溃的 owner 持有的租约不过期（`ownerservice` 在配置层检查）。

**APP-ACT-4（与现有接口的关系）** `OpenSession` 与 `Session.Close` 的语义不变；HTTP `open` 显式指定 preset 时仍走原路径，这样打开的 Session 同样受 APP-ACT-2 释放。`Takeover` 仍是唯一的强制接管开关，激活不使用它。`Application.Close` 先停止扫描，再关闭持有的 Session，再等待进行中的释放与后台激活。

**APP-ACT-5（重开成本）** 激活模型使"释放 → 另一副本重开"成为正常的 Turn 路径，重开成本因此是每 Turn 成本。Open 的读取由两部分构成：段的 `CommitIndex`（SES-REP-5）与各投影的起点。前者 adapter 应以索引列回答而不解码 commit 正文（Postgres 的 `session_commits.commit_id/streams`）；后者经 durable 的 `extension.ProjectionCache`（EXT-PRJ-3）从保存状态起折叠，落后上限为 `CacheEvery`，因此 Open 的折叠量与历史长度无关。写侧对应的上界：Close 不再无条件刷新（EXT-PRJ-7），每 Turn 的缓存写量最多为每 `CacheEvery` 个 commit 一次整状态，而不是每次释放一次。共享存储实现须提供 `ProjectionCacheProvider`（filestore、Postgres 均已提供）。kernel 句柄不在 Open 时装载 tip 段的 CommitID：`Committed`、`LookupCommit` 经 `Backend.Locate` 点查询，`StreamHead` 首问时读取（SES-REP-3），Open 的代价因此与 tip 段长度无关。

### 7.1 compaction

**APP-CKP-1** 机制与命令在 chatlog，策略在 `agent/context/compaction`。`chatlog.Commands.Compact(ctx, w, summaryText, retain, guard)` 在该 Session Writer 的 Commit 临界区内读 `twilight/chatlog/context`，以 `View.Head().Next - 1` 为 `CoveredThrough`，把 summary 与 `compaction_created` 同组提交；准入规则以 `guard` 注入同一临界区，turn 域提供两个守卫（chatlog 不能反向依赖 turn）：`turn.RequireNoActiveTurn` 只允许 Turn 之间；`turn.RequireQuiescentRun` 另允许 Turn 内的步间点，即 active Turn 的 Run 处于 `Open`，或处于 ToolStep 且没有 Executing 的 call；模型步 Prepared 或 Executing、工具 call Executing 时为 conflict。app 的 Compact 与自动策略都用后者。Turn 内的触发点在驱动循环里：Loop 在 `Open` 状态、Prepare 之前调用 `Settings.BeforePrepare`（RUN-LOP-10），driver 把它接到 `Driver.Planner`（DRV-2），`app.Application` 实现 Planner，按该 Session 的 `CompactAfterEntries` 决定是否压缩；压缩写入的 compaction 就是随后 PromptBuilder 读到的上下文。Turn 之间的触发点不变：结算且积压排空后。CompactionID 与 SummaryID 由 SessionID、base context digest 与摘要文本派生（同一后缀，前缀分别为 `ckpt-` 与 `sum-`），CommitID 为 `compaction/<CompactionID>`：对同一 base 重试的 Compaction 重放同一 CommitID，Writer 按 EXT-WRT-2 回答 `AlreadyApplied`，不会写入第二个 compaction。`app.Session.Compact` 经 `compaction.Summarizer` 用 AgentPreset 的模型生成摘要，**这次模型调用与其他效果一样经 Executor 端口**：请求经 `FreezeModelRequest` 冻结并 `Put` 进 Frozen，以一个不属于任何 Run 的临时 key 构成模型 Assignment 交给 `Executor.Dispatch`，等待 Outcome；Owner 因此不需要模型客户端，远端 Executor 以同一方式服务它。生成中崩溃不写任何事件。自动策略由 `SessionOptions.CompactAfterEntries` 启用：结算且积压排空后、Context 条目数超阈值时触发；失败经 `CompactWarn` 上报，不改变已结算的 `Result`。

**APP-CKP-2** retained 集必须封闭：保留的 tool_result 连同签发该 call 的 assistant，保留的带 tool_call 的 assistant 连同其在 Context 中的 result；call 的 result 尚未进入 Context 的 assistant 必须保留（其 result 会在 compaction 之后落下，配对不能断）。`compaction.RetainLast(entries, n)` 返回满足封闭的最短后缀，未结算 call 的 assistant 会把后缀向前拉；`chatlog.CheckRetainClosure` 在命令内校验封闭并拒绝违反者。子集与顺序由 fold 校验（CHT-EVT-3）。

### 7.2 模块与缓存

**APP-MEM-1（app module 开口）** `Ports.Modules` 把 application module（EXT 第 8 节）追加进 Registry。app module 的读写走既有入口：写事件经该 Session Handle 的 `Writer().Commit`（与 `chatlog.Commands.Submit` 同一 Writer）；读自己的投影经 `Owner.Projection(ctx, sid, id, version)`。

**APP-MEM-2（投影缓存归属）** 缓存解析一次并同时交给两处，各自只写自己有权写的投影（EXT-PRJ-6）：Store 实现 `extension.ProjectionCacheProvider` 时取它，否则进程内缓存。`Writers` 用 `runmod.WriterCachePolicy(CacheEvery)` 刷新全部投影、唯独不碰 machine projection；machine projection 由 Runtime 经 `SnapshotPolicy` 写入（RUN-CMT-2）。`CacheEvery` 是部署可调的区间。

### 7.3 资源 target

**APP-TGT-1（target 解析归 application）** core 对资源 target 只提供 RUN-LOP-9 的 seam：`loop.EffectContext`、`run.TargetRef` 与 `loop.TargetResolver` 接口。core 没有 target 事实，不持久化 Session 到资源的映射，也不提供默认解析器：`Ports.TargetResolver` 为 nil 时每个 effect 无 target。资源注册、Workspace 生命周期管理与 `TargetResolver` 实现属于 application；reference agent 的实现是 `agent/workspace`（APP-WSP-1..6，agent-workspace.md）：Session → Workspace 的绑定是 `agent` 源应用模块的 Session 事实，解析器只对 `PlacementWorkspace` 的 tool effect 解析。对话 lineage 与资源 lineage 是两套 lineage：Session fork（OWN-FRK-2）复制已提交事实，子 Session 把读到的父绑定视为继承（Share 策略的实现），其他策略（分配新 workspace、不给 workspace、从 fork 点的 snapshot 恢复、从最新 snapshot 克隆）由 `SessionOptions.InheritedWorkspace` 在子 Session 首次 Open 时写显式事实覆盖（APP-WSP-5、APP-WSP-7）。Workspace 自身的 fork 不复用父的 mutable RuntimeBinding。target 的含义由 Workspace domain 定义。

## 8. 子代理（spawn）

**SPN-1** 子代理是一个由 ToolCall 触发的普通 Session。模型调用 spawn 工具（默认 `agent_spawn`，经 `Config.Spawn` 配置 Tool、命名 Preset 解析与最大深度）；该工具的 `ResponsePolicy` 为 `ExternalResponse`（RUN-MCH），call 进入 Waiting，不经 Executor，也没有 Execution Record。应答者是 Owner 侧的 `spawn.Responder`，经 `Driver.Responders` 按 ToolRef 注册（DRV-4）：它创建或延续子 Session、驱动到结算并把子的回复作为 `SubmitToolResponse` 的 payload 提交；参数、命名 Preset 或深度错误在任何子 Session 建立之前返回，call 以 `RejectToolCall` 记 `ToolCallFailed(Known/response_rejected)`。子 Session 经 `Owner.Open(child)` 取得 Handle，经 `turn.Commands.Start`、`driver.Drive` 与 `chatlog.Commands.Submit` 推进，不经 app 门面；子 Turn 静止于 `waiting_for_recovery` 时等待其自身执行被控制面收养后继续驱动。Run 事实本体不新增子代理生命周期：父只看到一个以子代理回复应答的工具调用。approval 是另一种 Wait：由人应答，效果仍在效果层；两者只共享 Run 的 Wait 状态与提交入口。

**SPN-2** 调用到子 Session 的绑定是派生的：`ChildSessionID = spawn.ChildID(parent, runID, callID)`（preimage `twilight/spawn/child`）。子段 header 的 `twilight/spawn` 扩展槽（`SegmentHeader.Ext`，SES-WIR-5）记录完整 provenance（父 Session、父 Run、CallID、深度、全量参数），Responder 据此在任何进程中续接同一调用；同一 CallID 以不同参数再次应答为冲突。

**SPN-3** 嵌套深度从 provenance 链得出：未由 spawn 创建的 Session 深度为 0，子的深度为父深度加一。深度达到 `Options.MaxDepth`（默认 3）的 Session 发起 spawn 调用在开始前被拒（response_rejected），不创建子 Session。

**SPN-4** 崩溃接管不需要执行记录：父的 call 停留在 Waiting(ExternalResponse)，是父 ledger 里的事实；子 Session 的进度是子自己的 ledger。新 Owner 打开父 Session 时 Driver 的 Open-time 扫描（DRV-4）对每个有 Responder 的 Waiting call 再次调用 Responder，Responder 重新派生 ChildID、打开子（对子的接管由 `Ports.Ownership` 决定）、按子的持久状态继续：Turn 仍 active 则驱动，已完成则直接读结果提交。子的模型步若属于死去 Owner 的执行，由子自己的接管处置（RUN-CMT-7）撤回重规划。`Application.Close` 取消本进程全部 Responder 的子驱动，子的 Turn 保持 active 等待下一个 Owner。

**SPN-5** 模式 `spawn`（默认）从空 Session 起；`fork` 以 `turn.History.PrefixCommit` 为根，即父在调用 Turn 及其输入之前的全部历史。结算依子的持久状态推进：有 active Turn 则驱动至结算；有 submitted 输入则以其开新 Turn 并驱动；否则比较最新输入与 task——相同且已有 Turn 则读取已结算结果，不同则提交 task 开新 Turn。一个子每个 task 只运行一个 Turn，不排空积压。子 Turn 非 `completed` 时调用被拒绝。

## 9. 事件流（observe）

**OBS-1** `Application.Events(ctx, sid)` 是该 Session 从订阅时刻起的事件流：`observe.Bus` 以 `writer.CommitObserver` 接在 Writers 上（EXT-WRT-7），每个已应用组的每一行经 Registry 解码为 `Event{Position, Row, Module, Version, Value, Unknown}`，按提交顺序交付，`Position{Commit, Index}` 是该行在 ledger 中的位置；无 codec 的类型或版本以 `Unknown` 交付原行。同一条流还承载临时的进度观察：`Event.Progress{RunID, Effect, Generation, Sequence, Kind, Payload}`，来自 Executor 的进度帧（RUN-EXE-12）经 Loop 的 sink 与 `Driver.Sink`（`app.Build` 接为 Bus）发布，`Row` 为零；它不是事实，可丢失，`progress_reset` 作废同一 effect 此前的帧，随后落下的已提交结果取代它。UI 的渲染规则由此确定：按 delta 累积，收到 reset 时丢弃该 effect 已累积的内容，收到该步的已提交事实后以冻结正文替换。订阅者之间互不阻塞，慢读者只延迟自己的交付，从不阻塞 Commit。UI、SSE 与 CLI 的观察都从这一个源头派生；Loop 的 `EventSink` 只是 Loop 到 Bus 的适配器，不是第二条观察通道。

**OBS-2（catch-up 订阅与 checkpoint）** `Application.EventsFrom(ctx, sid, from)` 是从 `CommitSeq` `from` 起的订阅：Bus 先注册 live 订阅者，再经 `Store.ReadCommits` 读出 `from` 到 head 的历史并按 `Position` 交付，随后交付 live 事件，其中 `Position.Commit` 低于读取时 head 的 live 事件已由历史覆盖、不再交付；因此订阅期间的提交不丢，历史覆盖的提交不重复。失败与进度事件没有 Position，按到达交付。消费者保存自己处理到的位置（`agentcore/checkpoint.Store`：按 consumer 与 ledger 名保存 `Next`，只能前移，SQLite 实现与 execution ledger 同库），崩溃后以该位置调用 `EventsFrom` 续接，每个提交至少交付一次；消费者状态与 checkpoint 在同一事务写入时恰好一次，否则消费者须幂等。这是 Session 侧的 catch-up 读取原语，Execution 侧的对应物是 `executionstore.Store.Read(key, from)`（RUN-EXE-14）；两者是跨 authority 消费者的读取原语。

## 10. 组成

```text
// owner.New
registry    = extension.BuildRegistry(v1, chatlog.Module, runmod.Module, attempt.Module, turn.Module, Ports.Modules...)
frozen      = runmod.FrozenValues(Content)
content     = runmod.NewContent(frozen)                          // materializer：prompt、Reply、transcript
writers     = writer.NewWriters(Store, registry, Admission{Artifacts}, Ownership, {Cache, CachePolicy: runmod.WriterCachePolicy(CacheEvery), Observers})
runtime     = runmod.NewRuntime{Writers, registry, Store, Frozen: frozen, Bindings: Artifacts.Bindings, Cache, Clock}
projections = writer.Projections(writers)
turns       = turn.Coordinator{Writers, runtime}                 // 纯协议：命令经传入的 Writer 提交 + Status 读取
chatlog     = chatlog.Commands{Clock}
driver      = driver.New{runtime, turns, Executor, Presets, Decisions, Sources{projections, content}, Targets: Ports.TargetResolver, Fail}   // 规划读经传入 Writer 的投影；Targets 为 nil 时 effect 无 target（APP-TGT-1）
                                                                  // Loop 按 PresetRef 在 driver 内组合并缓存
// app.Build
routes      = Default(local | port | remote)                                          // backend 选择一次，持久化为 ExecutionRef.Provider
executor    = executor.NewWorker(ctx, Config.Executions, routes, Config.Worker)  // 本地模式恒经 Worker；Port/远端仅在配置 spawn 时经 Worker；Executions 必填（OWN-PRT-3）
                                                                  // Config.Worker.Progress 为 Worker 与本地 backend 共用的 ProgressHub（RUN-EXE-12）；本地 backend 以 streaming 打开
owner       = owner.New(Ports{..., Executor: executor, Observers: [observe.Bus, Config.Observers...], Fail: warn+bus.Failed})
spawn.Bind(owner); driver.Responders[agent_spawn] = spawn      // 子代理为 Responder（SPN-1、DRV-4），经 Owner.Open + turn/chatlog/driver 命令驱动
```

## 11. 部署形态

```text
本地（colocated）          Store: filestore    Content: filestore    Executor: NewLocalExecutor(Catalog)  一个进程
云端（Session Service）    Store: 共享/数据库   Content: 共享 cas     Executor: 远端 worker 的客户端        Owner 进程无模型客户端、无工具实现
                           Executor 进程：Catalog + Assignment/Outcome 传输，无 Store、无 Content
```

两种形态用同一个 `owner.New` 与 `app.Build`，差别只在端口实现。core 与运行时在两者之间没有一行分叉代码。

## 12. 未决

- **公共读取的成本**：`extension.NewProjectionReader` 每次 Load 从最近的缓存条目起折叠尾部提交，`CacheEvery` 决定尾部长度；`Coordinator.Status`、prompt 构造之外的应用读取都走这条路。命令路径（Loop 的 `Runtime.Load`）读 Writer 内存投影，不受影响。若公共读取成为瓶颈，后续是 owner 进程内一份随 Writer 更新、按 head 校验的只读缓存，仍不经 `Writers`。
- **重复 Open 的策略**：当前同一 Owner 内一个 Session 同时只有一代所有权（`ErrSessionOpen`）；若产品需要两个门面共享一个 Session，替代方案是共享 openSession 加引用计数。

## 13. conformance

- **OWN-SCP-3 / OWN-PRT-2**：以只记录 Assignment 的 Executor 组装 Owner，注册 AgentPreset、Send 一条输入：模型 Assignment 被 Dispatch 且携带冻结请求的 digest，该 digest 在 Frozen 中可取回，Outcome 回送后 Turn `completed`、`Reply` 等于 Outcome 文本。
- **PST-1/2**：同 ID 注册不同 SystemPrompt 得到不同摘要，两版均可解析；修改注册入参或 Resolve 返回值中的嵌套字段保持注册版本不变；未知 ID 或摘要返回 `ErrUnavailable`；未注册的 PromptBuilderRef 使 Loop 组合失败。
- **OWN-HDL-1**：同一 Session 第二次 Open 为 `ErrSessionOpen`，一代处于 opening 或 closing 时同样如此；关闭后重新 Open，旧 Handle 的 Close 不影响新一代（其 Writer 仍可提交）；按 SessionID 的读取在 Handle 关闭前后都可用且不重新打开 Session。
- **OWN-HDL-2/3**：命令以另一 Session 的 Writer 调用返回 conflict；接管后被替代进程的 Writer 上的 Commit/Deliver 得到 ownership lost 且不改变 ledger（runtimetest、turntest）；被替代进程按 SessionID 的 `Record` 仍读到新 owner 留下的状态；失去所有权的 Loop 在下一次提交返回 ownership lost 并停止结算（loop ownership/takeover 测试）；接管只在新进程打开 Writer 时发生，读取不触发；app module 经同一 Writer 提交的事件与 chatlog 输入出现在同一 ledger。
- **DRV-1/2**：同一 Run 的第二个本地驱动者得到 `AlreadyDriving` 的成功响应；ctx 取消后 Turn 保持 active、重开后驱动完成。
- **APP-RTE-1/2**：active Turn 时 Route 走 Deliver，输入在下一次模型请求里紧随工具结果之后；无 active Turn 时 Route 开新 Turn；Drain 取全部积压开一个 Turn；`attempt_failed` 时 Route 为 conflict。
- **DRV-3**：`missing` execution record 的工具记 Unknown 且同一 RunID 继续；缺失记录的模型步被撤回，Resume 时重新规划（`ModelSteps` 只计重规划的那一步）；`active`/`terminal` attempt 以实际 Outcome 完成原步骤，`orphaned` 映射为 `deferred` 并保持 Executing；Open 请求取消后恢复监听继续，Session/Owner 关闭后监听退出；旧进程的迟到结算被围栏。
- **APP-SES-1/2/3**：OpenSession 顺序；Send 的首个 Result 与排空 Result；并发 Send 的 `AlreadyDriving` 收敛。
- **APP-SES-4、OBS-1**：Submit 在模型仍阻塞时已返回且 Turn 为 active；事件流按 Seq 顺序交付该 Turn 的 `started`、attempt 模块的 `started` 与其 Run 的 `run_ended`；后台驱动失败以 `Event{Err}` 与 `Config.Warn` 报告；同一 Session 上 Send 仍阻塞到 Result。
- **APP-CKP-1/2**：Compact 的模型请求经 Executor 到达模型；压缩后下一请求以 summary 开头且只含 retained 后缀；重启进程组装同一上下文；active Turn 时 Compact 为 conflict；封闭校验的四类边界。
- **SPN-1..5、DRV-4**：spawn 调用以派生身份建子 Session 并以子回复应答父的工具调用（chatlog 条目 Source 为 tool_response）；fork 模式拿到当前 Turn 之前的对话且收到 task；参数错误、未知命名 Preset 与深度超限在建子之前被拒为 response_rejected 且不建子；所有者进程在子模型调用中途退出后，新进程打开父 Session 时 Responder 续接同一子并完成父 Turn，续接后子的 Turn 数与输入数不变。
- **APP-MEM-2**：`CacheEvery` 到达 Writer；machine projection 从不被 Writer 写入。
- **OWN-FRK-1/2**：父的 Turn 活动中时以该 commit 为点的 fork 被拒且不留根，Turn 结算后同一点可 fork；子对父 Run 的 `Record` 为 `ErrRunNotFound`，继承的 Turn 在子的 surface 上为 completed；在某 Turn 之前 fork 得到的子 Session 只含该 Turn 之前的回答且其输入仍待投递；`Drain` 以同一输入重新生成，`Withdraw` 后 `Send` 以新输入替代；两个子都读到共享前缀的冻结正文；父的 head 不变；每个子的首个自身 Commit 从 anchor 续链；未知 Turn 与自身为父被拒。
