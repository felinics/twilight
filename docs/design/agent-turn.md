# Twilight Agent Turn 协议

状态：v1 设计规范。本文定义 Turn 协议。Coordinator 只做协议提交与状态读取（Start / Deliver / Retry / Stop / Settle / Status），驱动属宿主；写入经 `writer.Writer`、以 `Seq` 定位、恢复走接管处置。Run 事实与 Turn、Chatlog 事件同在一条 Session Commit Ledger。

本文定义 `agentcore/turn`：回合生命周期、Run attempt 的创建与结算。attempt 的终态由 Run 自己的 `run_ended` 事实投影得到，本模块不写它的第二份表达：事实按发生的域只写一次，投影可以跨流折叠；判定一个事件是否该存在的标准是它是否记录了本域的独立决定（Retry、Stop、Settle、Superseded 是；`run_ended` 的确定性镜像不是），而不是某个投影希望只读哪条流。"必须""应该"为协议约束。Run Machine 与 Runtime 的 authority 是 [agent-run.md](agent-run.md)；对话内容的 authority 是 [agent-session-chatlog.md](agent-session-chatlog.md)；ledger、commit 与 projection 机制的 authority 是 [agent-session.md](agent-session.md) 与 [agent-session-extension.md](agent-session-extension.md)。

## 1. 模型与范围

```text
Turn   逻辑回合。由一组 delivered Input 触发，以 completed / failed / superseded 结束。
Run    完成一个 Turn 的一次 attempt。同一 Turn 至多一个非终态 Run；可以有多个已终结的 Run。
```

| Concern | Canonical owner | 写入者 |
|---|---|---|
| 回合存在、attempt 归属与结束 | `twilight/turn/` events | Coordinator |
| Run 执行状态 | `twilight/run/` events（[agent-run.md](agent-run.md)） | `runmod.SessionRunStore`：Loop 经 `Bind(w)` 得到的 `runtime.RunStore` 提交命令，Coordinator 在 unit of work 里放入它的 `Command` / `CreateRun` Part |
| 对话内容 | `twilight/chatlog/` events 与 `twilight/run/` 事实的投影 | Start 与 Deliver 时 delivered input；assistant 与 tool_result 是 Run 事实的投影条目（CHT-ENT-1/2），不另写事件 |
| Application policy | Application | preset、driver、retry、context 策略、产品策略 |

**TRN-SCP-1** Source 为 `twilight`，ModuleID 为 `turn`。一个 Turn 与它的全部 Run attempt 注册在同一 Session Commit Ledger 内，但分属两个模块：Turn 到 Run 的绑定是 attempt 模块的事实（attempt/&lt;TurnID&gt; 流上的 `twilight/attempt/started`，ATT-1，它建立 RunID 到 TurnID 的路由），终态由该 Run 在 run/&lt;RunID&gt; 流上的 `twilight/run/run_ended` 事实折叠得到；turn/&lt;TurnID&gt; 流只承载对话生命周期（started、failed、superseded）。surface 跨 turn、attempt、chatlog、run 四个 domain 的流折叠（EXT-PRJ-1）。run/&lt;RunID&gt; 流因此是 canonical history 的一部分，与 turn/&lt;TurnID&gt; 流同为长期保留的事实。本模块声明流 domain `turn`（Key 为事件的 TurnID，`LineageSession`，EXT-STR-1），每个 Turn 一条流，Turn 的事件全部写入它；可回收的是冻结正文、execution record 与投影缓存，历史长度问题日后经 compaction 产出的 summary（对某个前缀的权威 materialization）加尾部折叠解决，而不是复制第二套事实。fork 继承 run/&lt;RunID&gt; 流中的历史证据但不继承其执行所有权（SES-FRK-5、EXT-PRJ-8）。`Coordinator` 创建 Turn、在同一 unit 里写入 attempt 模块的绑定（ATT-2）、投递输入、停止及结算 Turn；宿主驱动 Run。Run 事实不记录 Turn；Run 到 Turn 的绑定只在 attempt 模块的事实里（ATT-1）。本模块的 `Requires`（EXT-REG-4）为：`attempt`（`twilight/attempt/started`）、`run`（`twilight/run/run_ended`）、`chatlog`（`twilight/chatlog/input_delivered`）。

### 1.1 attempt 模块

**ATT-1（绑定事实）** first-party 模块 `agentcore/session/attempt`（Source `twilight`，ModuleID `attempt`）拥有流 domain `attempt`（Key 为 TurnID，`LineageSession`，每个 Turn 一条流）与唯一事实 `twilight/attempt/started{turnId, runId, attempt}`：Turn 的第 N 次 attempt 由该 Run 执行。`attempt` 从 1 起连续；同一 Run 只能绑定一次。投影 `twilight/attempt/index`（authoritative）按 Run 与按 Turn 索引全部绑定，对重复绑定的 Run 与不连续的 ordinal 在 fold 阶段拒绝。attempt 是否终结、如何终结不是本模块的事实，它是该 Run 的 `run_ended`。本模块不 `Requires` 任何模块，run 模块也不知道它的存在。

**ATT-2（写入点）** 绑定只在为 Turn 创建 Run 的 unit 里写入：Turn 的 `Start` 与 `Retry` 把 `attempt.Started(turnID, runID, n, now)` 作为一个 batch 放进同一 `unit.Work`，与 `twilight/turn/started`（首次）、chatlog 的 `input_delivered` 与 run 模块的 `run_created`/`input_accepted` 同 commit 可见（TRN-STR-2、TRN-RTY-1）。除 Coordinator 之外没有写入方。

**ATT-3（消费方）** turn 的 surface 消费 `twilight/attempt/started` 建立 `RunOwner` 与 `TurnView.Attempts`，再以 `run_ended` 结算（TRN-PRJ-1）；宿主按 RunID 找 Turn 可读 surface 的 `OwnerOf`，或直接读 `attempt.Read(...)` 得到的 `Index`。对话单元（turn）与执行调度（attempt）由此各自演进：Turn 流的 codec 变化不涉及绑定，绑定的变化（例如记录调度决策的来源）不涉及 Turn。

**TRN-SCP-2** Turn 与 Run 的关系为 1:N。同一 Turn 至多一个非终态 Run，同一 Session 至多一个 `active` Turn。Start 与新 Retry 在 Writer 的串行提交边界内校验这一约束；已提交操作按各自的重放规则确认：

| 动作 | 语义 | RunID |
|---|---|---|
| resume | 继续一个非终态 Run（进程重启、接管、Waiting 响应后） | 不变 |
| retry | 前一 Run 已终结且未 completed，同一 Turn 再开一个 attempt | 新 RunID，`Attempt` 加 1 |
| replace | 输入内容被替换，`twilight/turn/superseded` 指向新 Turn | 新 Turn、新 RunID |
| regenerate | 已 completed 的回答需要重新生成：在该 Turn 之前 fork（OWN-FRK-2），子 Session 里以同一输入开新 Turn；同一 Session 内也可提交新 Input 开新 Turn 并以 `superseded` 关联。fork 只分叉对话历史，不恢复 workspace 状态（TRN-DUR-3） | 新 Session 或新 Turn、新 RunID |
| edit | 已投递的输入需要修改：在该 Turn 之前 fork，撤回原输入（`input_withdrawn`）后提交新输入。fork 只分叉对话历史，不恢复 workspace 状态（TRN-DUR-3） | 新 Session、新 Turn、新 RunID |

四种动作的持久性语义在第 7 节 TRN-DUR-1 至 4 逐条区分。subagent 使用独立 Session 与独立 Turn。

**TRN-SCP-3** Coordinator 没有隐藏状态。它从 `twilight/turn/surface` 投影与 `twilight/run/machine` 投影重建。

**TRN-SCP-4** 每个 Turn 命令是一个 unit of work（`agentcore/session/unit`）：Turn 自己的 Part、chatlog 的 `DeliverInputs` Part 与 Run 模块的 `CreateRun` / `Command` Part 在同一 View 上准备，经同一个 Writer 一次落盘（EXT-SCP-1）。Coordinator 不编码任何其他模块的事件。命令以调用方持有的 Writer 为参数（所有权能力，OWN-HDL-2）；Status 经 `extension.ProjectionReader` 与 `SessionRunStore.Record` 按 SessionID 读取，不取得 Writer。Artifact 由其 owner 管理。

**TRN-SCP-5** Application 管理 model、provider、tool、prompt、token、approval、queue、retry 决策与并发。宿主按 persisted preset 解析 driver 并驱动（DRV-1）。PromptBuilder 按 AgentPreset 的 `Prompt` ref 解析（DEC-CAT-2），每次 Build 使用 AgentPreset 的 `ModelRef`；Scheduling 与 MalformedRetries 是 AgentPreset 上的数据，Loop 直接读取。

**TRN-SCP-6** Start 之前建立 immutable execution preset。Session 保存 `PresetRef{ID, Digest}`。密钥与 client 留在进程内。Resolve 失败返回 `preset_unavailable`。字段与 digest 边界见 TRN-PST-1/2；决策组件的身份与解析见 [agent-decision.md](agent-decision.md)。

## 2. identity 与事件

```go
type TurnID string
type TurnRef struct { SessionID session.SessionID; TurnID TurnID }
type PresetRef struct { ID PresetID; Digest es.Digest }

// AgentPreset 是 Turn 记录的决策身份；Session 只保存 PresetRef{ID, Digest}。
type PromptBuilderRef string // 决策组件身份（agent-decision.md）
type TargetRef struct { Kind string; ID string } // opaque effect target
type PublicTool struct { Ref run.ToolRef; Definition model.ToolDefinition; Policy run.ResponsePolicy; Replay run.ReplayPolicy; Placement run.ToolPlacement }
type AgentPreset struct {
    SchemaVersion uint16 // 1
    Model run.ModelRef
    Tools []PublicTool     // ToolSpec 与 Request.Tools 都由此派生
    Streaming bool
    Prompt PromptBuilderRef          // 决策组件身份（agent-decision.md）
    Scheduling run.ToolScheduling    // 工具调用并行/串行与并发上限，冻结进 ToolStep（RUN-MCH）
    MalformedRetries uint8           // 畸形模型结果的同步重试上限；0 即首次失败
    SystemPrompt string    // 由 preset digest 冻结的对话指令
}
func DigestPreset(*AgentPreset) (es.Digest, error)
func ValidatePreset(*AgentPreset) error

type Settlement string
const (
    SettlementCompleted Settlement = "completed"
    SettlementFailed    Settlement = "failed"
    SettlementStopped   Settlement = "stopped"
)

type StartedPayload struct {
    TurnID TurnID
    InputIDs []chatlog.InputID
    Preset PresetRef
}
type AttemptStartedPayload struct {
    TurnID TurnID
    RunID run.RunID
    Attempt uint32
}
type FailedPayload struct {
    TurnID TurnID
    RunID run.RunID // 最后一个 attempt
    Settlement Settlement // failed | stopped
    FailureClass string
}
type SupersededPayload struct {
    TurnID TurnID
    ReplacementTurnID TurnID
}
```

**TRN-ID-1** `TurnRef`、RunID、preset ID、InputID 与 digest 非空且稳定。

**TRN-ID-2** `PlanDigest = Digest("twilight/turn/plan", TurnID, AgentPreset.Digest, ordered InputIDs)`。PlanDigest 只参与 TRN-ID-3 的派生，不落盘：`started` payload 的每个字段都是它的 preimage 成员，落盘该 digest 不提供额外判定。

**TRN-ID-3** `StartOperationDigest = Digest("twilight/turn/start-operation", SessionID, TurnID, PlanDigest)`。用户正文 identity 在对应 `twilight/chatlog/input_submitted` 中。

**TRN-PST-1** `AgentPreset.Digest = Digest("twilight/turn/preset", SchemaVersion, Model, Tools, Streaming, Prompt, Scheduling, MalformedRetries, SystemPrompt)`。摘要冻结模型与工具身份、prompt 构造组件、调度与畸形结果策略、对话指令。同一 `PresetRef` 解析为相同的决策输入（DEC-SCP-1/2）；外部执行目标由 application 绑定。

**TRN-PST-2** SchemaVersion、Model、Prompt 非空，且 Scheduling 的 Mode 合法（空、parallel 或 sequential）、MaxParallel 非负，是 AgentPreset 可被注册的前提（`ValidatePreset`）。宿主按完整 `PresetRef{ID, Digest}` 保存不可变版本，注册与解析时按 TRN-PST-1 校验摘要。相同 ID 的内容变更产生新 ref，旧 ref 继续解析为原版本；注册输入与解析结果均为独立值。宿主负责提供恢复所需的历史版本。

**TRN-ID-4** attempt 的 RunID 由 Coordinator 派生：`RunID = Digest("twilight/turn/run", SessionID, TurnID, Attempt)`。Attempt 从 1 开始。Run 自身不记录 Turn 与 attempt（RUN-NEW-1）；两者只在 `twilight/attempt/started{turnId, runId, attempt}` 中（ATT-1）。

**TRN-EVT-1** EventType：

```text
twilight/turn/started
twilight/turn/failed
twilight/turn/superseded
```

四种事件都是 Turn 域自己的决定。attempt 的终态与 Turn 的 `completed` 不是事件：它们由 `twilight/run/run_ended` 折叠得到（TRN-PRJ-1）。unsettled Turn 是尚未 completed、failed 或 superseded 的 `started`。

**TRN-EVT-2** 本模块产生的事件（含 Attach 产生的）没有独立 EventID，`Seq` 即身份（SES-WIR-1）；同一次写入的事件共用 CommitID。Start 的 CommitID 由 StartOperationDigest 派生；Retry、Settle、Stop 的 CommitID 见各自条目。重放只按 CommitID 判定（EXT-WRT-2）：同 CommitID 为 already-applied，以先提交的为准，例如同一 Turn 以不同 FailureClass 再次 Settle 得到第一次的结算。事件时间戳不参与幂等判定。

**TRN-EVT-3** 每个 Turn 的流 turn/&lt;TurnID&gt; 内至多一条 `started`，至多一条 `failed` / `superseded`；一个 Turn 至多有一个 attempt 以 `run_ended(completed)` 终结。

**TRN-PRJ-1** ProjectionID 为 `twilight/turn/surface`。消费 `twilight/turn/started|failed|superseded`、`twilight/attempt/started`、`twilight/chatlog/input_delivered` 与 `twilight/run/run_ended`，其他事件按 EXT-PRJ-2 处理。`run_ended` 经 `RunOwner`（由 `attempt/started` 建立）路由到它终结的 attempt：`RunOwner` 中没有的 RunID 不属于本 Session 的 Turn，跳过；已有 `End` 的 attempt 再次终结在 fold 阶段报错，不写入。`completed` 结束使 Turn 为 `completed`；`failed` 与 `stopped` 结束使 `active` 的 Turn 进入 `attempt_failed`，同 commit 附加的 `failed` 事件（TRN-STP-1）随后把它结算为 `stopped`：

```go
type TurnStatus string
const (
    TurnActive        TurnStatus = "active"         // 存在非终态 Run
    TurnAttemptFailed TurnStatus = "attempt_failed" // 最后一个 Run 已终结且未 completed，Turn 未结算
    TurnCompleted     TurnStatus = "completed"
    TurnFailed        TurnStatus = "failed"
    TurnStopped       TurnStatus = "stopped"
    TurnSuperseded    TurnStatus = "superseded"
)
type AttemptView struct {
    RunID run.RunID
    Attempt uint32
    End *run.RunEnded    // 非终态时为 nil；终态来自该 Run 的 twilight/run/run_ended
}
type TurnView struct {
    TurnID TurnID
    Status TurnStatus
    InputIDs []chatlog.InputID // started 的初始输入，加此后经 Deliver 进入任一 attempt 的输入，按 accepted 顺序去重
    Preset PresetRef
    Attempts []AttemptView // 按 Attempt 递增
    ActiveRun run.RunID    // Status=active 时非空
    ReplacementTurnID TurnID
}
type TurnSurface struct {
    Order []TurnID
    Turns map[TurnID]TurnView
    RunOwner map[run.RunID]TurnID // attempt/started 建立；宿主按 RunID 找 Turn
}
```

UI 按 `TurnID` 连接 `twilight/chatlog/surface` 的条目，按 `RunID` 连接 `twilight/run/machine` 的实时视图。终态 attempt 的结果记录在 `AttemptView.End`，来自 run/&lt;RunID&gt; 流的 `run_ended`；surface 的重建跨 turn、attempt、chatlog、run 四个 domain 的流折叠。

## 3. API

```go
type Coordinator struct {
    Projections extension.ProjectionReader // Status 的无所有权读侧
    Runs *runmod.SessionRunStore           // Record 与 Run 模块的 Part
}

// Commands 只做协议提交；驱动 Run 属 driver（DRV）。每个命令以该 Session 的
// Writer——调用方的所有权能力（OWN-HDL-2）——为参数，经它提交；请求所指
// Session 与 Writer 不一致为 conflict。每个方法在提交落盘后立即返回，响应反映已提交的状态。
type Commands interface {
    Start(context.Context, writer.Writer, StartRequest) (TurnResponse, error)
    Deliver(context.Context, writer.Writer, DeliverRequest) (TurnResponse, error)
    Retry(context.Context, writer.Writer, RetryRequest) (TurnResponse, error)
    Stop(context.Context, writer.Writer, StopRequest) (TurnResponse, error)
    Settle(context.Context, writer.Writer, SettleRequest) (TurnResponse, error)
}
// Reader 是状态读取，不要求所有权。
type Reader interface {
    Status(context.Context, TurnRef) (TurnResponse, error)
}
type StartRequest struct {
    Ref TurnRef
    Inputs []run.AgentInput // ID 为已 submitted 的 InputID，Payload 等于其 Content
    Preset PresetRef
}
type DeliverRequest struct { Ref TurnRef; Inputs []run.AgentInput } // 回合中途追加输入
type RetryRequest struct {
    Ref TurnRef
    PreviousRunID run.RunID // 本次重试所接续的失败 attempt
    Reason string
}
type StopRequest struct { Ref TurnRef; Reason string }
type SettleRequest struct { Ref TurnRef; FailureClass string }
type TurnResponse struct {
    Ref TurnRef
    RunID run.RunID
    Attempt uint32
    Status TurnStatus
    Disposition ResumeDisposition
    End *run.RunEnd // 该 attempt 已终结时非空，来自 twilight/run/run_ended
    Waiting []run.ResponseRequest
}
type ResumeDisposition string
const (
    ResumeWaitingForResponse ResumeDisposition = "waiting_for_response"
    ResumeWaitingForRecovery ResumeDisposition = "waiting_for_recovery"
    ResumeFinished           ResumeDisposition = "finished"
)
```

"同 Run 的另一个本地驱动者正在推进"不是 Turn 的持久状态，不进入该词汇表：driver 以 `DriveResult.AlreadyDriving` 单独报告（DRV-1）。

**TRN-API-1** Coordinator 经 Writer 的 `Projections()` 读取 `twilight/turn/surface` 与 `twilight/run/machine` 两个投影（EXT-PRJ-4）；命令读传入 Writer 的投影，每个方法先读投影再决定动作。Coordinator 不持有 `session.Store`。

**TRN-API-2** Run 的写入只经 Run 模块自己的 Part（`runmod.Command`、`runmod.CreateRun`）与 Loop 手里的 `runtime.RunStore`。driver 的组装与解析在宿主（PST-2）。

**TRN-API-3** DTO 为值语义。`Waiting` 为对 `twilight/run/machine` 投影状态求 `plan.WaitingCalls` 的结果。`plan.NeedsRecovery` 为 true 时返回 `ResumeWaitingForRecovery`，表示仍有待结算的 Executing 目标；宿主按 RUN-CMT-7 重连或接管处置。`ResumeWaitingForRecovery` 是 Turn API 的观察 disposition，不等同于 Executor 的 `AttachmentState`；其中 `AttachmentState=orphaned` 经 `agentcore/run/reconcile` 映射为 `Verdict=defer`，在显式 reconcile/takeover 前保持该 disposition。可重连与 deferred 目标在处置后仍可保持 `ResumeWaitingForRecovery`，直到实际结算。

**TRN-API-4** `twilight/turn/superseded` 由 Application 追加。Coordinator 的方法不写该事件。superseded 的 Turn 若仍有非终态 Run，Application 必须先 Stop。

## 4. Start 与 Retry

**TRN-STR-1** StartRequest：

1. Ref、preset ref 非空；
2. `Inputs` 无重复 ID；每个 ID 对应 chatlog 中状态为 submitted 的 Input，Payload 等于其 Content（Coordinator 经 chatlog surface 投影核对）。

`started.InputIDs` 与 `input_delivered`、`input_accepted` 的顺序都取 `Inputs` 的顺序。

**TRN-STR-2** Start 是一次原子 commit，顺序为：

```text
twilight/turn/started{TurnID, InputIDs, Preset}
twilight/attempt/started{TurnID, RunID, Attempt:1}
twilight/chatlog/input_delivered{InputIDs[0], TurnID}
...
twilight/chatlog/input_delivered{InputIDs[n-1], TurnID}
twilight/run/run_created{RunID, CausationID}
twilight/run/input_accepted{RunID, InputIDs[0], Payload}
...
twilight/run/input_accepted{RunID, InputIDs[n-1], Payload}
```

InputIDs 为空时 group 为 `started`、`attempt/started` 加 `run_created`。`attempt/started` 由 attempt 模块的 `attempt.Started` batch 写入（ATT-2），`run_created` 与 `input_accepted` 由 Run 模块的 `runmod.CreateRun(newRun, inputs)` Part 写入（RUN-NEW-1），`input_delivered` 由 chatlog 的 `DeliverInputs` Part 写入并在同一 View 上检查 TRN-STR-1 (2)；Coordinator 只负责把这些 Part 放进同一个 `unit.Work`。

**TRN-STR-3** 派生 PlanDigest、StartOperationDigest、RunID 与 group identity，再经 `unit.Commit` 写入一组。相同 identity 为 applied / already-applied；Writer 串行执行全部写入，不存在 head conflict。

**TRN-STR-4** append 成功后 Start 返回已提交状态的响应；驱动新 Run 是宿主的下一步（DRV-1）。

**TRN-RTY-1** 新 Retry 要求 Turn 为 `attempt_failed`、`PreviousRunID` 指向该 Turn 最新的失败 attempt n、Session 当前无 `active` Turn。Coordinator 在 Writer 的串行提交边界内校验这些条件。commit 的 session 侧为 `twilight/attempt/started{Attempt: n+1}`（ATT-2），run 侧为 `twilight/run/run_created` 加该 Turn 已 delivered 的全部 Input 的 `input_accepted`，顺序与 `TurnView.InputIDs` 相同（初始输入在前，中途 Deliver 的输入按 accepted 顺序在后）；payload 与首次 delivered 时相同，仅 RunID 与 Attempt 不同。准入失败返回 conflict。

**TRN-RTY-2** Coordinator 在该 Turn 的历史中按 `PreviousRunID` 取得 previous attempt，令 `Attempt = previous.Attempt + 1`。Retry 的 CommitID 由 `Digest("twilight/turn/retry", SessionID, TurnID, Attempt)` 派生。Coordinator 先查询该 CommitID：已提交时直接返回原 Retry 创建的 RunID 与 Attempt，以及当前 Turn 状态和该 attempt 的 End；确认后 ledger 保持原样。该重放先于 TRN-RTY-1 的准入校验，适用于后继 attempt 已 active、已终结、已有更晚 Retry、另一 Turn active 或接管后的情形。下一次新的 Retry 显式传入新的失败 RunID。

**TRN-RTY-3** 失败 attempt 已提交的 Run 事实保留在 ledger 中，其 assistant 与 tool_result 投影条目随之保留，协议不删除、不隐藏。它们是否进入后续 attempt 的模型请求是 Application 策略，由 PromptBuilder 依据 turn surface 的 attempt 状态决定（DEC-PMT-6）；协议只保证内容可用。

## 5. Deliver、Status 与 Stop

**TRN-DLV-1** Deliver 在回合中途追加输入，要求 Turn 为 `active`；`attempt_failed`、已结算或不存在的 Turn 返回 conflict，输入保持 `submitted`，由 Application 决定开新 Turn。输入的校验与 TRN-STR-1 第 2 条相同：每个输入必须是 chatlog 中状态为 `submitted` 的 Input 且内容一致，任一不满足即 conflict，整批不写入。该校验读 chatlog surface，在 `Runtime.Commit` 之前、Writer 互斥区之外进行；校验与提交之间输入被 withdraw 的竞态由 chatlog 投影的预折叠兜底——`input_delivered` 对非 `submitted` 的输入折叠失败，整组被拒（EXT-PRJ-1），结果仍是全有或全无。

**TRN-DLV-2** `Inputs` 作为一个批次以一次 `Runtime.Commit` 提交：命令为 `AcceptInput{Inputs}`（有序列表，RUN-MCH-4），`Attach` 为每个输入一条 `twilight/chatlog/input_delivered{InputID, TurnID}`。Run 接受全部输入与 chatlog 把全部输入挂到 Turn 在同一组可见；任一输入被拒则一条都不写。envelope 经 `schema.Wire.Envelope` 构造（RUN-WIR-3）；`Base` 为零值（`AcceptInput` 不做 hard CAS，RUN-CMT-4）；Deliver 不读取 `twilight/run/machine` 投影。`AcceptInput` 在 Run 的任意非终态都被接受，Deliver 不关心 Run 当前处于哪一步。CommandID 由 RunID 与有序 InputID 列表派生（RUN-WIR-4），同一批次重放幂等；不同批次（含子集或另一顺序）是不同命令，其中已接受过的输入使该批次整体被 Decide 以 conflict 拒绝。

**TRN-DLV-3** Deliver 不取消正在进行的模型调用或工具调用；要打断用 Stop。提交后 Deliver 返回；是否驱动由宿主决定（DRV-1），已在驱动时运行中的 Loop 在下一次 Load 看到 `PendingInputs`。Deliver 与该 Run 的最后一步 `SubmitModelResult` 并发时由 Writer 串行定序：输入先提交，Run 回到 `Open` 继续；结果先提交，Run 已终结，Deliver 得到 `ErrRunTerminal` 并返回 `completed`，该输入未被 delivered。

**TRN-STA-1** Status 是纯读取，disposition 判定的单一来源：读投影设置 `Disposition` 与 `End`。Run 终态为 `ResumeFinished`，`End` 取 surface 中该 attempt 的 `AttemptView.End`；`NeedsRecovery` 为 true 为 `ResumeWaitingForRecovery`；仅有 WaitingCalls 为 `ResumeWaitingForResponse`。宿主驱动结束后调用 Status 组装结果（DRV-1）；Start/Deliver/Retry/Stop/Settle 的响应用同一判定。

**TRN-STA-2** EventSink 的 `text_delta` / `reasoning_delta` 为临时观察。Waiting 由 Application 提交 `ApproveToolCall` / `RejectToolCall` / `SubmitToolResponse` 后再次驱动（DRV-1）。

**TRN-STP-1** Stop 要求 Turn 为 `active`。Coordinator 的 unit 由 Run 的 `Command` Part（`CancelRun{Reason:ReasonCancelled}`）与 Turn 自己的 Part（`twilight/turn/failed{Settlement:stopped, FailureClass:"cancelled"}`）组成；两者在同一 commit 可见。envelope 与 Deliver 同样经 `schema.Wire.Envelope` 构造，`Base` 为零值。Stop 结算 Turn，已 delivered 的 PendingInputs 保留归属。Application 单独提交 `CancelRun` 时，Turn 进入 `attempt_failed`；此时 Retry 为新 attempt 重放全部 delivered 输入，Settle 则结束该 Turn。

**TRN-STP-2** Cancel CommandID = `Digest("twilight/turn/cancel-run", SessionID, TurnID, RunID, ReasonCancelled)`。StopRequest.Reason 供审计。

**TRN-STL-1** Settle 要求 Turn 为 `attempt_failed`，追加 `twilight/turn/failed{Settlement:failed, FailureClass}`。CommitID 由 `Digest("twilight/turn/settle", SessionID, TurnID, RunID)` 派生。

## 6. 对话内容与结算：Run 事实的投影

Run 事实只保存执行状态与内容 digest（RUN-WIR-4）。模型文本、工具调用与工具输出的正文在 `frozen.Store` 中，由 Run 的 `Command` Part 在构造时存入；对话条目（assistant、tool_result）与 Turn 的结算都是这些事实的纯投影，本模块与 chatlog 都不再写第二份表达。Run 的 Part 只写 Run facts；TRN-DLV-2、TRN-STP-1 里本模块与 chatlog 的事实由各自的 Part 写在同一个 unit 中。

**TRN-MAP-1** 事实到投影的对应：

| Run fact | 投影结果 |
|---|---|
| `ModelStepCompleted` | chatlog assistant 条目 `{TurnID, RunID, StepID, ResultDigest}`（CHT-ENT-1） |
| `ToolStepOpened` | 补入所属 assistant 条目的 `CallIDs` |
| `ToolCallCompleted` / `ToolCallAnswered` | chatlog tool_result 条目 status=`success`，命名冻结正文 |
| `ToolCallFailed` Outcome=`Known` | tool_result 条目 status=`error`，携带 Failure |
| `ToolCallFailed` Outcome=`Unknown` 或 class=`effect_unknown` | tool_result 条目 status=`unknown`，携带 Failure |
| `RunEnded(completed)` | turn surface：attempt `End`，Turn `completed` |
| `RunEnded(failed / stopped)` | turn surface：attempt `End`，`active` 的 Turn 进入 `attempt_failed`，由 Retry、Stop 的 Attach 或 Settle 决定 |

其余 fact 不影响这两个投影。模型无 tool call 但 Run 有 pending 输入时不产生 `RunEnded`（RUN-MCH 表），Turn 保持 `active`。

**TRN-MAP-2** 条目 identity 直接取 Run identity：`AssistantID = ModelStepID`，`ToolResultID = CallID`；带外替换结果为 `<CallID>/superseded`（CHT-ENT-2）。CallID 由 Run 从 `(ModelStepID, index)` 派生，同一 Turn 内不跨 ModelStep 复用。materializer 按 `Assistant.CallIDs` 与冻结 `ModelResult.ToolCalls` 逐位配对出 `ProviderCallID`（CHT-MAT-1）。

**TRN-MAP-3** assistant 条目的 `ResultDigest` 等于 `ModelStepCompleted.ResultDigest`，tool_result 条目的 `OutputDigest` 等于 `ToolCallCompleted.OutputDigest` 或 `ToolCallAnswered.ResponseDigest`；`ToolCallFailed` 产生的条目没有正文 digest。这些 digest 命名 `frozen.Store` 中的冻结正文，Runtime 在写入事实之前存入正文并登记其 Binding（RUN-CMT-3 第 0 步、RUN-WIR-4）。

**TRN-MAP-4** Known 对应 `error`；Unknown 对应 `unknown`。`tool_result_superseded` 由 Application 写入，本模块不写。

## 7. recovery

### 7.1 Run 的持久性语义

下面四条区分四种表面相似、语义不同的情形。它们的差别只在两个问题上：哪个身份保持不变，谁做出决定。

| 情形 | 保持不变 | 新建 | 决定者 |
|---|---|---|---|
| 进程崩溃 / 所有权丢失 | Turn、Run、attempt | 无 | 无人：接管处置是协议动作 |
| 语义重试 | Turn | Run（attempt 加 1） | Application |
| 重新生成已提交的回答 | 原 Turn 与其回答 | Turn，或 Session 分支 | Application |
| 外部工具执行中且 owner 丢失 | Turn、Run、该 call 的 Unknown 事实 | 无 | 模型或 Application，从不自动 |

**TRN-DUR-1（崩溃恢复同一 Run）** 进程崩溃或所有权丢失不结束 Run，也不创建 attempt。新 owner 的 `RecoverInterrupted`（RUN-CMT-7）对每个 Executing 目标先经 Executor 询问其 attempt 是否仍在执行（start 事实记录了该 effect 的 EffectID）：仍在执行则目标保持 Executing，Outcome 到达时以原 Effect 结算——同一次执行接着算完；不再执行则处置——模型步被撤回，Run 回到 Open，下一次 Prepare 以**恢复时刻**的状态重新规划：Executing 期间投递的输入、此时的上下文与 AgentPreset 都进入新请求，并作为新的 Prepared 事实记录；旧请求不重发。工具 call 记 Unknown。之后同一 RunID 在同一 Turn 下由宿主 Drive 继续。恢复不改变 Run 的身份、attempt 号或已提交的任何事实。

**TRN-DUR-2（语义重试是新 Run、同一 Turn）** Application 显式指定 `PreviousRunID` 发起新 Retry，按 TRN-RTY-1 创建 attempt n+1、新 RunID，并重新接受该 Turn 已 delivered 的全部输入。历史 Retry 按 TRN-RTY-2 确认原提交；崩溃恢复按 TRN-DUR-1 继续原 Run。失败 attempt 的 Run 事实及其投影条目保留在 ledger 中，是否进入新 attempt 的模型请求由 PromptBuilder 决定（TRN-RTY-3）。

**TRN-DUR-3（重新生成与编辑不改写历史）** 已 completed 的 Turn 及其回答是不可变事实：不存在"修改回答"、"重开同一 Turn"或"对 completed Turn 再开 attempt"。`Start` 要求输入处于 `submitted`（TRN-STR-1），已 delivered 的输入不能再次开 Turn。重新生成有两种形态：在该 Turn 之前 fork（SES 第 8 节、OWN-FRK-2），子 Session 继承到该 Turn 开始之前的全部事实，其输入仍为 `submitted`，投递它即为新 Turn；或在同一 Session 内提交新 Input（内容可与原输入相同）并 Start，以 `twilight/turn/superseded` 关联原 Turn（TRN-API-4）。编辑只有 fork 形态：撤回原输入后提交新输入。两种形态都不改写已有历史；原回答是否进入上下文由 PromptBuilder 决定。fork 的范围是 Session 的已提交事实（SES 第 8 节）：子 Session 继承对话历史与 Turn 结算，不继承也不恢复任何 workspace 状态。原 Turn 及其后续 Turn 的工具调用已经作用于父 Session 绑定的 workspace，fork 不撤销这些效果，子 Session 的 Run 在其 target 绑定所解析到的 workspace 上执行（agent-workspace.md）。因此 regenerate 与 edit 在当前协议下只是对话历史层面的分叉；让 fork 点 `k-1` 解析到一个 Turn 边界的 workspace snapshot、并恢复为绑定到子 Session 的新 workspace，属于后续设计，需要 Turn 边界的 snapshot 引用作为前置条件；恢复到哪个 workspace 由 application 的 fork policy 决定（APP-TGT-1）。

**TRN-DUR-4（外部效果未知不等于重试）** owner 丢失时处于 Executing 的工具 call 有两种去向，由 Executor 是否仍持有该 attempt 决定（RUN-CMT-7）：仍持有则等待同一次执行的 Outcome，这是重连；不再持有时，`Replay()` 为 `ReplayAllowed` 的工具由 Worker 在同一 effect 下重派（幂等性由工具自身保证，Run 只看到同一 call 的 Outcome，RUN-EXE-9），声明为 `ReplayForbidden` 或未判断（`ReplayUnknown`）的工具由接管处置记为 Unknown，对话投影得到 status=`unknown` 的 tool_result 条目。Unknown 是该 call 的终态事实，协议在任何路径上都不重新执行它：接管处置不执行（它只记录）；下一次 Loop 不执行（start barrier 只启动 Pending call，Executing 与终态 call 永不重跑，RUN-LOP-4）；Retry 不执行（新 attempt 从上下文重新规划步骤，Unknown 结果作为对话内容可见）。外部效果是否已经发生、是否需要重做，由模型依据上下文判断，或由 Application 在带外核实后以 `tool_result_superseded` 换成 `success`/`error`（CHT-ENT-2）；两者都是决定，不是协议的自动行为。`CancelRun` 留下的 `UncertainCalls` 同理。

### 7.2 恢复表

**TRN-REC-1** 恢复扫描 `twilight/turn/surface` 中 `active` 与 `attempt_failed` 的 Turn。

**TRN-REC-2**

| 情形 | 动作 |
|---|---|
| `started` 已提交、进程在驱动前退出 | 新 owner 的 `RecoverInterrupted` 无事可做（Run 在 Open）；宿主 Drive |
| Loop 的 Commit 返回非 sentinel 错误 | Loop 以同一 Claim 重放一次（RUN-LOP-5）；Writer 按 CommitID 幂等 |
| 模型 Executing、owner 进程崩溃 | 新 owner 的 `RecoverInterrupted` 先经 Executor 询问该 attempt 是否仍在执行：是则保持 Executing、等待其 Outcome；否则提交 `RecoverModelExecution`（RUN-CMT-7），该步撤回、Run 回到 Open，宿主 Drive 时按恢复时刻的状态重新规划。Run 保持 Active，同一 RunID 继续 |
| 工具 Executing、owner 进程崩溃 | 新 owner 的 `RecoverInterrupted` 先经 Executor 询问该 attempt 是否仍在执行：是则保持 Executing、以原 Effect 接受其 Outcome（重连同一次执行，不产生新 attempt）；否则提交该 call 的 Unknown，对话投影得到 status=`unknown` 的条目。Run 保持 Active |
| Writer 返回 `ErrOwnershipLost` | 本进程放弃该 Session 的全部 Turn 与 Loop（RUN-CMT-6）；由持有新 Epoch 的进程按上两行接管 |
| Run 已 `failed`、Turn 未结算 | Turn 为 `attempt_failed`；Application 选择 Retry 或 Settle |
| Stop 的 Commit 返回非 sentinel 错误 | 以同一 Cancel CommandID 重放 |
| Deliver 的 Commit 返回非 sentinel 错误 | 以同一批次 CommandID 重放整批，得到 already-applied（TRN-DLV-2） |
| Start 的 Commit 返回非 sentinel 错误 | 重放相同 StartRequest，以同一 CommitID 确认原提交 |
| Retry 的 Commit 返回非 sentinel 错误 | 重放相同 RetryRequest（含 PreviousRunID），按 TRN-RTY-2 确认原 RunID 与 Attempt |
| preset 缺失 | 宿主 Drive 返回 `preset_unavailable`（PST-2）；Turn 状态不变 |

**TRN-REC-3** 没有跨存储的对账：Run 事实与 Attach 的 Turn 事实在同一组，`Append` 原子，要么全部可见要么全部不可见；对话内容与 Turn 结算是这些事实的投影，没有第二份需要对齐的写入。冻结正文在 Append 之前存入并建立 claim，崩溃只可能留下未被任何事实引用的正文与孤儿 claim，由 artifact 的回收前核对释放（EXT-WRT-3、ART-RET-3）。

## 8. conformance

套件以 `session.Store` 为参数（`agentcore/turn/turntest`），Memory 与每个 durable adapter 跑同一组断言。Coordinator 只做提交与读取，因此套件不含 Loop、driver、模型或工具桩：Run 的推进由 `SessionRunStore.Bind(w)` 的 command 提交完成，Application 的 `CancelRun` 制造 `attempt_failed`，`SubmitModelResult` 制造 completed 与 approval 等待。

- **TRN-STR-1 至 TRN-STR-4、TRN-ID-2/3/4、TRN-EVT-2**：缺 preset、重复 InputID、未 submitted 的输入、Digest 与已提交 Content 的 digest 不符各自被拒且不写入；Start 的 group 为 `started`、每输入一条 `input_delivered`、`attempt/started{TurnID, Attempt:1}`、`run_created`、每输入一条 `input_accepted`，CommitID 为 StartOperationDigest，RunID 为 `twilight/turn/run` 派生值；响应为 `active`、attempt 1、无 disposition；不同时间戳的重放为 already-applied 且不写入；同 TurnID 的另一 plan 与第二个活跃 Turn 为 conflict，被拒输入保持 `submitted`。
- **TRN-DLV-1、TRN-DLV-2**：一个批次的全部 `input_accepted` 与 `input_delivered` 在以批次 CommandID 为 CommitID 的同一 commit；Run 的 `PendingInputs` 与 surface 的 `InputIDs` 追加全部输入；同一批次重放不写入；不存在或非 `active` 的 Turn 为 conflict 且输入保持 `submitted`；未提交的输入或内容不一致的输入使整批 conflict，批内其他输入也不写入、Run 的 `PendingInputs` 不变。
- **TRN-RTY-1、TRN-RTY-2、TRN-RTY-3**：新 Retry 对缺失、错误或非最新失败的 `PreviousRunID`、非 `attempt_failed` Turn、已有其他 `active` Turn 的 Session 返回 conflict；合法请求得到 attempt n+1、`twilight/turn/retry` 派生的 CommitID、`run_created` 加全部已 delivered 输入按 `InputIDs` 顺序的 `input_accepted`（payload 同首次）；surface 的 `InputIDs` 唯一，失败 attempt 的记录保留。相同 RetryRequest 在后继 active、后继失败、另一 Turn active、更晚 Retry 和接管后均返回原 RunID 与 Attempt，ledger 长度保持不变；后续新 Retry 以新的失败 RunID 为 PreviousRunID。
- **TRN-STP-1、TRN-STP-2、TRN-STL-1、TRN-EVT-3**：Stop 的 `CancelRun` 与 `failed{stopped, cancelled}` 在以 Cancel CommandID 为 CommitID 的同一 commit，其中含 `run_ended`；Settle 需要 `attempt_failed`，写 `failed{failed, FailureClass}`，CommitID 为 `twilight/turn/settle` 派生值；已结算（stopped、failed、completed）的 Turn 上 Stop、新 Retry、Settle、Deliver 返回 conflict，历史 Retry 仍按 TRN-RTY-2 确认原提交；completed 由 Run 终结组内的 `run_ended` 折叠得到，该组不含任何 turn 事件。
- **TRN-STA-1、TRN-API-3**：Open 的 Run 无 disposition 与 Waiting；模型 Executing 为 `waiting_for_recovery`；approval 调用为 `waiting_for_response` 且 `Waiting` 含该请求；completed 与 Application 取消的 Run 为 `finished`，`End` 分别为 completed 与 stopped；不存在的 Turn 为 conflict。
- **TRN-PRJ-1、TRN-EVT-3、TRN-SCP-2/3**：Owner 不是本 Session Turn 的 Run 不进入 surface；第二条 `started`、未知 Turn 的结算、第二次结算在 fold 阶段被拒且不写入；已有 `End` 的 attempt 再次 `run_ended` 为 fold 错误；`run_ended(completed)` 使 Turn `completed`，其他结束写入 `AttemptView.End` 并使未结算 Turn 进入 `attempt_failed`、清空 `ActiveRun`；`Order` 按 started 顺序；结算后的 Session 没有活跃 Turn。
- **TRN-REC-1、TRN-REC-2、TRN-SCP-3**：`started` 提交后接管，`RecoverInterrupted` 处置 0 个目标，Status 仅从投影重建为 `active`；模型 Executing 时接管，处置 1 个目标后 disposition 不再是 `waiting_for_recovery`；被替代的 Coordinator 的 Deliver 得到 `ErrOwnershipLost` 且不改变输入状态，新 owner 的 Deliver 成功。
- **TRN-MAP-1 至 TRN-MAP-4**：Run 终结组不含 turn 事件；assistant 与 tool_result 条目的 digest 等于 Run fact 记录值且正文可从 `frozen.Store` 取回；Unknown 的条目 status=`unknown` 且无正文 digest，由 RUN-CMP-2 套件经 Runtime 的组构成与 chatlog 投影观察。
- **TRN-DLV-3** 的并发定序（输入与最后一步结果的两种先后）由 Writer 串行保证，单进程套件不构造并发，以 Deliver 对已终结 Run 的 `completed` 响应作为可观察结果。
- **TRN-PST-1、TRN-PST-2**：SystemPrompt、Streaming、Prompt、Scheduling、MalformedRetries 任一变化改变摘要；相同 ID 注册变更后的 preset 得到新 ref，旧 ref 仍解析为原内容；修改注册输入或解析结果后，再次 Resolve 得到原注册值；缺 SchemaVersion、Model 或 Prompt、Scheduling 非法的 AgentPreset 被拒绝。
- **TRN-DUR-1 至 TRN-DUR-4**：接管后同一 RunID 继续、`Attempt` 不变、Turn 保持 `active`；工具 Executing 时接管，该 call 记 Unknown 后，接管处置、随后的 Loop 与 Retry 的新 attempt 都不再出现该 call 的 `StartToolCall` 或 `ToolCallCompleted`；Retry 得到新 RunID 与 attempt 加 1、Turn 不变；对已 `completed` 的 Turn 以其已 delivered 的输入再次 Start 为 conflict。Unknown call 不被重跑的断言与 Loop 一起在 RUN-LOP-4/5 的套件中验证。
