# Twilight Agent Session Chatlog Module

状态：v1 设计规范。本文定义 chatlog first-party Module。

本文定义 `agentcore/session/chatlog` first-party Module，依赖 [Session](agent-session.md)、[Session Module Framework](agent-session-extension.md) 与 [Run](agent-run.md) 的事实。回合生命周期由 [Turn](agent-turn.md) 拥有。文中的“必须”“不得”“应该”是协议约束；canonical JSON 与 digest 遵循 `agentcore/jsonstable`、`agentcore/es`。

## 1. module 与 ontology

```text
Source       = twilight
ModuleID     = chatlog
EventType    = twilight/chatlog/<name>
Projections  = twilight/chatlog/surface, twilight/chatlog/context
```

Chatlog 拥有对话自身的事实：Input 的生命周期、summary、compaction、带外核实后的结果替换。模型输出与工具结果不是本模块的事实：`assistant` 与 `tool_result` 是 Surface 与 Context 从 `twilight/run/` 事实折叠出的条目（`ModelStepCompleted`、`ToolStepOpened`、`ToolCallCompleted`、`ToolCallAnswered`、`ToolCallFailed`），一个模型或工具结果在 ledger 上只有一份 canonical 表达。条目只保存结构与 digest；正文经 materializer 从 `frozen.Store` 读取（第 8 节）。

```text
twilight/run/ 事实 + twilight/chatlog/ 事实
        ↓ 纯折叠
Surface / Context（结构 + digest）
        ↓ Materialize（IO）
可展示 / 可发送的表示
```

`assistant` 与 `tool_result` 条目携带 `TurnID`（来自绑定该 Run 的 `twilight/attempt/started`）；Input 在 `input_delivered` 之后挂上 TurnID；summary 与 compaction 不携带 TurnID。回合的创建、attempt 与结束由 `twilight/turn/` 事件表达。summary 的外部内容经 `ReferencePart` 关联 Artifact BindingID。

流式 `text_delta` / `reasoning_delta` 由 Loop EventSink 发送，属于临时观察。Chatlog 权威是已提交的事实。

**CHT-SCP-1** 本模块拥有对话自身的事实与两个投影，并声明单例流 domain `chatlog`（`LineageSession`，EXT-STR-1），对话事实全部写入这条流。Application 拥有模型调用、provider transport、发送策略与审计。`turn` 拥有回合与 Run linkage。本模块的 `Requires`（EXT-REG-4）为 `run` 的事实 `model_step_completed`、`tool_step_opened`、`tool_call_completed`、`tool_call_answered`、`tool_call_failed`、`run_ended`，以及 `attempt` 的 `started`（Run 到 Turn 的绑定，ATT-1）；事件中的 `TurnID` 是 opaque 字符串，不需要 turn 的 codec。

## 2. stable entity 与生命周期

```go
type TurnID string
type InputID string
type AssistantID string   // = run.StepID
type ToolResultID string  // = run.CallID；带外替换结果为 "<CallID>/superseded"
type SummaryID string
type CallID string
type CompactionID string
```

`TurnID` 与 turn 模块同一 identity。InputID、AssistantID、ToolResultID、SummaryID 在 chatlog 流内唯一；CallID 在同一 Turn 内唯一。replacement graph 无环，一个实体至多一个直接 replacement。

| 实体 | 来源 | 可变过程 | 终态/替换 | 不变量 |
|---|---|---|---|---|
| Input | `input_submitted` | 无 | delivered / withdrawn / rejected | 只终结一次；delivered 后进入 Context |
| Assistant | `run/model_step_completed`，其 `tool_step_opened` 补入 CallIDs | 无 | — | immutable；StepID 单次出现 |
| Tool result | `run/tool_call_completed` / `tool_call_answered` / `tool_call_failed` | 无 | 可被 `tool_result_superseded` | 同一 CallID 至多一条 active |
| Summary | `summary` | 无 | 随 compaction 失效 | compaction 的摘要正文 |
| Compaction | `compaction_created` | 无 | invalidated | 指向已有条目的 ledger Position |

**CHT-LIF-1** reducer 拒绝 identity mutation、非法状态迁移、replacement conflict 与重复 ID。模型步骤进行中走 EventSink；定稿即 `ModelStepCompleted` / `ToolCallCompleted` 等 Run 事实本身。同一 Turn 的多个 Run attempt 各自产生 assistant 与 tool_result 条目，全部保留并出现在 ContextFold 的输出里；哪些条目进入模型请求由 PromptBuilder 决定（TRN-RTY-3、DEC-PMT-6），本模块不作取舍。

## 3. parts 与条目

```go
type PartKind string
const (
    PartText      PartKind = "twilight/chatlog/text"
    PartReference PartKind = "twilight/chatlog/reference"
)
type Part interface { PartKind() PartKind }
type TextPart struct { Text string }
type ReferencePart struct { BindingID artifact.BindingID; Name string }

type ToolResultStatus string
const (
    ToolSuccess ToolResultStatus = "success"
    ToolError   ToolResultStatus = "error"
    ToolUnknown ToolResultStatus = "unknown"
)
type ToolResultSource string
const (
    SourceToolOutput   ToolResultSource = "tool_output"   // ToolCallCompleted.OutputDigest
    SourceToolResponse ToolResultSource = "tool_response" // ToolCallAnswered.ResponseDigest
)

type Input struct {
    ID InputID
    TurnID TurnID // delivered 之后赋值；与 turn 模块同一 identity
    Content jsonstable.Value
    Digest es.Digest
}
type Assistant struct {
    ID AssistantID           // = StepID
    TurnID TurnID
    RunID run.RunID
    StepID run.StepID
    FinishReason run.FinishReason
    ResultDigest es.Digest   // = ModelStepCompleted.ResultDigest，冻结 ModelResult 的身份
    CallIDs []CallID         // ToolStepOpened.Calls 的 CallID，与 ModelResult.ToolCalls 同序
    Digest es.Digest
}
type ToolResult struct {
    ID ToolResultID          // = CallID
    TurnID TurnID
    RunID run.RunID
    CallID CallID
    Status ToolResultStatus
    Source ToolResultSource  // success 时非空
    OutputDigest es.Digest   // success 时为冻结输出或响应的身份
    Failure *run.ToolFailure // error / unknown 时为 Run 记录的失败
    Digest es.Digest
}
type Summary struct {
    ID SummaryID
    Parts []Part // Text 与 Reference
    Digest es.Digest
}

type EntryKind string
const (
    EntryInput      EntryKind = "input"
    EntryAssistant  EntryKind = "assistant"
    EntryToolResult EntryKind = "tool_result"
    EntrySummary    EntryKind = "summary"
)
type Entry struct {
    Kind EntryKind
    ID string
    Digest es.Digest
    Input *Input
    Assistant *Assistant
    ToolResult *ToolResult
    Summary *Summary
}
```

**CHT-ENT-1** assistant 条目是 `ModelStepCompleted` 的结构投影：`AssistantID = StepID`，`ResultDigest` 等于事实记录的 digest；同 commit 的 `ToolStepOpened{Source: StepID}` 把 `Calls[i].CallID` 按序补入 `CallIDs`，条目 digest 随之重算。文本、reasoning 与 tool call 参数不在条目里，它们在 `ResultDigest` 命名的冻结 `ModelResult` 中（RUN-WIR-4）。

**CHT-ENT-2** tool_result 条目是 call 终态事实的结构投影：`ToolResultID = CallID`；`ToolCallCompleted` 与 `ToolCallAnswered` 为 `success`，分别以 `tool_output` / `tool_response` 命名冻结正文；`ToolCallFailed` 按 TRN-MAP-4 为 `error` 或 `unknown`，携带事实中的 `Failure`。CallID 在同一 Turn 内唯一（由 `(ModelStepID, index)` 派生，ModelStepID 含 RunID）。active Context 视 `unknown` 为未决，直到 Application 在 Turn 尚未结算时写入 `tool_result_superseded`：替换条目 ID 为 `<CallID>/superseded`，在 Context 中原位取代旧条目（配对不变），Surface 保留旧条目并记录 `Superseded[old] = new`；每个条目至多一个 replacement。Run 事实不受影响。

**CHT-ENT-3** Summary 的 Parts 为单层 TextPart 或 ReferencePart。

**CHT-ENT-4** 用户侧内容是 Input。`input_delivered` 把 Input 挂到 TurnID；Context 将已 delivered 的 Input 作为用户条目。

## 4. canonical wire codec

**CHT-COD-1** Parts 的 wire 是 discriminated union：Decode 先检查 object、discriminator、unknown fields 和 limits，再构造 typed value；payload 版本字段 `v` 由 Registry 处理（EXT-REG-2），本模块 codec 不读写它。有效值满足 `Encode → Decode → Encode` canonical-equivalent。

**CHT-COD-2** 本模块提供 `PartsExtractor`，实现 `extension.BindingExtractor`，按 appearance order 返回 summary 中 ReferencePart 的 BindingID，随 `summary` 的 EventDefinition 声明。`tool_result_superseded` 声明另一提取器：`OutputDigest` 非空时返回 `runmod.FrozenBindingID(OutputDigest)`，替换正文由 Application 经 run 的 `frozen.Store` 以 `tool_output` 信封存入，与 Run 事实命名的正文走同一 Binding 派生与 claim（RUN-WIR-4）。

**CHT-COD-3** EventType 为 `twilight/chatlog/<name>`。事件 payload 的 Digest domain 与 EventType 相同；投影条目的 Digest domain 为 `twilight/chatlog/assistant` 与 `twilight/chatlog/tool_result`，覆盖条目的全部结构字段（ID、TurnID、RunID、StepID/CallID、FinishReason/Status/Source、ResultDigest/OutputDigest、CallIDs/Failure），不覆盖 `v`：

```text
Digest("twilight/chatlog/input_submitted", ...)
Digest("twilight/chatlog/assistant", ...)     // 条目，非事件
Digest("twilight/chatlog/tool_result", ...)   // 条目，非事件
Digest("twilight/chatlog/summary", ...)
```

## 5. event payloads

payload 为 object，identity 为 string，整数按 Session preset 编码。未列字段在 v1 拒绝。

```go
type InputSubmittedPayload struct {
    InputID InputID
    Content jsonstable.Value
    SubmittedAtUnixMilli int64
}
type InputDeliveredPayload struct { InputID InputID; TurnID TurnID }
type InputWithdrawnPayload struct { InputID InputID; Reason string }
type InputRejectedPayload struct { InputID InputID; Reason string }

type ToolResultSupersededPayload struct {
    ToolResultID ToolResultID
    Status ToolResultStatus   // success | error
    OutputDigest es.Digest    // success 时必填，冻结 tool_output 的身份
    Reason string
}

type SummaryPayload struct { Summary Summary }

type CompactionCreatedPayload struct {
    CompactionID CompactionID
    CoveredThrough session.Position // 最后一条被覆盖条目的事件在 ledger 中的位置（commit seq + 组内下标）
    BaseContextDigest es.Digest
    SummaryID SummaryID
    SummaryDigest es.Digest
    Retained []EntryDigestPair
    Digest es.Digest
}
type EntryDigestPair struct { Kind EntryKind; ID string; Digest es.Digest }
type CompactionInvalidatedPayload struct { CompactionID CompactionID; Reason string }
```

**CHT-EVT-1** EventType：

```text
twilight/chatlog/input_submitted
twilight/chatlog/input_delivered
twilight/chatlog/input_withdrawn
twilight/chatlog/input_rejected
twilight/chatlog/tool_result_superseded
twilight/chatlog/summary
twilight/chatlog/compaction_created
twilight/chatlog/compaction_invalidated
```

**CHT-EVT-2** `input_submitted` 创建 Input。Delivered、Withdrawn、Rejected 各终结一次。`input_delivered` 要求 Input 仍为 submitted，并写入非空 TurnID；它与把该输入交给 Run 的事实同组：Start group 中与 `twilight/turn/started` 一起，回合中途与 `twilight/run/input_accepted` 一起（TRN-STR-2、TRN-DLV-2）。SummaryID 在 chatlog 流内单次创建。

**CHT-EVT-3**（compaction）compaction 压缩 active Context：合法 compaction 使其变为 `[Summary] + Retained`，其后的事件照常折叠。digest 规则：`Digest` 的 domain 为 `twilight/chatlog/compaction_created`，覆盖除 `Digest` 外的全部字段；`BaseContextDigest` 以同一 domain 对 `{base: [(Kind, ID, Digest)]}` 计算，覆盖截至 `CoveredThrough` 的有序 active Context 序列；`Retained` 为空与省略是同一 wire 值，两个 digest 预映像都把空列表折叠为 nil。summary 应与 compaction 同组提交，gap 不变量因此原子成立。

fold 在提交前逐条校验（EXT-WRT-1 的投影预折叠），违反者整组拒绝：`CoveredThrough` 早于 compaction 事件自己的 Position；`CoveredThrough` 与 compaction 之间的 Context 条目恰为该 `SummaryID` 的 summary 且 digest 相符；`BaseContextDigest` 与 base 序列重算值相符；`Retained` 是 base 序列的有序子集（逐项 (Kind, ID, Digest) 全等）。retained 集的 provider 合法性（tool call 与 result 的配对封闭，按 `Assistant.CallIDs` 判定）是宿主的职责（APP-CKP-2），fold 不校验。

`compaction_invalidated` 只能指向最近一个仍 active 的 compaction：active Context 回到 base 加 compaction 之后折叠的尾部，summary 条目随之离开 active Context（Surface 与历史保留）；连续 invalidate 逐层回退。指向被压缩条目的 `tool_result_superseded` 是协议违规而非 compaction 失效条件：被压缩条目的 Turn 已结束，CHT-ENT-2 已排除对它的 supersede。失效途径只有显式 invalidate 最近的 active compaction。

## 6. Surface projection

```go
type SurfaceEntry struct { Kind EntryKind; ID string; Position session.Position }
type Surface struct {
    Inputs Table[InputID, InputView]
    Assistants Table[AssistantID, Assistant]
    ToolResults Table[ToolResultID, ToolResult]
    Summaries Table[SummaryID, Summary]
    EntryOrder []SurfaceEntry
    Superseded Table[ToolResultID, ToolResultID]
    Compactions Table[CompactionID, CompactionView]
    Runs Table[run.RunID, TurnID] // attempt/started 的 TurnID
}
// Table 是持久化 map：Set 返回新值、不改写接收者，各状态共享未变的存储；编码为普通 JSON object。
```

Surface 的折叠遵守 EXT-PRJ-1：Apply 不改写传入状态。内容表用 `Table` 承载，一次写入的代价为 O(√n)，因此折叠一条 N 行日志的代价随 N 线性增长；`EntryOrder` 以 append 增长。

**CHT-SUR-1** SurfaceFold 消费本模块事件与 CHT-SCP-1 列出的 run 事实，跨 chatlog 流与 run/&lt;RunID&gt; 流折叠（EXT-PRJ-1），其他事件按 EXT-PRJ-2 处理。`EntryOrder` 为 commit 顺序下的 delivered input、assistant、tool_result、summary，并带产生它的事件的 ledger Position（`extension.DecodedEvent.Position`，由 fold 盖上）：投影不维护自己的计数器，快照恢复也不需要重扫。`Surface.Runs` 与 `Context.Runs` 在 `run_ended` 时释放该 Run 的条目，大小与活动 Run 数成正比。回合列表由 turn 投影提供，按 `TurnID` 连接。compaction 记录于 `Surface.Compactions`（active / invalidated）；compaction 不改动 `EntryOrder`，也不触及输入队列。

## 7. Context projection

```go
func ContextFold(events []extension.DecodedEvent) ([]Entry, error)
```

**CHT-CTX-1** 输入为已验证、按 commit 顺序的 chatlog 与 run decoded events，其他事件按 EXT-PRJ-2 处理。输出为 delivered input、assistant、tool_result、summary 经 supersession 与 compaction 处理后的有序 `[]Entry`。ContextFold 为纯函数：不读取 ContentStore，正文缺失不影响折叠。

**CHT-CTX-2** fold 执行 ID 单次创建、`ToolStepOpened` 对 assistant 的 CallIDs 补入、原位 replacement 规则。合法 compaction 按 CHT-EVT-3 应用。Context 只含已 delivered 的 Input。

## 8. materializer

```go
type ContentResolver interface {
    ModelResult(context.Context, es.Digest) (model.ModelResult, error)
    ToolOutput(context.Context, es.Digest) (run.CanonicalJSON, error)
    ToolResponse(context.Context, es.Digest) (run.CanonicalJSON, error)
}
type Call struct { CallID CallID; ProviderCallID string; Name string; Input run.CanonicalJSON }
type Materialized struct {
    Entry *Entry
    Result *model.ModelResult    // assistant
    Calls []Call               // CallIDs 与 Result.ToolCalls 逐位配对
    Output *run.CanonicalJSON  // success 的 tool_result
}
func NewMaterializer(ContentResolver) *Materializer
func (*Materializer) Entries(context.Context, []Entry) ([]Materialized, error)
func (*Materializer) Entry(context.Context, *Entry) (Materialized, error)
```

**CHT-MAT-1** materializer 是投影与表示之间的 IO 边界：它按 digest 读取冻结正文（`agentcore/session/run.Content` 为 first-party 实现），同一 Materializer 内每个 digest 至多读取一次；投影从不调用它。正文缺失返回 `frozen.ErrMissing`，投影与 ledger 不受影响。`Calls` 由 `Assistant.CallIDs` 与 `ModelResult.ToolCalls` 逐位配对，CallIDs 为空时按该 Run 版本的 `Schema.Identity.DeriveCallID(StepID, i)` 派生。provider capability 与发送策略由 Application 决定；PromptBuilder 组装见 [Decision](agent-decision.md)（DEC-PMT）。

## 9. conformance

- **CHT-LIF-1、CHT-EVT-1、CHT-EVT-2**：所列 EventType、Input 终结一次、delivered 带 TurnID、ID 单次创建；
- **CHT-ENT-1 至 CHT-ENT-4**：run 事实折叠为条目、CallIDs 补入、原位 replacement、重复 supersede 与范围外 supersede 被拒、用户侧为 Input；
- **CHT-COD-1 至 CHT-COD-3**：codec；条目 Digest domain；supersede 的 Binding 提取；
- **CHT-SUR-1、CHT-CTX-1、CHT-CTX-2**：EntryOrder；compaction 的折叠、显式失效回退与非法 compaction 的整组拒绝（含排队输入不受压缩影响）；
- **CHT-MAT-1**：配对、单次读取、失败正文的渲染、正文缺失的错误。
