# Twilight Agent Run Protocol

状态：v1 设计规范。本文定义 Run Machine、RunStore 与 Loop。`agentcore/run` 及其子包只说 Run 自己的类型；Session 侧由 `agentcore/session/run` 的 `SessionRunStore` 把 `runtime.RunStore` 绑定到 `writer.Writer` 上写入；Session/Run 语义不使用 per-effect lease，Executor Worker 的 ownership 由 Execution Store 的租约与 fencing epoch 管理。`RecoverInterrupted` 负责无法关联或明确放弃的语义接管处置。本文依据 [agent-session.md](agent-session.md)（Session 级单写者、一行一个 event）与 [agent-session-extension.md](agent-session-extension.md)（`writer.Writer`）。

本文定义 `agentcore/run` 及其子包、`agentcore/run/loop` 与 Run 作为 Session Module 的存储形态。文中的"必须""不得""应该"是协议约束；canonical JSON、JCS 与 domain-separated digest 使用 `agentcore/jsonstable` 和 `agentcore/es` 的通则。

## 1. 范围与 authority

```text
Session ledger               唯一 authority：twilight/run/ 事实写入 run/<RunID> 流，与 turn、chatlog 的流同在一条 ledger，可在同一 Commit 内落下
MachineState                 Run 的语义状态投影（twilight/run/machine）；投影缓存为可丢弃的派生缓存
RunStore                     Run 的事务端口（agentcore/run/runtime）：已绑定调用方写能力的 Load / Commit / FrozenRequest，只说 Run 自己的类型
SessionRunStore              agentcore/session/run 的适配器：Bind(w) 实现 RunStore，Record 按 SessionID 读，Command / CreateRun 是 unit of work 里的 Run Part
loop.Loop                    当前进程的 execution interpreter
frozen.Store                 Owner 侧的不可变正文存储（agentcore/run/frozen）：模型请求、模型结果、工具输出与外部响应，按 digest 寻址；请求在 Dispatch 时形成 executor-owned payload
```

`MachineState` 决定 Run 当前可执行动作。每次接受的 command 产生一组同 CommitID 的 Session event，其中的 `twilight/run/` 事件经该 Run 版本的 `schema.Machine.Evolve` 从 `twilight/run/run_created` 重放后必须得到同一 `MachineState`。

Run 的职责分成五个相互独立的层面：

```text
Agent Machine   = Run/Step 状态与合法转移（Decide、Evolve）与执行规划（plan.Next）
Agent Loop      = 决策解释器：把 plan.Next 的 action 记录为事实、把 start 请求的 effect 交给 Executor 为 Assignment、把 Outcome 结算为事实
RunStore        = command 到事实组的原子提交端口（agentcore/run/runtime）；SessionRunStore 是它的 Session 适配器，接管处置在 agentcore/run/reconcile
Executor        = 效果层端口：执行 Assignment（一次模型请求或一次工具调用）并交回 Outcome
Prompt Builder  = 从 Session context 构造下一条 prompt（决策层，agent-decision.md）
```

Machine 处理已冻结的值和已提交的事实；Loop 解释 `plan.Next` 派生的 transient action，只做决策与记录，从不在一次推进里等待效果；RunStore 保存并验证 Machine 的推进；Executor 执行外部 effect 并以数据形式交回结果，它与 Loop 是否同进程是部署选择（第 6 节 RUN-EXE）；Prompt Builder 构造下一条 prompt。

`Step` 是 Run 的持久化恢复边界，描述逻辑进度（ModelStep、ToolStep）。`effect` 是 Run 对外部世界的一次请求：一个 ModelStep 的模型调用，或一个 tool call 的工具调用；其身份 `EffectID = Digest("twilight/effect", RunID, StepID, CallID, sequence)`，model effect 的 CallID 为空、sequence 为该 step 已记录的 `Rejects`（Retry 使同一 StepID 回到 Prepared 并请求下一个 effect），tool effect 的 sequence 为 0（一个 call 最多 start 一次）。effect 的 kind 与 binding 由请求者决定（ModelStep 的 `RequestDigest`、call 的冻结 `ToolCallBinding`），Run 不在 step 之外另存 effect 记录。`attempt` 是 Executor 对一个 effect 的一次物理执行：Execution Record、worker、lease、`ExecutionRef` 都属于 attempt，Run 不记录它们，只经 EffectID 与 Executor 相连。`Wait` 是 tool call 尚缺失的外部输入（`ResponseRequest`：approval 或 external response），Waiting 的 call 不请求 effect。Session Writer 负责 Run 语义事实的 ownership；Executor Worker 可以在另一个 control-plane ownership 下执行同一个 Assignment。start 事实记录 effect（`ModelStepStarted.Effect`、`ToolCallStarted.Effect`），接管者以 EffectID 向 Executor 询问该 effect 的 attempt 是否仍存在（RUN-CMT-7）。

**RUN-SCP-1** `agentcore/run` 只定义一个 Run 的 identity、事实、状态、命令与合法状态转移（`MachineState`、`Command`、`Fact`、`Decide`、`Evolve`）。子包分为两层。协议层的公开类型随 Run 的 schema 一起变更，或是 Run 核心定义的端口：`model`（冻结的模型请求、模型结果、消息、用量与工具定义的数据模型，它们是 canonical digest 的预映像；`model/sdkconv` 是与 `sdk` 类型互转的唯一位置）、`canonical`（digest 规则与 identity 派生）、`wire`（command/fact 编解码、变体注册表与 `MachineState` codec）、`frozen`（冻结正文的信封编解码与 `Store` 端口）、`schema`（Machine、Wire、Canonical、Snapshot、Identity、Bodies 六个契约的无版本单值）、`plan`（`Next` 执行规划、`WaitingCalls` / `NeedsRecovery` 查询与接管处置 `RecoveryTargets` / `RecoveryCommands`）、`runtime`（`RunStore` 端口、`Snapshot`、`CommitRequest` / `CommitResult`、`EvaluateCommit` 与 `FoldRun`）。除 `model/sdkconv` 外，协议层只依赖 `agentcore/es`、`agentcore/jsonstable` 与彼此，`sdk` 在协议层只出现在 `model/sdkconv`；协议层不依赖 `agentcore/session`、`writer`、loop、turn 或 extension：store 身份是不透明的 `Scope`，位置是 `RunPosition`，envelope 不命名 store。执行层使用协议层与 `sdk`，Run 核心与协议层不引用执行层：`effect`（Loop 与 Executor 之间与进程无关的端口合同；`ModelAssignment` 携带冻结的 `model.ModelRequest`，`ModelSucceeded` 以 `sdk.ModelResult` 交回模型结果，由 Loop 经 `model/sdkconv` 冻结后提交）、`loop`（只依赖 `runtime.RunStore`，拥有 prompt builder/model/tool ports、streaming、并发执行、EventSink 与 Loop policy）、`reconcile`（比较 Run 机器的 Executing 目标与 execution store 的记录，产生 Run command）。effect 端口的传输编码 `agentcore/executor/protocol` 属于 Executor，只被 Executor 及其 store / http 适配器导入。`agentcore/session/run` 是 Run 的 Session Module 实现：EventDefinition（每个事实类型一个 payload 版本的 codec，wire 类型来自 `wire.FactTypes()`）、`twilight/run/machine` projection、`SessionRunStore`（`Bind(w)` 实现 `runtime.RunStore`，`Command` / `CreateRun` 是 unit of work 的 Part）、Session 级接管入口、`frozen.Store` adapter。

**RUN-SCP-2** Run 是 first-party Session Module（Source `twilight`，ModuleID `run`）。Run 不知道它的上层实体：哪个 Turn 的第几次 attempt 由这个 Run 执行，是 attempt 模块的事实（ATT-1），Run 事实不携带它。本模块的 `Requires`（EXT-REG-4）为空。Run 不写任何其他模块的事件：Turn 结算与对话内容是 Run 事实的投影，由 [agent-turn.md](agent-turn.md) 与 [agent-session-chatlog.md](agent-session-chatlog.md) 定义；ledger、所有权、组追加与投影机制由 [agent-session.md](agent-session.md) 与 [agent-session-extension.md](agent-session-extension.md) 定义。Artifact、queue、provider registry、权限与产品 policy 分别由其 package 或 Application 拥有。

## 2. identity、persisted values 与 wire

```go
type RunID string
type StepID string
type CallID string
type CommandID string
type ResponseID string
type InputID string
type ToolRef string
type ModelRef string
type PromptToken string
type EffectID string
type Digest = es.Digest
```

**RUN-WIR-1** identity 必须非空、稳定且为有效 UTF-8。`EffectID` 由 `DeriveEffectID(RunID, StepID, CallID, sequence)` 派生（namespace `twilight/effect`）；一个 effect 的 start、settlement 与 recovery CommandID 只以 EffectID 为 preimage（namespace `twilight/start-command`、`twilight/settlement-command`、`twilight/recovery-command`），因此同一 effect 的 start 重试与结算重放得到同一 CommandID，接管处置的 CommandID 与 owner、Epoch 无关。start 事实记录 Effect（`ModelStepStarted`、`ToolCallStarted`），Executing 的 step 与 call 在 MachineState 中携带它：这是接管者向 Executor 询问"该 effect 的 attempt 是否仍在执行"并接受其迟到 Outcome 所需的唯一身份。start、settlement 与 recovery command 都携带 Effect：缺少 Effect 时无法派生 CommandID，Commit 返回 `ErrCommandConflict`；Effect 与 Decide 按当前状态派生或记录的值不符时返回 `ErrStaleRuntime`。Pending call 在 start 之前的失败没有 effect，以 `DeclineToolCall` 提交，其 CommandID 以 RunID、StepID、CallID 为 preimage（namespace `twilight/decline-command`）。Run 跨 domain causation 记录在 `twilight/run/run_created` 的 `CausationID`。 start 事实与 settlement 事实都记录 Effect：`ModelStepStarted.Effect`、`ToolCallStarted.Effect` 记录请求的 effect，`ModelStepCompleted`、`ModelStepRecovered`、`ModelStepRejected`、`ModelStepFailed`、`ToolCallCompleted`、`ToolCallFailed` 的 `Effect` 记录该结算关闭的 effect，Evolve 校验它等于目标正在执行的 effect；`RunEnded` 不结算 effect，Evolve 拒绝在仍有 Executing 目标时折叠它，因此每个请求过的 effect 都恰由一条指认它的事实关闭；从未启动的 call 的失败（decline、被拒的 response、取消时的 Pending call）`Effect` 为空。一条 settlement 事实因此自足地指认它回答的 execution，读者不必回到 start 事实按坐标匹配。

Run 持久化协议保存 run-owned frozen values。模型请求、模型结果、消息、工具定义、usage 在进入 command 前，分别经 `FreezeModelRequest`、`FreezeModelResult`、`FreezeToolDefinition`、`FreezeToolArguments` 等入口转为 `agentcore/run/model` 的闭合值类型：JSON 文档（工具参数、工具输出、schema、provider options）冻结为 immutable `CanonicalJSON`；provider metadata 在 SDK 侧已是 namespace→name→string 的字符串 token，镜像保持同一形状，不经 JSON 转换；无法成为 JSON 的工具参数文本按原文保存在 `ToolArguments.Text`（非法 UTF-8 拒绝冻结）。RunStore 接收 agent-owned value；调用方负责在边界前完成冻结。

**RUN-WIR-2** Run 事实是 Session event：EventType 为 `twilight/run/<name>`，payload 为 canonical JSON object，第一层携带 `runId` 与 payload 版本字段 `v`（SES-VER-1、EXT-REG-2）。`v` 是该事实类型的 payload 版本，属于事件类型而不属于 Run：`runmod` 为每个事实类型登记一份 codec 历史（`EventDefinition.Codecs` 按版本各一个 codec，`Version` 为当前版本），当前版本由 `factCodec` 按 Run 核心今天定义的 wire 编解码，被取代的版本各保留自己的 codec（`olderFactCodecs`，版本 1..n-1 连续，缺口在构建时报错），解码旧 wire 并 upcast 到当前内存类型，Decide 与 Evolve 只面对当前类型（RUN-CMT-8）。某个事实类型形状变化时，把被替换的 codec 以其旧版本号加入历史，当前版本自然加一；已发布版本的 codec 永不删除、永不修改。当前所有事实类型均为版本 1、无历史。行字段（Seq、CommitID、Index、Last、digest）由 Session kernel 提供，Run 不另设 envelope。fact codec 必须拒绝 unknown type、duplicate key、unknown field、trailing data、非法 UTF-8、非 canonical-equivalent wire。精确 identity 和 digest 使用 JSON string，整数字段使用 Session preset 的整数 wire shape。

```go
type CommandEnvelope struct {
    Type string
    RunID RunID          // envelope 不命名 store：command 到达哪个 Session 由绑定的 RunStore 决定
    ID CommandID
    Command AgentCommand
}
```

command 不持久化。`CommandEnvelope.ID` 就是该 command 产生的 event 组的 `CommitID`；重放由 Writer 按 CommitID 判定（EXT-WRT-2）。envelope 只经 `Schema.Wire.Envelope` 构造（RUN-WIR-3），不携带自校验 digest。

**RUN-WIR-3** 一个 command 恰产生一组事件（一次 `Append`，同一 CommitID）；其 `twilight/run/` 事件在 Run 自己的 stream 内 Index 从 0 连续递增。同一语义操作里其他模块的事实（input_delivered、turn/failed）不由 Run 附带：它们是同一个 unit of work（`agentcore/session/unit`）里那个模块自己的 Part，与 Run 的 Part 在同一 View 上准备、同一 commit 落盘。事件没有独立 EventID，`Seq` 即身份（SES-WIR-1）。RunStore 提交的组其 CommitID 等于 CommandID，Coordinator 写入的 Start 与 Retry 组使用该组自己的 CommitID。`RecordedAtUnixMilli` 由写入方的时钟填入，是 metadata，不参与 Run 的任何派生，也不参与重放判定（EXT-WRT-2）。构造 command 必须使用 `schema.Wire.Envelope`；`agentcore/run` 自身不提供 `Envelope`、`Decide`、`Evolve` 或 `Digest*` 的包级函数，六个契约只在 `agentcore/run/schema` 以单值暴露（RUN-CMT-8）。

**RUN-WIR-4** 内容与执行状态分离。fact 只保存执行状态与内容 digest；用户输入也不例外：`AgentInput{ID, Digest}`，`input_accepted` 与 `PendingInputs` 只记录输入身份与其内容 digest，正文只在 chatlog 的 `input_submitted` 里存在一份，chatlog 的 `DeliverInputs` Part 在同一 unit 里核对 digest；digest 是不可变正文在 `frozen.Store` 中的 canonical 身份，命名它的 fact 是正文的 retention root。fact 不携带 `artifact.Ref`：Scheme、Authority、MediaType、Durability 属于存储层，由 `agentcore/session/run` 的适配器从 digest 确定性派生（`FrozenRef`、`FrozenBinding`），Run 协议只知道 digest。

| 内容 | fact 中的字段 | 正文信封 |
|---|---|---|
| 冻结模型请求 `ModelRequest`（含工具定义） | `ModelStepPrepared.RequestDigest` | `model_request`；Dispatch 后由 Execution Ledger 的 `execution_accepted` 另持有 payload |
| 工具定义 `ToolDefinition` | `ToolSpec.DefinitionDigest`，只用于执行前校验 | 请求本体内；不另设存储 |
| 模型结果 `ModelResult`（文本、reasoning、tool call 列表、usage、provider metadata） | `ModelStepCompleted.ResultDigest` | `model_result` |
| 工具输出 | `ToolCallCompleted.OutputDigest` | `tool_output` |
| 外部响应 | `ToolCallAnswered.ResponseDigest` | `tool_response_payload` |
| tool call 参数 | `ToolCallBinding.Arguments` | fact 本身 |

正文以其 digest 的预映像信封存储（`frozen.Codec` 的 `Encode*`），因此 `sha256(bytes) == digest`，cas Key 即 digest。Run 的 `Command` Part 在构造时（进入 Writer 之前）存入命令携带的正文，`FrozenValues.Put` 同时登记 Binding（RUN-CMT-3 第 0 步）；命名正文的四种 fact 在 EventDefinition 上声明该 Binding 的提取，Writer 在 `Append` 之前 admission 并建立 claim（EXT-REF-2、EXT-WRT-3），正文的保留期与 ledger 中的 fact 一致。对话与 Turn 的投影只保存 digest，正文在读取时经 materializer 取回（CHT-MAT-1）；因此 `run/<RunID>` 流是 canonical history，不能独立回收，正文可以迁移到冷存储但不得丢弃。`frozen.Store` 是 run 层对内容寻址存储的端口：`Put(digest, bytes)` 幂等，`Get(digest)`。它不是第二个内容寻址存储，而是 artifact `cas` ContentStore 的一个 Authority（`twilight/run/frozen`，`agentcore/session/run.FrozenValues` 适配）。Dispatch 时必须把请求复制到 executor-owned 的 durable Execution Record，或复制到 Worker 可访问的 payload store。Worker 不应在执行时反查 Authority 的 Session 或依赖某个 Worker 的本地文件。接管时继续使用相同 AssignmentKey；是否重试由 control plane 和 effect recovery policy 决定。内存与文件两种 ContentStore（`artifact.NewMemoryContentStore`、`filestore.NewContentStore`）经同一适配器服务。工具列表摘要（`DigestToolSpecs`）的预映像不区分 nil 与空列表：fact wire 省略空列表，重算方拿到的是 nil。

下列 identity 稳定派生并由 Commit 验证：

| identity | preimage |
|---|---|
| PrepareModelRequest CommandID | RunID、loaded `RunPosition`（该 RunID 最后一条事件的 Seq） |
| ModelStep StepID | RunID、prepare CommandID |
| ToolStep StepID | source ModelStepID（一个 ModelStep 只完成一次，至多打开一个 ToolStep） |
| CallID | source ModelStepID、该 call 在模型结果 `ToolCalls` 中的位置 |
| ResponseID | RunID、ToolStepID、CallID、ResponseKind |
| response CommandID | RunID、StepID、CallID、ResponseID |
| input CommandID | RunID、有序 InputID 列表（批次） |
| withdraw CommandID（WithdrawPreparedStep） | RunID、StepID |
| EffectID | RunID、StepID、CallID（model 为空）、sequence（model 为该 step 已记录的 Rejects，tool 为 0） |
| start CommandID（StartModelExecution / StartToolCall） | EffectID |
| settlement CommandID（model result/failure/reject、tool result/failure） | EffectID |
| recovery CommandID（RecoverModelExecution、接管处置的 tool Unknown） | EffectID |
| decline CommandID（DeclineToolCall） | RunID、StepID、CallID |

派生 identity 使同 CommandID 即同一 command：内容差异只可能出现在 identity 有意不覆盖内容的两族（同一 ResponseID 的 approve 与 reject、同一 effect 的两次结算），RunStore 对它们按精确重放处理，调用方从投影读取实际生效的结果。`PromptToken` 是 Application-owned opaque freshness token，属于 prepare command identity 内容；Run 不校验它的语义（RUN-CMT-4）。

## 3. 创建与 canonical record

```go
type NewRun struct {
    RunID RunID
    CausationID es.CausationID
}
type RunCreated struct {
    RunID RunID
    CausationID es.CausationID
}
type RunRecord struct {
    Created session.StreamSeq
    Snapshot runtime.Snapshot
    Events []session.Event // 该 RunID 的 run/<RunID> 流的全部事件，按 StreamSeq 顺序
}
```

**RUN-NEW-1** `twilight/run/run_created` 是 Run 的第一个事实。初始状态恰为：相同 RunID、`RunActive`、`Current=Open`、无 pending input、零 model step、零 usage、无 result。Run 不记录它为哪个 Turn 的第几次 attempt 服务：那是 Turn 与 Run 之间的绑定，由 attempt 模块的 `twilight/attempt/started` 在同一 commit 记录（ATT-1/2），Run 只是一次执行。初始输入随后以 `twilight/run/input_accepted` 进入同一组（TRN-STR-2）。`agentcore/session/run` 的 `CreateRun` Part 由 `schema.Machine.CreateGroup(NewRun, []AgentInput)` 返回 `created` 与 `input_accepted` 的 facts，编码为 Session event 由 `agentcore/session/run` 完成，Coordinator 不自行编码。同一 RunID 第二条 `created` 为 Evolve 错误。

**RUN-NEW-2** `runtime.FoldRun(schemaVersion, facts)` 按 Seq 顺序折叠该 RunID 的完整事实序列，第一条必须是 `created`；`schemaVersion` 取自这些事实的 `v`，据此绑定 `Schema`。Fold 过程执行纯状态重建。import、诊断与 `SessionRunStore.Record` integrity verification 都经 FoldRun；投影缓存通过 FoldRun 结果校验。

## 4. Machine

```go
type Current interface{ current() }
type Open struct{}
func (Open) current() {}
func (ModelStep) current() {}
func (ToolStep) current() {}

type MachineState struct {
    RunID RunID
    Status RunStatus
    Current Current
    PendingInputs []AgentInput
    ModelSteps int
    LastToolStep *ToolStep
    Usage Usage
    Result *RunResult
}

type RunResult struct {
    Status  RunStatus
    Reason  RunReason
    Failure *RunFailure
    UncertainCalls []CallID
    UncertainModel StepID
    Usage   Usage
}

type ModelStepStatus uint8 // Prepared | Executing
type ModelStep struct {
    RefValue StepRef
    RequestDigest Digest // Owner 侧 frozen payload；Dispatch 后由 Execution Ledger 持有
    Model ModelRef
    Tools []ToolSpec
    Status ModelStepStatus
    Rejects int // 已接受的 ModelStepRejected 次数
}
type ToolSpec struct {
    Ref ToolRef
    DefinitionDigest Digest // 本体在请求内
    Policy ResponsePolicy
    Replay ReplayPolicy     // 工具实现的 replay 声明，随 PublicTool 进入 preset 摘要；复制到 ToolCallBinding / ToolCallState / ToolAssignment（RUN-EXE-9）
    Placement ToolPlacement // 工具实现的 placement 声明，随 PublicTool 进入 preset 摘要；复制到 ToolCallBinding / ToolCallState / ToolAssignment；解析器与 Worker 路由据此判断
}
type ReplayPolicy uint8 // ReplayUnknown（零值，未判断，wire 上省略）| ReplayAllowed（只读或按 CallID 幂等）| ReplayForbidden（有不可重复的副作用）；工具级能力
type ToolPlacement uint8 // PlacementProcess（零值，在执行它的进程内运行，wire 上省略）| PlacementWorkspace（必须在 Session 绑定的 workspace 内运行，Assignment 须带 target）
type RetryDisposition uint8 // RetryUnknown（零值，不重试）| RetryNever | RetryAllowed；一次具体 Known 失败自己的属性：Allowed 表示该失败足以确认本次 attempt 没有产生不能安全重复的外部效果（RUN-EXE-11）
// ToolFailure.Class 的工具执行失败类别（RUN-EXE-11）：not_found | invalid_input | timeout | unavailable | rate_limited | conflict | internal；execution_failed 为未分类。类别描述错误，可重试性由该失败的 RetryDisposition 单独给出
type ToolScheduleMode string // "parallel" | "sequential"；空值按 parallel 解释
type ToolScheduling struct {
    Mode ToolScheduleMode
    MaxParallel int // 0 表示当前 Start 批次全部 Pending call 可并行
}
type ToolStep struct {
    RefValue StepRef
    Source StepID
    Calls []ToolCallState
    Scheduling ToolScheduling
}
```

`Status` 是 Run 的生命周期：`RunActive | RunCompleted | RunStopped | RunFailed`。后三者是终态。`RunStatus` 表示当前 MachineState 的投影；终态 fact 使用 RunEnd union 表达具体结果。

`Current` 是 Active 期间的内容。`Open` 是规划区间：可提交 `PrepareModelRequest`，`Next` 返回 `NeedModelRequest`。`ModelStep` 与 `ToolStep` 表示正在进行的步骤。`AcceptInput` 在任意非终态都被接受，只把输入追加到 `PendingInputs`；`PendingInputs` 是回合中途追加输入的持久化队列，在下一次 Prepare 时被一次消费。终态的 `Current` 为空；终态由 `Status` 表达，不另设 Current variant。Active 的 `Current` 不得为空。`Step` 仍只有 `ModelStep` 与 `ToolStep`，提供 `Ref()`。

MachineState 不保存模型输出与工具输出本体。上一步的内容由 PromptBuilder 从 chatlog fold 读取（DEC-PMT），MachineState 只提供 `LastToolStep` 作为 Run 边界事实。

终态 fact 使用 Go 的 sealed-union 形式，终态结构由合法的 RunEnd variant 构成：

```go
type RunEnd interface{ runEnd() }

type RunCompletedEnd struct{}
type RunStoppedEnd struct {
    Reason RunReason
    UncertainCalls []CallID
    UncertainModel StepID
}
type RunFailedEnd struct {
    Reason  RunReason
    Failure RunFailure
}

func (RunCompletedEnd) runEnd() {}
func (RunStoppedEnd) runEnd() {}
func (RunFailedEnd) runEnd() {}

type RunEnded struct { End RunEnd }
```

`RunEnded.End` 必须恰好是上述三个 variant 之一；`RunStoppedEnd.Reason` 必须非空，`RunFailedEnd.Reason` 必须是失败原因，`RunFailedEnd.Failure.Class` 必须非空。`RunEnded` 是 terminal 组中最后一个 `twilight/run/` 事实。`RunEnded` 自身不结算任何 effect：Evolve 拒绝在 Current 仍有 Executing 的 ModelStep 或 tool call 时折叠 `RunEnded`，结束一个 effect 的只能是指认它的 settlement 事实（`ModelStepFailed`、`ModelStepRejected`、`ToolCallFailed` 等，RUN-WIR-1）。RunStatus、RunResult 等读取模型从该 union 派生。v1 wire 是 tagged union：`{"completed":{}}`、`{"stopped":{reason, uncertainCalls?, uncertainModel?}}` 或 `{"failed":{reason, failure}}`，恰有一个 variant key；codec 拒绝零个或多个 variant、缺失字段与多余字段。Cancel 时仍 Executing 的 tool call 与 model step 必须写入 `RunStoppedEnd` 并投影到 `RunResult`。

```text
ModelStep: Prepared -> Executing -> Completed
             |           |             |
             |           +-> Recovered -> Open  (该 effect 的 attempt 已不存在：撤回该请求，下一次 Prepare 重新规划)
             |           +-> Rejected     (retry 回到 Prepared，或同组失败 Run)
             |           +-> Failed -> Open     (该 effect 以最终失败结算；同组随后 RunEnded(failed)，Cancel 时 class 为 effect_unknown、随后 RunEnded(stopped))
             +-> Withdrawn -> Open        (Prepared 期间有 pending input，放弃该请求并重规划)

ToolCall:
  Pending -> Executing -> Completed
     |          |
     |          +-> Failed(Known|Unknown)
     +-> Failed(Known)
  Waiting(Approval)         -> Pending | Failed(Known)
  Waiting(ExternalResponse) -> Completed | Failed(Known)
```

同一 Assignment 的恢复不重新生成 ModelRequest；Recovered 与 Withdrawn 都使 Run 回到 `Open`、不计入 `ModelSteps`，下一次 `Prepare` 按当时的投影与 `PendingInputs` 重新规划，产生新的 StepID 与 RequestDigest：恢复记录为一次新的 Prepared 事实，StepID 与 RequestDigest 均为新值（TRN-DUR-1）。Prepared 期间到达的输入使该请求不再完整，`Next` 改为返回 `WithdrawPrepared`，Loop 提交 `WithdrawPreparedStep` 后回到 `Open` 重规划；Executing 期间到达的输入等待该步结算或撤回，在随后的 `Open` 被消费。

**RUN-MCH-1** MachineState 保存 Run 的 execution semantics，是其事实沿四个维度的折叠：Progress（`Current`、`ModelSteps`、`LastToolStep`：Run 处于哪个 Step、冻结了什么）、Inbox（`PendingInputs`：已接受、尚未被 prepare 消费的输入）、Effects（Executing 的 ModelStep 或 call 请求的 `Effect`，与 Waiting call 持有的 `Waiting`）、End（`Status`、`Result`）。控制面元数据（哪个 worker 在执行某个 effect、lease、epoch、backend 句柄）属于 Executor 的 attempt 记录，Session 的 owner fence 与队列 claim 属于宿主，都不进入 MachineState。`Usage` 与 `Result` 是为读取方折叠的投影：Usage 累计每条模型事实报告的用量，Result 复述 `RunEnded` 并附带该 Usage；Decide 不读取它们，Run 结束的 authority 是 `RunEnded` 事实。`LastToolStep` 保存最近一个经 Evolve 关闭路径写下的 ToolStep 只读投影，必须与事件序列折叠出的最后关闭 step 一致，供下一次 prompt builder 定位 `SourceStep`。Cancel 经 `RunEnded` 把 `Current` 置空、不走关闭路径时不改写 `LastToolStep`。terminal state 吸收所有未幂等命令；`RunEnded` 建立唯一 terminal result。

**RUN-MCH-2** `ToolCallBinding` 冻结 CallID、ProviderCallID、ToolRef、definition digest、canonical arguments 与 response policy。canonical arguments 是 `ToolArguments.Canonical()`：模型给出 JSON 文档时为该文档，零值为空对象，不是 JSON 的参数文本以 JSON 字符串绑定，使 fact 保留模型实际写出的内容，并在校验阶段以 invalid_arguments 失败。`CallID` 由 Run 派生（`DeriveCallID(source, index)`），是 Run 内的持久化 identity，进入 fact、派生 CommandID 与 chatlog；`ProviderCallID` 是模型发出的 `tool_call_id`，只用于 PromptBuilder 回传工具结果时与模型配对，Run 不以它为键，也不要求它唯一或非空。Decide 校验每个 binding 的 CallID 等于派生值、ProviderCallID 等于模型结果中对应位置的 id。已知工具使用匹配 frozen ToolSpec 的 ref/digest/policy；未知工具保留为同名 unresolved DirectExecution binding，并在执行前收束为已知 lookup failure。approval/external response 的 `ResponseRequest` 由 Decide 稳定派生。Unknown outcome 使用 class `effect_unknown`，只把该 Executing call 记为 `ToolCallFailed(Unknown)`。Run 保持 Active；同 step 其他 call 继续。全部 call 进入 Completed 或 Failed 后 Evolve 关闭 ToolStep。

`AgentCommand` 与 `Fact` 都是 sealed interface。v1 的 command→fact 规则为：

| command | precondition / facts |
|---|---|
| `AcceptInput` | 任意非终态；批内每个输入一条 `InputAccepted`，按顺序追加到 `PendingInputs`，全有或全无。空批次为拒绝；同一 InputID 已在 pending 或在批内重复为 conflict，整批无事实 |
| `PrepareModelRequest` | `Open`，完整有序消费 PendingInputs，RequestDigest 等于请求本体的 digest，ToolSpec 与请求内工具定义逐一对应；`ModelStepPrepared`。command 携带请求本体，fact 只留 digest，本体由 Run 的 Command Part 写入 `frozen.Store`；Dispatch 时复制到 executor-owned payload |
| `WithdrawPreparedStep` | Model Prepared 且 `PendingInputs` 非空；`ModelStepWithdrawn`，`Current` 回到 `Open`，该请求本体可释放 |
| `StartModelExecution` | Model Prepared；`ModelStepStarted`。command 携带本次 start 请求的 `Effect`，须等于 `DeriveEffectID(RunID, StepID, "", Rejects)` |
| `RecoverModelExecution` | Model Executing；`ModelStepRecovered`，`Current` 回到 `Open`、不计入 `ModelSteps`、PendingInputs 保留。携带该 step 正在执行的 `Effect`，须等于 `ModelStep.Effect` |
| `SubmitModelResult` | Model Executing；携带该 step 正在执行的 `Effect`，须等于 `ModelStep.Effect`；`ModelStepCompleted{Effect, Usage, FinishReason, ResultDigest}`。有 calls 时随后 `ToolStepOpened`（携带冻结的 `Scheduling` 与 bindings）；无 calls 且 `PendingInputs` 为空时随后 `RunEnded(completed)`；无 calls 且 `PendingInputs` 非空时 `Current` 回到 `Open`，Run 继续。command 携带冻结 `ModelResult` 本体，Runtime 先以 ResultDigest 存入 `frozen.Store` |
| `SubmitModelFailure` | Model Executing；携带 `Effect`，须等于 `ModelStep.Effect`；`ModelStepFailed{StepID, Effect, Failure}` 结算该 effect，同组随后 `RunEnded(failed/provider_failure)`（class 为 `effect_unknown` 时 reason 为 `effect_unknown`） |
| `RejectModelResult` | Model Executing；携带 `Effect`，须等于 `ModelStep.Effect`；`ModelStepRejected{StepID, Effect, Usage, Failure}` 结算该 effect，由调用方显式选择回到 Prepared 或在同一组追加 `RunEnded(failed/malformed_model_result)` |
| `StartToolCall` | Tool Pending；`ToolCallStarted`。command 携带本次 start 请求的 `Effect`，须等于 `DeriveEffectID(RunID, StepID, CallID, 0)` |
| `SubmitToolResult` | Tool Executing；携带 `Effect`，须等于该 call 的 `Effect`；`ToolCallCompleted{Effect, OutputDigest}`。command 携带输出本体，Run 的 Command Part 先以 OutputDigest 存入 `frozen.Store`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `SubmitToolFailure(Known)` | Tool Executing；携带 `Effect`，须等于该 call 的 `Effect`；`ToolCallFailed(Known)`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `SubmitToolFailure(Unknown)` | Tool Executing；携带 `Effect`，须等于该 call 的 `Effect`；`ToolCallFailed(Unknown)`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `DeclineToolCall` | Tool Pending；`ToolCallFailed(Known)`。start 之前的校验失败（RUN-EXE-5）：不写 `ToolCallStarted`，该 call 没有 effect。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `ApproveToolCall` | Waiting(Approval)；`ToolCallApproved` |
| `RejectToolCall` | Waiting(Approval) 记 `ToolCallFailed(Known/permission_denied)`；Waiting(ExternalResponse) 记 `ToolCallFailed(Known/response_rejected)`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `SubmitToolResponse` | Waiting(ExternalResponse)；`ToolCallAnswered{ResponseDigest}`。Evolve 后若全部 call 已 terminal，则关闭 ToolStep |
| `CancelRun` | active；Pending / Waiting tool call 记 `ToolCallFailed(Known/cancelled)`，Executing call 记 `ToolCallFailed(Unknown/effect_unknown)`，保留已有终态结果；Executing 的 ModelStep 记 `ModelStepFailed{Effect, Failure{Class: effect_unknown}}`。全部 call 进入终态后关闭 ToolStep 并写入 `LastToolStep`，随后 `RunEnded(stopped/cancelled)` 把 `Current` 置空。`RunStoppedEnd` / `RunResult` 的 `UncertainCalls` 只列本次产生 Unknown 的 CallID，`UncertainModel` 标识仍 Executing 的模型步骤。 |

最后一个 ToolCall 进入 Completed 或 Failed 时，`Evolve` 在折叠该 fact 后若全部 call 已 terminal，则把 Current 设为 `Open` 并写入 `LastToolStep`；下一次 `PromptInput.SourceStep` 取自 `LastToolStep.RefValue.ID`。Cancel 产生的 failure facts 同样走这条关闭规则；`RunEnded` 再把 Current 置空。

**RUN-MCH-3** `schema.Machine.Decide(state, command)` 执行全部验证与 derived consequence，一次返回该组的完整 ordered fact group；验证成功后返回完整 facts。`Machine.Evolve(state, fact)` 机械折叠 fact，依赖 fact 携带的完整执行状态数据。accepted facts 必须 self-contained；若该组 terminalize，`RunEnded` 必须是 Decide 输出的最后一个 fact。

启动 command 的最小公共形状为：

```go
type StartModelExecution struct {
    StepID StepID
    Effect EffectID
}
type StartToolCall struct {
    StepID StepID
    CallID CallID
    Effect EffectID
}
type RecoverModelExecution struct {
    StepID StepID
    Effect EffectID
}
```

一个 effect 的全部 command identity 都从其 `EffectID` 派生：start、settlement、recovery 的 CommandID 分别按上表计算，Commit 对 start、settlement 与 recovery 强制校验该派生，对 `DeclineToolCall` 校验以 call 坐标派生的 decline CommandID（RUN-WIR-1）。start 事实持久化 Effect；重连沿用该 EffectID 定位 Assignment 与提交结果。提交返回非 sentinel 错误时，以同一 Effect 重放得到同一 CommandID，Writer 对精确重放返回 AlreadyApplied（RUN-LOP-5）。进程崩溃后由接管者按 RUN-CMT-7 重连或处置 Executing 目标。

`Next(state)` 最多返回一个 transient `Action`：

| state | action |
|---|---|
| terminal | 返回 `ErrRunTerminal`，没有 action |
| `Open` | `NeedModelRequest{PromptInput}` |
| Model Prepared 且 `PendingInputs` 非空 | `WithdrawPrepared` |
| Model Prepared | `StartModelCall` |
| Model Executing | `Idle` |
| ToolStep 有 Pending calls | `StartToolCalls` |
| ToolStep 无 Pending、仍有 Waiting 或 Executing | `Idle` |

Waiting call 上的 `ResponseRequest` 由 `WaitingCalls(state)` 读取。Executing call 由 `ExecutingCalls(state)` 读取。`NeedsRecovery(state)` 在 Model Executing 或 ToolStep 无 Pending 且仍有 Executing 时为 true。这些查询只读状态，不派生 Action。

**RUN-MCH-4** Action 由调用方每次 Load 后重新派生，不持久化；start action 请求的 effect 在其 start 事实提交后才存在，其余 action 不请求 effect。`AcceptInput{Inputs}` 携带一个有序、非空的输入批次，在任意非终态入队，Decide 不因 Run 正在执行而拒绝它；批次全有或全无，任一输入非法则不产生任何事实；`PendingInputs` 只在 `Open` 的 Prepare 中被消费。`PrepareModelRequest.InputIDs` 必须与当前 PendingInputs 等长、同顺序、逐项相同；prepare 接受后一次消费全部 pending input。ToolStep 的 Waiting call 禁止 Start，同一 step 中的 Pending call 仍可执行。没有可执行 Start 时 `Next` 返回 `Idle`。Application 从投影读取 `WaitingCalls` 并提交 `ApproveToolCall` / `RejectToolCall` / `SubmitToolResponse`。Executing 目标在当前 owner 进程内由其 worker 结算；owner 崩溃后由接管者按 `NeedsRecovery` 一次性处置（RUN-CMT-7）。

## 5. RunStore、投影与 Commit

Run 核心（`agentcore/run`）只依赖自己的类型。它对存储的全部要求是 `agentcore/run/runtime` 中的一个端口，版本绑定在 `agentcore/run/schema`：

```go
// package agentcore/run
// Scope 是 Run 所在 store 的不透明身份（Twilight 里是 SessionID）；Run 不解释它，
// 只用它区分共享 executor 上不同 store 的执行键、接管询问与 prompt builder 的读取范围。
type Scope string
// RunPosition 是该 Run 自己 stream 中最后一条事实的下标；只有这个 Run 自己的事实会移动它。
type RunPosition uint64

// package agentcore/run/runtime
// RunStore 是已绑定调用方写能力的事务端口（RUN-CMT-1）。它不认识 Writer、Session、Head。
type RunStore interface {
    Scope() run.Scope
    Load(context.Context, run.RunID) (Snapshot, error)
    Commit(context.Context, CommitRequest) (CommitResult, error)
    FrozenRequest(context.Context, run.Digest) (model.ModelRequest, error)
}
type Snapshot struct {
    State run.MachineState // detached in-process view
    Position run.RunPosition
}
type CommitRequest struct {
    Base run.RunPosition // Load 时的 Position；PrepareModelRequest 为 hard CAS，其他 command 可为零值
    Command wire.CommandEnvelope
}
type CommitResult struct {
    Status CommitStatus // CommitAccepted | CommitAlreadyApplied
    Snapshot Snapshot
    Facts []run.Fact // 本次 command 产生的 Run facts；重放时为原 commit 的 facts
}

// package agentcore/run/schema：Run 协议的六个契约，无版本的包级单值（RUN-CMT-8）
var (
    Machine run.Machine         // Decide / Evolve / CreateGroup（RUN-MCH）
    Wire wire.Codec             // FactType / CommandType / DecodeFact / DecodeCommand / EncodeFact / Envelope（RUN-WIR）
    Canonical run.Canonical     // Digest*（RUN-WIR-4）
    Snapshot wire.SnapshotCodec // Encode / Decode MachineState
    Identity run.Identity       // Derive*：RunID 作用域内的 step、call、response 与 command identity（RUN-WIR-3）
    Bodies frozen.Codec         // 冻结正文的信封编解码，digest 由这些信封计算（RUN-WIR-4）
)
// fact 与 command 的变体在 wire 的一张注册表里登记一次（wire/registry.go）：discriminator、decoder 与 Go 类型来自同一条目，
// wire.FactTypes() 是 Session 模块注册 wire 类型的来源，没有第二份清单。
```

Session 侧的适配器是 `agentcore/session/run` 的 `SessionRunStore`：`Bind(w writer.Writer) runtime.RunStore` 把端口绑定到调用方的 Writer（所有权能力，OWN-HDL-2）；`Record(ctx, sid, runID)` 按 SessionID 从 Store 折叠，不取得所有权；`Command(ctx, req)` 与 `CreateRun(newRun, inputs)` 是 Run 模块在跨模块 unit of work 里的 Part。`agentcore/session/run` 声明流 domain `run`（Key 为 `runmod.Event.RunID`，`LineageSegment`，EXT-STR-1）：每个 Run 一条流 `run/<RunID>`，Run 事实全部写入它。它以 `extension.Registry`、`session.Store`、`frozen.Store`（`FrozenValues(content, bindings)`：Put 同时登记 FrozenBinding，调用方不再协调两个 store）、`SnapshotPolicy` 与投影缓存构造。`frozen.Store` 保存 Authority 生成的 immutable 正文；Executor 接受 Assignment 后必须把请求转存为 executor-owned payload。

**跨模块提交** 只有一个协调者：`unit.Commit(ctx, w, now, unit.Work{CommitID, Parts})`。每个 Part 在同一 View 上 `Prepare` 出自己模块的 batch，按 Part 顺序合并为每 stream 至多一个 batch，一次 Append 或什么都不写；任一 Part 拒绝即整组拒绝；CommitID 已在 log 中时不准备任何 Part，直接返回 already-applied。Turn 不再拼 Run 的 wire 事件，Run 不再携带 chatlog 事件（TRN-STR-2、TRN-DLV-2、TRN-STP-1 都是三个模块各出一个 Part）。`RunStore.Commit` 本身就是只含 Run Part 的 unit。

**RUN-CMT-1** RunStore 按 RunID 寻址，Scope 由绑定决定。Run 由 owner 模块的创建 unit 里的 `CreateRun` Part 建立（TRN-STR-2），RunStore 没有 `Create`。`ErrRunNotFound` 只用于该 Session 中不存在的 RunID。已终结的 Run 不在 `twilight/run/machine` 投影中（RUN-CMT-2），`Load` 对它读取该 Run 自己的 stream 后 FoldRun，返回终态 snapshot；`Commit` 对它返回 `ErrRunTerminal`（终态与未知由 kernel 的 stream 索引 `View.StreamHead` 区分）。这条路径是兜底：正常流程中 Loop 从结算返回的 snapshot 读到终态（第 7 节），Coordinator 从 turn surface 的 `AttemptView` 取终态与 SchemaVersion（TRN-PRJ-1），都不依赖它。

**RUN-CMT-2** 投影 `twilight/run/machine` 消费全部 `twilight/run/` 事件，其他模块的事件按 EXT-PRJ-2 跳过，状态为：

```go
type MachineProjection struct {
    Active map[RunID]MachineState   // 非终态 Run
    Positions map[RunID]RunPosition // 非终态 Run 的最后事件位置
    Schemas map[RunID]uint16        // 非终态 Run 的协议版本
}
```

终态 Run 在 `RunEnded` 折叠后整体离开投影；终态结果由 `Record` 与 turn surface 提供，投影大小与活动 Run 数成正比。同一 RunID 的第二条 `created`（RUN-NEW-1）在提交时由 `CreateRun` Part 用 kernel 的 stream 索引（`View.StreamHead(run/<RunID>)`）拒绝为 `ErrRunExists`，投影不保留已终结 RunID 的集合；一条 ledger 里若真出现两个同 RunID 的 Run，是 SES-REP-1 的完整性问题。`Load(ctx, w, runID)` 是命令路径的读取：经传入 Writer 的 `Projections()` 读 owner 内存中的投影（EXT-PRJ-4），失去所有权的 owner 因此仍按自己的视图规划并在提交时被围栏；`Record(ctx, sid, runID)` 与独立进程的观察者经 `extension.NewProjectionReader` 从 Store 读取，不取得所有权（OWN-HDL-2），投影缓存（EXT-PRJ-3）是可丢弃的派生数据，写入策略由 `agentcore/session/run` 的 `SnapshotPolicy` 决定，默认在 Run 的 `Current` 回到 `Open` 或 Run 终结时写入，并可按组计数补充。`Record` 以 `Types=[twilight/run/]` 过滤 `Read` 读取该 RunID 的全部事件（SES-REP-2），FoldRun 重建，该折叠即权威读取；该 Run 仍在投影中且两次读取落在同一 head 时与投影状态比对，divergence 必须失败；head 不同说明两次读取之间有提交落盘，两者各自正确，不比对。

**RUN-CMT-3** Commit 经 unit of work 在该 Session 的 Writer 互斥区内完成（EXT-WRT-1）。Run 的 `Command` Part 调用同一个 pure `runtime.EvaluateCommit`，顺序固定为：

```text
  0  构造 Command Part 时冻结正文：PrepareModelRequest 的 Request、SubmitModelResult 的 Result、
     SubmitToolResult 的 Output、SubmitToolResponse 的 Payload 以其 digest 的信封存入 frozen.Store
     （Put 幂等，并登记 FrozenBinding(digest)）
unit.Commit(view):
  1  view.LookupCommit(CommitID = CommandID)；found -> 不准备任何 Part，返回 already-applied，
     Command.Result 以原 commit 的 facts 与当前投影构造 CommitAlreadyApplied
  Command.Prepare(view):
  2  state = view.Projection(twilight/run/machine).Active[RunID]
     不在 Active：view.StreamHead(run/<RunID>) 存在 -> ErrRunTerminal；否则 ErrRunNotFound
  3  validate envelope RunID/type，derived CommandID check
  4  validate hard CAS（prepare 的 Base == Positions[RunID]）/ target state
  5  facts = schema.Machine.Decide(state, command) exactly once
  6  schema.Machine.Evolve in order；facts -> TypedEvent（Type twilight/run/<name>，v = 该事实类型的 payload 版本）
  7  return [run/<RunID>: facts]；其他 Part 的 batch 由 unit 按 Part 顺序合并
)
// Writer 完成 codec、Binding admission（含命名正文的 fact 的 FrozenBinding）、claim（先于 Append）、Append 与投影折叠（EXT-WRT-1/3）。
// SessionRunStore 在 Commit 返回后按 SnapshotPolicy 写投影缓存；缓存写入失败不影响 commit 结果。
```

`frozen.Store` 的 `Put` 幂等且内容寻址，在进入 Writer 之前完成；Commit 失败时留下的本体无害，可由保留策略回收。Assignment 被接受后，执行所需 payload 的生命周期由 Execution Store 管理，不能依赖 Owner 的 `frozen.Store` 的短期可用性。

**RUN-CMT-4** `PrepareModelRequest` 是 hard-CAS command：`Base` 必须等于投影记录的该 Run 的 `Position`。这是有意选择：同一 Session 内其他模块的写入（用户提交新输入、summary、compaction、其他 Turn 的事件）不移动 Position，因此不使 Prepare 失效；Plan 与 Prepare 之间发生的 chatlog 写入不会被本次请求包含，新鲜度由 Application 经 `PromptToken` 与 PromptBuilder 自行负责，Run 不校验 `PromptToken` 的语义。其他 command 通过当前 target state 做 call-local rebase，`Base` 可为零值或过期值；stale Base 本身不阻止无冲突的 ingress/control/settlement。相同 command 的 replay 判定先于 terminal check，因此 terminal Run 仍能返回原组。

**RUN-CMT-5** 幂等键为 Session 的 `(SessionID, CommitID)` 提交索引（SES-REP-3/4、EXT-WRT-2），CommitID 等于 CommandID，RunStore 不另设幂等索引。同 CommandID 的重放返回 `CommitAlreadyApplied`、当前 snapshot 与原完整组，且不得再次 Decide 或产生外部 effect；command 不持久化，重放只按 CommandID 判定（SES-APP-4、EXT-WRT-2）：identity 有意不覆盖内容的两族（同一 effect 的两次结算、同一 ResponseID 的 approve 与 reject）在内容不同时同样返回 `CommitAlreadyApplied`，以先提交的为准，调用方从投影读取实际生效的结果。对于 start、settlement 与 recovery command，EffectID 是 CommandID 的 preimage，不同 effect 即不同 command：start 按当前 target state 评估，target 已是 Executing 时返回 `ErrStaleRuntime`；settlement 的 Effect 与 Executing 目标记录的 Effect 不符时返回 `ErrStaleRuntime`；缺少 Effect 的 command 无法派生 CommandID，返回 `ErrCommandConflict`。`DeclineToolCall` 没有 effect，其 CommandID 以 call 坐标派生，同一 call 的第二次 decline 为重放。

**RUN-CMT-6** Run 语义提交的 ownership fencing。RunStore 不签发 per-effect grant，也不校验 Worker 的 operational lease；同一进程内同一 Run 至多一个 Loop 在驱动（第 7 节的 driver slot）。Executing 目标的 settlement 必须通过 Session Writer；跨进程的迟到语义写入由 kernel 的 Epoch fencing 拒绝（SES-OWN-2）。Writer 返回 `ErrOwnershipLost` 时 RunStore 原样返回该错误，Loop 必须取消全部 worker、放弃 settlement 并以该错误返回（RUN-LOP-5）。Executor Worker 的 owner/epoch 由 Execution Store 独立校验。

**RUN-CMT-7** 接管处置。处置只恢复同一 Run：不创建 attempt、不结束 Run；对工具 call 的 Unknown 结算是该 call 的终态事实，协议在任何路径上都不据此自动重新执行（TRN-DUR-1、TRN-DUR-4）。新 owner 取得 Writer 后，在驱动任何 Run 之前以该 Writer 调用一次 `SessionRunStore.RecoverInterrupted(ctx, w, reconciler)`。三方的分工固定：Run 机器说哪些目标是 Executing、请求了哪个 effect（`plan.RecoveryTargets`）；execution store 说该 effect 的 attempt 是否还存在（`ExecutionPort.Attach`）；`agentcore/run/reconcile` 的 `Reconciler` 是比较两侧的唯一位置，对每个目标给出 Verdict：`keep`（`active` / `terminal`，保持 Executing，后台读取 Outcome 并交付）、`defer`（`orphaned`：记录存在但没有活租约，不按 `missing` 处理，保持 Executing 并同样等待 Outcome；`Executions` 实现 `effect.Recoverer` 时 Reconciler 随即调用一次 `RecoverExecution(key)` 请求接管，此后 Outcome 读取循环在退避到上限后按 Attach 结果每个 orphaned 阶段再请求一次；放弃只经显式 `Dispose`）、`redispatch`（`missing` 且重派预算未用尽：先在 dispatch ledger 记下这一次，交 Executor 后再记送达，保持 Executing，RUN-EXE-15）、`dispose`（`missing` 且策略为 `DisposeMissing`，或策略为 `RedispatchMissing` 而预算已尽，产生 Run 的恢复 command）。对 `missing` 的处理由显式策略 `reconcile.MissingPolicy` 决定（`DisposeMissing` 为零值；`RedispatchMissing` 要求 `Redispatch` 端口与 dispatch ledger 同时存在，缺一为 `ErrMissingPolicyPorts`），端口是否装配不反过来选择策略。处置命令由 `plan.RecoveryCommand` 按目标派生，Run 只验证该处置是否合法并记录事实；Loop、Driver 与 store 适配器都不解释 executor 的观察。若同一 effect 的 attempt 仍存在，Run 保持 Executing，结果以原 Effect 结算，属于重连同一次执行。只有执行记录被证明不存在（`missing`）或调用方明确放弃（`Reconciler.Abandon`）时，才处置；对 `missing` 的处置先调用 `Executions.Abort(key)` 在 Executor 上关闭该 key（RUN-EXE-16）：返回 `aborted` 才提交恢复 command，返回其他状态说明一次 acceptance 抢先到达，按该状态改为 `keep`/`defer`；两步之间崩溃，下一次接管处置得到 `aborted` 再次处置，恢复 CommandID 由 effect 派生，天然幂等；`Executions` 为 nil 而未声明 `Abandon` 时 Plan 返回 `ErrNoExecutionPort`，"无 executor 可问"不等于"无执行"：Executing ModelStep 提交 `RecoverModelExecution{Effect}`，回到 `Open` 并按恢复时刻重新规划；Executing tool call 提交 `SubmitToolFailure{Outcome: Unknown}`。Pending call 不处置，Waiting call 不处置。每个处置是一次普通 Commit，Run 保持 Active，同一 RunID 继续。处置的 CommandID 由 `DeriveRecoveryCommandID(Effect)` 派生，与 owner 和 Epoch 无关，任何 owner 的重复处置得到 AlreadyApplied。

**RUN-CMT-8（Run 协议不设版本）** Run 的六个契约（Machine、Wire、Canonical、Snapshot、Identity、Bodies）是 `agentcore/run/schema` 的包级单值，没有版本号，也没有按版本选择的入口。理由按契约分述：Identity 派生的 CommandID、StepID、EffectID 是持久化语义，派生规则变化会使同一 effect 的重放得到另一个 CommandID，因此它不能有版本，其预映像常量归自身（与 SES-VER-3 同理）；Canonical 与 Bodies 的 digest 是 cas 的键，事实记录写下时的 digest，其预映像与信封的版本常量（`v1:` 前缀）归各自的字节，不是选择器；Wire 与 Machine 面对的事实形状由每个事实类型的 payload 版本 `v` 与 codec 演进（SES-VER-1、EXT-REG-2），codec 把旧版本 upcast 到当前内存类型，Decide 与 Evolve 只有一份；Snapshot 是投影缓存的编码，格式不符只导致重折。`CommandEnvelope` 与 `runtime.Snapshot` 都不携带版本，machine 投影不记录每个 Run 的版本，命令不做版本比对。Run 之间因此没有"不同版本并存"的情形：新事实类型或新字段以事件类型的 `v` 发布，所有 Run 由同一份 Decide 与 Evolve 处理。

### 5.1 不进入 ledger 的数据

Executor 的 in-flight 表可以只是进程内缓存；跨 Worker 恢复所需的是 durable Execution Ledger（RUN-EXE-14）。投影缓存是可丢弃的派生数据（EXT-PRJ-3）；`frozen.Store` 是 Owner 侧的内容寻址旁存。Worker crash 后，观察到该 record 为 `orphaned` 的一方（持有该 Run 的 Owner，或外部控制器）调用 `RecoverExecution(key)`，接管的 Worker 从共享 Execution Store 获取同一 AssignmentKey 的 payload，并按 backend 的 Attach 结果决定继续观察、Restart 或 Unknown（RUN-EXE-6）。Execution Record 还持久化 `ExecutionRef{Provider, Ref}`（RUN-EXE-9）：backend 只按 Ref 寻址，Attach、Status、Outcome、Cancel 都经 record 的 Ref 到达它；目标到 Workspace/Runtime 的解析由 backend（provider adapter）在 `Prepare` 与 `Start` 中完成。Session 所有权与 Worker execution ownership 是两层不同的 ownership。

## 6. Loop ports 与 policy

```go
// package agentcore/run/plan
type PromptInput struct {
    Scope run.Scope // 由 Loop 填入；Next 不读取它
    RunID run.RunID
    SourceStep run.StepID
    Inputs []run.AgentInput
}
// package agentcore/run/loop
type PromptBuilder interface {
    Build(context.Context, plan.PromptInput) (Prompt, error)
}
type Prompt struct {
    Model run.ModelRef
    Request sdk.Request
    InputIDs []run.InputID
    Token run.PromptToken // 构造时刻 context 的新鲜度标记
    Tools []run.ToolSpec // 与 Request.Tools 一一对应；DefinitionDigest 由 Loop 校验
}
type ModelCatalog interface { ResolveModel(run.ModelRef) (ModelInvoker, error) }
type ModelInvoker interface { Generate(context.Context, sdk.Request) (sdk.ModelResult, error) }
type StreamingModelInvoker interface { Stream(context.Context, sdk.Request) (sdk.ModelStream, error) }
type ToolCatalog interface { ResolveTool(run.ToolRef) (ExecutableTool, error) }
type ExecutableTool interface {
    Ref() run.ToolRef
    Definition() sdk.ToolDefinition
    ResponsePolicy() run.ResponsePolicy
    ValidateArguments(run.CanonicalJSON) error
    Execute(context.Context, ToolExecutionRequest) ToolExecutionOutcome
    Replay() run.ReplayPolicy // 必填声明：前次执行丢失后同一 call 的 Execute 能否重跑；经 PublicTool → ToolSpec → Assignment 到达 Worker
    Placement() run.ToolPlacement // 必填声明：在进程内还是在 workspace 内运行；经 PublicTool → ToolSpec → Assignment 到达解析器与 Worker 的 Route 表
}
```

`ToolExecutionOutcome` 是 sealed interface：`ToolExecutionSucceeded`、`ToolExecutionFailed`（明确未完成）或 `ToolExecutionUnknown`（可能已发生）。`ValidateArguments` 在 start barrier 前运行，并保持无外部 effect。`ToolExecutionRequest` 携带 RunID、StepID、CallID、该 call 的 `Effect`、冻结 binding 与可选 opaque `TargetRef`（Loop 不解释）。`TargetRef` 由 `TargetResolver` 按 effect 解析（RUN-LOP-9）。

```go
// 效果层端口（RUN-EXE）
type AssignmentKind string // model | tool
type AssignmentKey struct { Session run.Scope; RunID run.RunID; Effect run.EffectID }
type ModelAssignment struct { Model run.ModelRef; Request *model.ModelRequest; RequestDigest run.Digest } // Dispatch payload；digest 仍绑定 frozen request
type ToolAssignment struct { ToolRef run.ToolRef; DefinitionDigest run.Digest; Arguments run.CanonicalJSON; Policy run.ResponsePolicy; Replay run.ReplayPolicy; Placement run.ToolPlacement }
type Assignment struct {
    Session run.Scope; RunID run.RunID; StepID run.StepID; CallID run.CallID; Effect run.EffectID
    Target *run.TargetRef
    Schema uint16 // Run 的协议版本
    Body AssignmentBody // sealed：ModelAssignment | ToolAssignment；Kind 由变体派生，wire 上仍是 {Kind, Model, Tool}，解码拒绝 Kind 与 body 不一致
}
// Outcome.Result 是封闭变体，没有 Go error，也没有可以互相矛盾的标志位：
//   ModelSucceeded{Result} | ModelFailed{Code, Message} | ToolExecutionSucceeded | ToolExecutionFailed{Failure, Retry} | ToolExecutionUnknown | Cancelled{Message} | Unknown{Message}
// FailureCode 是 wire 稳定的：executor_error | frozen_value_missing | malformed_frozen_request | malformed_result | deadline_exceeded | rate_limited | provider_unavailable | connection_failed | authentication_failed | billing | bad_request；FailureCode.Retry() 对 rate_limited、provider_unavailable、connection_failed 给出 RetryAllowed，其余 RetryNever（RUN-EXE-11）
type Outcome struct { Key AssignmentKey; Result OutcomeResult }
func (Outcome) Status() ExecutionStatus // 终态 Outcome 对应的 ExecutionStatus，唯一的派生点
type Attachment struct {
    State AttachmentState // missing | active | orphaned | terminal
    Execution ExecutionStatus
    Owner string; FencingEpoch uint64; LeaseUntilUnixMilli int64
    BackendAttached bool
}
// agentcore/executor —— Worker 之下的 backend 契约（RUN-EXE-9/10）
type ExecutionRef struct { Provider string; Ref string }            // 只在 Execution Ledger 与 Backend 契约中出现
type ExecutionBackend interface {
    Validate(context.Context, Assignment) (*run.ToolFailure, error)
    Prepare(context.Context, Assignment) (ref string, err error)     // 分配或派生 Ref，不启动；按 AssignmentKey 幂等
    Start(context.Context, ref string, Assignment) error             // 启动 Ref；ErrDispatchUnknown 语义同 Dispatch
    Restart(context.Context, previous string, Assignment) (ref string, err error) // 上一代 missing 后分配下一代执行的 Ref；工具能否重派由 Worker 先按 Assignment.Replay 裁决
    Attach(context.Context, ref string) (Attachment, error)          // missing 只在能证明 Ref 的执行不存在且不会开始时给出；无法确认时为 orphaned
    Status(context.Context, ref string) (ExecutionStatus, error)
    Outcome(context.Context, ref string) (Outcome, error)            // 一次读取：终态 Outcome，未结算为 ErrOutcomeNotReady，未知 Ref 为 ErrExecutionNotFound；从不阻塞在执行上
    Cancel(context.Context, ref string) error
}
type notice.Source interface { Settled(ctx context.Context, epoch string, after uint64, fn func(notice.Ref) bool) error } // Backend 可选：按 Ref 的结算通知流，Worker 据此等待（RUN-EXE-17）
type notice.Ref struct { Ref string; Epoch string; Sequence uint64 }
type notice.Ring[T any] struct{ /* epoch、有界窗口、Sequence */ } // 全部结算通知源共用的一个环：Worker 的 SettlementHub 与 Backend 的 RefHub 都是它的实例
type Route struct { Provider string; Match func(Assignment) bool; Backend ExecutionBackend } // nil Match 接受全部
type ExecutionPort interface { // effect.ExecutionPort；loop.Executor 是它的别名
    Validate(context.Context, Assignment) (*run.ToolFailure, error) // start barrier 前的无副作用校验
    Dispatch(context.Context, Assignment) error                    // 接受后通过 GetOutcome 读取结果
    Attach(context.Context, AssignmentKey) (Attachment, error)     // 区分 active、orphaned、terminal、missing；missing 是已证明的不存在
    GetStatus(context.Context, AssignmentKey) (ExecutionStatus, error)
    GetOutcome(context.Context, AssignmentKey) (Outcome, error)    // 纯读取，立即返回；未结算为 ErrOutcomeNotReady（RUN-EXE-17）
    Cancel(context.Context, AssignmentKey) error
}
type Acknowledger interface { Acknowledge(context.Context, AssignmentKey) error } // 可选：Owner 结算提交后确认，Executor 随即回收 record 内容（RUN-EXE-13）
type Recoverer interface { RecoverExecution(context.Context, AssignmentKey) error } // 可选：把 orphaned record 接回活租约并继续执行；何时请求由观察到 orphaned 的一方决定（RUN-EXE-6）
type Settlement struct { Key AssignmentKey; Epoch string; Sequence uint64 } // 结算通知：不携带 Outcome，只说何时去读（RUN-EXE-17）
type SettlementPort interface { Settlements(ctx context.Context, epoch string, after uint64, fn func(Settlement) bool) error } // 可选：每个 executor 一条订阅，覆盖它结算的全部 key
type Watcher struct { Port ExecutionPort; Poll, Reconnect time.Duration } // Owner 侧：一条 Settlements 订阅 + 周期性读取，服务多个 key 的等待；Watch(ctx, key, deliver, fail) 登记一个 key；Await(ctx, key) 是单个 effect 调用方的同步形态，共用同一条订阅
type WorkerOptions struct { ID string; LeaseDuration time.Duration; Clock func() time.Time; Retry RetryBudget; Progress *ProgressHub; Settlements *SettlementHub }
type RetryBudget struct { MaxAttempts int; Backoff time.Duration } // 部署级重发预算（RUN-EXE-11）；零值关闭；哪次失败值得重发由失败自己的 RetryDisposition 决定
func NewWorker(ctx context.Context, records executionstore.Store, routes []Route, options ...WorkerOptions) (*Worker, error)
func (*Worker) Close() // 停止 heartbeat 与 watch 并等待；不取消 backend 执行
func (*Worker) RecoverExecution(context.Context, AssignmentKey) error // effect.Recoverer：接回一条租约过期或从未持有的记录并继续；活租约与终态记录不动
func (*Worker) Dispose(context.Context, AssignmentKey) error          // 调用方放弃一条记录：结算为 Unknown 终态
func NewLocalExecutor(models ModelCatalog, tools ToolCatalog, sink EventSink, streaming bool) (*LocalExecutor, error) // ExecutionBackend；经 Worker 成为 ExecutionPort

// agentcore/run/reconcile
type Verdict string // keep | defer | dispose
type Reconciler struct { Executions ExecutionPort; Abandon bool; Deliver func(Outcome); Fail func(AssignmentKey, error); Watcher *effect.Watcher; Lifetime context.Context; OrphanProbe time.Duration } // Abandon：不询问 executor 而全部处置，须由调用方显式声明；Deliver 需要 Watcher 与 Lifetime
func (*Reconciler) Plan(ctx, scope run.Scope, snapshot *runtime.Snapshot) ([]Decision, error)
func (*Reconciler) Reconcile(ctx, store runtime.RunStore, snapshot *runtime.Snapshot) (int, error)
```

这里有三个不同的状态域，不能用同一个枚举替代：`ExecutionStatus` 描述 provider execution 的生命周期；`AttachmentState` 描述 Executor 对 Assignment 的观察（`missing | active | orphaned | terminal`）；reconcile 的 `Verdict` 描述 authority 的下一步（`keep | defer | dispose`）。因此 `AttachmentState=orphaned` 明确映射为 `Verdict=defer`：前者是资源观察，后者表示暂时不得处置 Run。`RunStatus` 与 `TurnStatus` 的 `active` 也只表示各自聚合仍未终态，不等价于 Executor 的 `active`。

**RUN-EXE-1（Assignment）** Assignment 是 Owner 交给 Executor 的工作单元：目标（RunID、StepID、CallID）、请求的 effect（Effect）、Run 的协议版本，以及执行所需的冻结输入。模型 Assignment 携带 `ModelRequest` 与 `RequestDigest`；Executor 接受后必须将该 payload 写入自己的 durable Execution Record，不能依赖 Authority Session 或某个 Worker 的本地存储。Assignment 可携带 opaque `TargetRef`，但 Agent Core 不解释其 Kind 或生命周期。`Key()` 命名该 effect（Scope、RunID、EffectID），EffectID 也是其结算 CommandID 的 preimage（第 2 节 identity 表）。AssignmentKey 是 Agent Core 与 Executor 之间唯一的连接；effect 的 attempt（Execution Record 及其到物理执行的映射 `ExecutionRef{Provider, Ref}`，RUN-EXE-9）只由 Executor 持有，不进入 Session 事实，也不出现在 `effect.Port` 或 Agent Core 的任何接口上。

**RUN-EXE-2（Outcome）** Outcome 是 Executor 对一个 Assignment 的唯一回答：模型 Assignment 得到 `Model` 或 `Err`，工具 Assignment 得到 sealed 的 `Tool`；`Cancelled` 表示 Executor 按要求停止了该效果；`Unknown` 表示 Executor 已明确结束该执行的恢复，外部结果仍无法确定。模型结果写入 record 或 wire 之前须能冻结（`sdkconv.FreezeModelResult`，含 UTF-8 校验）：JSON 编码会改写无效 UTF-8，因此不能冻结的结果不以 `Model` 交付，而以 `Err{Code: malformed_result}` 交付，Loop 按 `RejectModelResult` 处理并计入 `MalformedRetries`。Outcome 通过 `GetOutcome` 按 key 读取，也可以由 deployment 层通过通知唤醒读取方；每个被接受的 Assignment 最终至多提交一个 authoritative Outcome。`GetOutcome` 返回的 error 表示读取操作失败，执行状态保持原值。Worker 对读取失败退避重试并保持 lease；失去 ownership 后停止处理。Loop.Run 将读取错误返回给调用方，保留 Executing，后续可重新关联结果。Reattach 的 Attach 请求由调用 context 控制，后台结果读取由构造时传入的 Session ownership `lifetime` 控制。

**RUN-EXE-3（Dispatch 与 Attach）** `Dispatch` 接受 Assignment 后立即返回。它的错误分三类：对 Assignment 本身的确定拒绝（缺内联正文、digest 不符、未知 provider、内容冲突）返回普通 error，效果未开始，Loop 按 Known 失败处理；executor 因自身原因在 barrier 之前拒绝（record store 不可用等，`ErrDispatchRetryable`），效果同样未开始，但同一 Assignment 稍后可以再发，Loop 在同一次 Advance 内有界重发（`loop.Settings.Dispatch{Retries, Backoff}`，零值为 3 次、50ms×第几次；由 `driver.Driver.Dispatch` 提供，属于部署设置而非 AgentPreset）后才按 Known 失败处理；请求发出后的超时、取消或响应丢失返回 `ErrDispatchUnknown`，Authority 保留 Executing。接受时 Worker 先选择 backend（RUN-EXE-10）并经 `Backend.Prepare` 取得 Ref，把完整 Assignment payload 与 `ExecutionRef` 一起持久化为 record，再进入 `Dispatching`，然后 `Backend.Start(ref)`；`Prepare` 发生在 acceptance 写入之前，而 `Abort` 可能抢先（RUN-EXE-16），因此 `Prepare` 只能是按 AssignmentKey 的纯派生，不得分配或预留 backend 之外的任何资源，分配一律发生在只对已接受执行运行的 `Start` 中；`Accepted` 表示确定尚未开始，`Dispatching` 表示可能已经开始，`Running` 表示 backend 已接受。backend 返回 `ErrDispatchUnknown` 时 Worker 保持 Dispatching、续租并读取最终 Outcome；HTTP Server 可确认 Worker 已持久化的 acceptance。相同 Assignment 的 Dispatch 重放确认已有 acceptance，并保留该 record 的 owner、epoch、状态与 `ExecutionRef`；唯一例外是仍为 `Accepted`（确定尚未开始）且没有活租约的 record：写下这条 acceptance 的 Dispatch 在 Start 之前失败（`ErrDispatchRetryable`）或其 Worker 在 Start 之前死亡，重放就是同一请求，Worker 取租约继续它；已开始的 record 只属于租约持有者，其恢复经 `RecoverExecution` 触发（RUN-EXE-6）。`Attach` 按 AssignmentKey 查共享 store 的 record，record 是唯一依据、与应答的 Worker 是谁无关：record 缺失即 `missing`，这是可证明的不存在：record store 只有 durable 实现（本协议不承认随进程消失的 record store），record 在 Start 之前持久化，缺失即从未开始，但它只证明此刻没有，不阻止之后被接受，因此处置前先以 `Abort` 关闭该 key（RUN-EXE-16）；已关闭的 key 为 `aborted`；终态 record 为 `terminal`；record 有未过期租约即 `active`（持有者的 heartbeat 证明其存活，其 watcher 把 Outcome 结算进同一 store，`GetOutcome` 从 store 读取，Owner 不需要重新 Attach）；`GetOutcome` 是纯读取，立即返回：未结算的 record 回答 `ErrOutcomeNotReady`（HTTP 为 204），何时再读由结算通知流告知（RUN-EXE-17）；租约过期或从未被持有为 `orphaned`；应答 Worker 自己持有活租约时另经 `ExecutionRef` 向 backend 核实，backend 找不到 Ref 时同为 `orphaned`。只有 `missing`（经 `Abort` 转为 `aborted` 之后）与 `aborted` 允许 Authority 自动处置，`orphaned` 经 `RecoverExecution` 接管或经 `Dispose` 放弃。`GetStatus` 与 `GetOutcome` 不读取 Session。接管由 `RecoverExecution(key)` 触发，Worker 以新的 fencing epoch 获取同一个 AssignmentKey；接管 `Running`/`Dispatching` 的 record 时先 `Backend.Attach(ref)`：可 attach 则继续观察原执行；backend 报 `orphaned`（无法确认）时 Worker 持有租约、退避重问 Attach，期间不重派，直到 backend 报 `active`/`terminal`（转为观察）或 `missing`；backend 报 `missing` 时模型 Assignment 经 `Backend.Restart` 取得下一代执行的 Ref 后 Start（同一冻结请求；旧 Ref 进入 record 的 `Superseded`，RUN-EXE-9），工具 Assignment 按其 Replay 声明重派或以 Unknown 结算（RUN-EXE-9、TRN-DUR-4）。`Cancel` 针对一个 Assignment；Run 级批量取消由上层枚举 targets。终态 record 的保留与回收见 RUN-EXE-13：record 在 store 中存在期间，其 key 的 acceptance 始终可查，同一 key 的 Dispatch 重放不产生新执行。三分类到 HTTP 状态码的映射是绑定规则，见 CLD-WIR-2。Worker 不运行任何扫描或定时循环：orphaned record 只在有人调用 `RecoverExecution(key)` 时被接管（RUN-EXE-6）。

**RUN-EXE-4（Outcome 的结算）** `Loop.Deliver` 以 `Outcome.Key` 在投影中定位 Executing 的目标：请求了同一 Effect 的 step 或 call。找到则以该 Effect 派生的结算 CommandID 提交携带该 Effect 的 Submit*（模型：结果、provider 失败、畸形结果的 Reject、取消、Unknown 或本体缺失的 Recover；工具：按 sealed outcome 映射，Executor 返回的缺失或未知执行结果记 Unknown）；找不到——effect 已被结算或处置、Run 已终结、Effect 不符——则丢弃，不写任何事实（`LoopDropped`）。GetOutcome 的读取错误保留 Executing，只有成功读取的 Outcome 进入 Deliver。结算使用独立 control context（RUN-LOP-5）。

**RUN-EXE-5（在 start barrier 前校验）** 工具 Assignment 在 `StartToolCall` 之前经 `Executor.Validate` 校验 lookup、definition digest、response policy 与 arguments；非 nil 的失败以 `DeriveDeclineCommandID(RunID, StepID, CallID)` 提交 `DeclineToolCall`，记 `ToolCallFailed(Known)`，不跨越 start barrier，该 call 没有 effect（RUN-LOP-4）。模型 Assignment 在 `StartModelExecution` 之前经同一入口校验 Executor 能否服务该 `ModelRef`；非 nil 的失败使 Loop 以 `ErrModelUnavailable` 返回，step 保持 Prepared，不写入 start 或 recovery 事实——目录缺失不应在每次驱动上留下三条事实。校验不产生外部效果。

**RUN-EXE-6（恢复原语与恢复触发）** Executor 拥有“如何恢复一次执行”的语义，不拥有“何时、对哪些、由谁触发”。前者只有 Executor 知道：`ExecutionRef`、`Backend.Attach/Restart`、ReplayPolicy、租约与 fencing epoch。Worker 因此提供两个按 key 的原语，都不进 `effect.ExecutionPort`（数据面按单个 Assignment 收发消息）：`RecoverExecution(key)` 是可选端口能力 `effect.Recoverer`（与 `effect.Acknowledger` 同一模式，Worker 与 HTTP Client 都实现），把一条租约过期或从未持有的非终态 record 接回本 Worker 的活租约并继续（RUN-EXE-3 的 Attach → 观察 / Restart / Unknown），对终态记录、已在本 Worker 租约下的记录与他人活租约下的记录不做任何事；`Dispose(key)` 把一条非终态记录无条件结算为 Unknown 终态（OutcomeEnvelope 携带 `WireError{Code:"disposed"}`，经 record 的 `ExecutionRef` 找到 backend 后 best-effort `Cancel(ref)`，不要求 backend 可达），Owner 经下一次 GetOutcome 读取后按 RUN-CMT-7 处置。Worker 不运行扫描或定时循环，`executionstore.Store` 也不提供全量列表：数据面只有 `ListOwned(owner)`，供同 ID 重启的 Worker 找回自己持有租约的记录并恢复 heartbeat 与 watch。触发恢复的是观察到 `orphaned` 的一方。发现来源是 Run 状态而不是 execution store：`plan.RecoveryTargets` 给出 Executing 的 effect，`Attach` 给出其中 `orphaned` 的那些。Owner 侧的两个触发点（RUN-CMT-7）：`RecoverInterrupted` 的 Reconciler 对 defer 目标调用一次 `RecoverExecution`；kept 目标在等待期间由 Reconciler 按 `OrphanProbe` 间隔（默认 15s）重新 Attach，每个 orphaned 阶段再调用一次。这两处只涉及本 Owner 自己持有的 Run 的 effect，没有间隔、放弃时限之类的策略参数。重复驱动、跨 Session 巡检与放弃（`Dispose`）属于部署：云端控制器以 Session 租约过期为发现机制，`Owner.Open` 后自然进入同一条链路；本地部署没有控制器，Core 保证“有人问时能恢复”，不保证“无人问时自动恢复”。`RecoveryDisposition=deferred` 在协议层无界（反重复执行，TRN-DUR-4），上界由部署经 `Dispose` 给出。远端 Worker 经 HTTP 控制端点 `/recover`、`/dispose`、`/acknowledge` 暴露同一组原语。Execution Store 是 fencing authority：所有权终止条件是记录缺失、进入终态、或 owner/fencing epoch 被新 owner 改变；租约过期不终止所有权，heartbeat 与 watch 在短暂 store 故障下继续工作（Renew 不检查过期，恢复后续租；PutOwned 要求活租约，结算随续租恢复）。

**RUN-EXE-7（Assignment payload）** Dispatch 必须携带内联 payload：模型 Assignment 的 `ModelRequest` 随 Assignment 内联，Executor 派生 request digest 并校验 `RequestDigest` 与 ModelRef 一致后才接受，接受时把完整 Assignment 持久化进 Execution Record；`Attach`/`GetStatus`/`GetOutcome`/`Cancel` 只按 AssignmentKey 定位记录，从不读取 payload。digest-only 的模型 Assignment 只允许出现在 Owner 内部的 `AssignmentFromTarget` 重建（RUN-CMT-7 的 Attach 询问），不得进入 Dispatch：缺少内联请求体的 Dispatch 是确定的 acceptance 拒绝，效果未开始。`frozen.Store` 仍是 Owner 侧的持久旁存（RUN-WIR-4）；执行 payload 的生命周期由 Execution Record 管理，Executor 在执行时不反查 Authority 的 `frozen.Store`。

**RUN-EXE-8（部署说明）** v1 假设 worker 池同构：池内全部节点服务同一 Catalog（同一 ModelRef、ToolRef 集合与定义 digest），Assignment 的 definition digest 校验在同构池上恒通过，异构池上转为确定性拒绝；跨异构池的放置由 application 路由，协议不规定。数据面与控制面只面向 loopback 同机信任域：协议层的线上身份只有 AssignmentKey 的 EffectID（RUN-EXE-1）与 Execution Store 的 owner/fencing epoch，无认证机制；跨机器部署由 application 在信任域边界提供传输保护，协议不规定。v1 不规定推送通道：Owner 侧统一经 GetOutcome 长轮询读取结果，deployment 层的通知只作为唤醒读取方的优化（RUN-EXE-2），不改变读取语义。colocated 部署同样经 Worker 与 record store 运行效果：`app.Build` 的本地模式组装 `executor.NewWorker(ctx, records, routes)`，record store 由 `Config.Executions` 显式提供（`agent/store/sqlite` 的 SQLite 实现，没有内存实现，OWN-PRT-3）；`LocalExecutor` 是 local provider 的 Backend 实现，不实现 `effect.Port`。Worker 的 goroutine（每条 record 的 heartbeat 与 watch）由 `Worker.Close` 统一停止并等待，`Application.Close` 关闭 `Build` 组装的 Worker；`effect.Port` 不带 Close，Worker 的所有权在组装它的一方。本地与远端部署之间没有分叉的生命周期代码，差别只在 record store 与 backend 表。

**RUN-EXE-9（ExecutionRef）** `ExecutionRef{Provider, Ref}` 是 Executor 为一个 effect 的 attempt 建立的物理绑定。Provider 命名 backend（`local`、`twilight/session`，部署自定的 provider 名），Ref 是该 backend 内的不透明句柄。它在 `Backend.Prepare` 时确定，在 Start 之前随 record 持久化。Prepare 与 Restart 是两个契约：`Prepare(a)` 按 AssignmentKey 幂等且确定——同一 key 重复 Prepare 返回同一 Ref，进程死在 Prepare 与 record 写入之间也恢复同一物理绑定（派生式 backend 直接计算：local 以编码后的 AssignmentKey 为 Ref；分配式 backend 以 key 为幂等键记录分配结果）；`Restart(previous, a)` 在接管发现 backend 对 previous 报 `missing` 后分配同一 effect 下一代 attempt 的 Ref，不要求与 previous 相同（local 以 `#<generation>` 后缀派生新 Ref）。Worker 对模型与工具 Assignment 都调用 Restart：模型总是重放；工具由 Worker 依据 Assignment 携带的 `Replay` 裁决，不询问 Backend：该值源于工具实现的 `Replay()` 声明，经 `PublicTool`（进 preset 摘要）→ `ToolSpec` → `ToolCallBinding` / `ToolCallState`（Run 事实）→ `ToolAssignment`（wire 与 record）到达每个 Worker，本地与远端 Worker 因此对同一 record 得到同一裁决；`ReplayAllowed` 按模型同样重派，`ReplayForbidden` 与零值 `ReplayUnknown` 结算 Unknown（`adopted_without_replay`，TRN-DUR-4），message 记录声明值，未判断的工具因此在审计中可辨。`Replay` 不进入 BindingDigest 与 DefinitionDigest（它是执行提示，不是 call 身份）；Backend 的 `Validate` 核对 Assignment 的 `Replay` 与实现声明一致，不一致为 definition mismatch（与 response policy 同样处理）。子代理工具不经效果层：它以 ExternalResponse 等待 Owner 侧 Responder 的应答（SPN-1、DRV-4）。重派时把被替代的 `ExecutionRef` 追加到 record 的 `Superseded`（最旧在前）再写入新 Ref，审计保留该 effect 绑定过的每一代 attempt；RUN-EXE-11 的 retry 走同一 Restart 与同一审计。

**RUN-EXE-11（失败分类与重试）** Replay 与重试是两个独立的问题。Replay 是工具级能力（RUN-EXE-9）：旧 attempt 的结果丢失时重新执行是否安全。重试处置是失败级属性：对一次已经知道原因的 Known 失败，是否允许执行同一 Assignment 的下一 attempt。`RetryAllowed` 的含义不是"这个错误看起来是瞬时的"，而是：当前 Known 失败已经足以确认本次 attempt 没有产生不能安全重复的外部效果。由此形成三分：`Known + RetryNever`，已知失败，不再执行；`Known + RetryAllowed`，已知失败，且确认可以重新执行；`Unknown`，不知道效果是否发生，进入 Replay 与 reconciliation 语义（RUN-CMT-7、TRN-DUR-4）。一个扣款工具超时不能因为类别是 `timeout` 就报 `ToolExecutionFailed{Retry: RetryAllowed}`：扣款可能已经发生，正确的回答是 `ToolExecutionUnknown`，除非工具自己的幂等机制能证明重复执行安全。同一个工具因此不存在统一的可重试性，没有工具级的 `Retry()` 声明。模型调用是效果调用的范本：模型调用对外部世界没有效果，Known 失败从不留下可被重复的东西，唯一的问题是失败会不会过去。effect 层先把 provider 错误分类为 `FailureCode`（`effect.ClassifyModelError`：按 `sdk.HTTPStatusError` 的 HTTP 状态与传输错误映射），处置只从类别派生（`FailureCode.Retry()`：`rate_limited`、`provider_unavailable`、`connection_failed` 为 RetryAllowed，其余含未分类的 `executor_error` 为 RetryNever），`ModelFailed{Code, Message}` 不存储处置，wire 上也不携带，因此不可能出现 `Code` 与处置不一致的表示。工具调用照同一形状：`ToolFailure.Class` 用工具执行失败类别（`not_found`、`invalid_input`、`timeout`、`unavailable`、`rate_limited`、`conflict`、`internal`，未分类为 `execution_failed`）描述错误，`ToolExecutionFailed{Failure, Retry}` 由工具为这一次失败声明处置并经 wire（`ToolOutcomeEnvelope.retry`）携带，因为工具失败的安全性协议层不知道；未声明的零值 RetryUnknown 不重试，协议层不从类别推断。Worker 对两种失败用同一条规则：读到处置为 RetryAllowed 的 Known 失败且尝试次数（`len(Superseded)+1`）未达 `WorkerOptions.Retry.MaxAttempts` 时，按 `Backoff × 已尝试次数` 等待（等待期间持续核对租约），经 `Backend.Restart` 取下一代 Ref、旧 Ref 进 `Superseded`、`Start` 后继续观察同一 record；Restart 返回同一 Ref 的 Backend（PortBackend）不能重发，按原失败结算。预算是部署配置，零值关闭重试；Run 只看到该 effect 的最终 Outcome，重试不进入 Run 事实。

**RUN-EXE-12（进度帧）** 效果执行中的临时观察经 effect 端口的进度侧到达 Owner：`ProgressPort.Progress(ctx, key, after, fn)` 按 AssignmentKey 与起始 Sequence 顺序交付 `ProgressFrame{Key, Generation, Sequence, Kind, Payload}`，直到 fn 停止、ctx 结束或 `end` 帧关闭该流；未见过的 key 为 `ErrExecutionNotFound`。Kind 有 `model_text_delta`、`model_reasoning_delta`、`tool_progress`、`reset`、`end`。帧不是事实：Worker 只在内存环形缓冲中保留每个 key 最近的一段（`ProgressHub`，默认 256 帧），被淘汰的帧丢失，订阅者由 Sequence 的空洞得知；Worker 重启后从头开始。backend 经 `ProgressSink.Publish` 发出帧（LocalExecutor 的模型 delta 与工具 progress），hub 盖上该 key 的 Generation 与 Sequence。Worker 每次为同一 effect 起下一代执行（接管重派或 RUN-EXE-11 的重试）先发一帧 `reset` 并使 Generation 加一：接收方丢弃此前该 effect 的全部帧并重新开始；record 进入终态时发 `end`。Outcome 不变：它仍是每个 Assignment 至多一个的原子终态回答，进度流只提示读取方去 GetOutcome。HTTP 以 `POST /progress` 暴露为 server-sent events，每帧一行 `data: <json>`；PortBackend 把内层 port 的进度原样中继，因此一条 Worker 链交付的是真正运行该效果的 Worker 的帧。`ProgressPort` 是可选能力，不实现它的 port 没有进度。

**RUN-EXE-13（结算确认）** 回收由 Owner 的确认驱动：Loop 每次结算 commit 成功后，对实现 `effect.Acknowledger` 的 Executor 调用 `Acknowledge(key)`（Worker 直接实现；HTTP Client 经 `/acknowledge`）；对非终态 record 的确认为 `ErrStateConflict`（HTTP 409），无 record 为 `ErrExecutionNotFound`，确认失败不影响结算。`Acknowledge` 提交 `outcome_acknowledged`，折叠把它记为一个事实（`Acknowledged`），Assignment、Outcome 与 `Superseded` 仍在折叠里：折叠只表达事实，不承担回收。正文的物理回收待正文进入 cas 后经 retention claim 完成（RUN-EXE-14 待定项）；没有按时间回收的兜底，按龄回收属于部署的运维操作。已确认的 record 对 `Attach` 为 `terminal`、对 `GetStatus` 为其终态、对 `GetOutcome` 为 `ErrOutcomeCollected`（包装 `ErrOutcomeUnavailable`，Reconciler 视为确定答案；HTTP 为 410，Client 还原为 `ErrOutcomeUnavailable`）：Outcome 已进入 Session，不再由 Executor 交付。同一 key 的 Dispatch 重放确认已有 acceptance 且不启动任何执行。执行中的 record 不能确认。record 本身不删除。frozen 正文的回收不在本条范围内。

**RUN-EXE-14（Execution Ledger）** Executor 是与 Session 并列的第二个 event-sourced authority，两者只以 AssignmentKey 互相指认（RUN-EXE-1）。每个 effect 一条 ledger（`agentcore/executor/store`），词汇与 Session kernel 一致（`agentcore/ledger`）：commit 携带 `Seq`、`CommitID` 与事件列表，事件为 `Type`、`RecordedAtUnixMilli`、canonical `Payload`；`Head{Next}` 为 ledger 的尖端。事实类型：`execution_accepted`（Assignment，由 Dispatch 打开的 ledger 的首个事实）、`execution_aborted`（tombstone，由 `Abort` 打开的 ledger 的唯一事实，与 `execution_accepted` 争 Seq 0，RUN-EXE-16）、`execution_bound`（`ExecutionRef`）、`execution_claimed`（Owner 与新 Epoch）、`execution_started`（进入 Dispatching）、`execution_running`、`cancel_requested`、`execution_restarted`（被替代的 Ref 与新 Ref）、`execution_settled`（终态与 Outcome）、`outcome_acknowledged`。`ExecutionState` 是事实的折叠，只含 Assignment、`ExecutionRef`、`Superseded`、State、Outcome 与 `Acknowledged`；`Load` 在同一事务里读折叠与租约行，返回 `Execution{ExecutionState, Lease}`，Owner、Epoch 与到期时刻只来自租约行，`execution_claimed` 事实只是出处。`Attach`、`GetStatus`、`GetOutcome` 读 `Execution`。三条规则与 Session 相同：`Append` 的 `Seq` 必须等于 `Head.Next`（`ErrConflict`，写者重读再决定）；同一 `CommitID` 再次提交为 `ErrAlreadyApplied`（视为成功），store 不比对内容，Worker 的 `Dispatch` 在得到 `ErrAlreadyApplied` 或 `ErrConflict` 后读回折叠、现算已接受 Assignment 的 digest 与本次比较，不同为 `ErrAssignmentConflict`（ledger 不另存该 digest，Assignment 本身是唯一权威）；每个 commit 在写入前折叠，不合法的状态迁移为 `ErrStateConflict`，ledger 因此不含非法步骤。命令 identity 由 executor store 自己以 key 与命令名派生（`agentcore/ledger` 只保证唯一性，不规定派生方式）：接受、关闭、结算、确认各一次（`AcceptCommitID`、`AbortCommitID`、`SettleCommitID`、`AcknowledgeCommitID`；接受与关闭的 identity 不同，两者靠 Seq 0 互斥而非靠重放合并），结算共用一个 identity，因此租约持有者的结算与控制器的 `Dispose` 争同一身份、后者读到前者的 Outcome；按 Epoch 或代际重复的命令（claim、bind、start、restart）以 Epoch 或被替代的 Ref 为判别项。fence：`execution_bound`、`execution_accepted`、`execution_settled`、`outcome_acknowledged` 之外的事件只能在 key 的当前租约下提交（`Append` 携带 `Lease`，store 校验 owner、Epoch 与未过期，`ErrLeaseLost`）；租约本身是单独一行（owner、Epoch、到期），是 fence 的 authority，`Acquire` 在同一事务里写租约行并追加 `execution_claimed`，续期只改租约行不产生事实，与 Session 的 SES-OWN-1 同一处理。`Read(key, from)` 按 Seq 返回 commit 与 Head，是恢复、审计与后续跨 authority 订阅的读取原语。待定项：Assignment 正文与 Outcome 目前内联于事实，进入 cas 并按 digest 引用后，`outcome_acknowledged` 释放 retention claim 即完成 RUN-EXE-13 的物理回收。

**RUN-EXE-15（重派与 dispatch ledger）** Session 与 Executor 之间没有共享事务：Loop 提交 start 事实后向 Executor `Dispatch`，两步之间的崩溃留下一个 Session 已记录为 Executing、Executor 从未收到的 effect。这个空窗由接管处置处理（RUN-CMT-7）：Reconciler 就是两个 authority 之间的 process manager，它读三处状态——Run 机器给出 Executing 的 effect（`plan.RecoveryTargets`，带 EffectID），Executor 的 `Attach` 给出 attempt 是否存在，dispatch ledger（`agentcore/process`）给出该 effect 决定过第几次重派、该次是否已送达 Executor、是否放弃。三者各是自己事实的唯一来源，process 不转录另外两方的观察。dispatch ledger 每个 effect 一条，事实有三种：`dispatch_planned{attempt}`（attempt 从 1 连续编号，前一次必须已 dispatched）、`dispatched{attempt}`（该次 Dispatch 已到达 Executor：被接受或 Executor 已持有同 key）与 `given_up{reason}`；折叠为 `{Planned, Dispatched, GivenUp, Reason}`，`Planned` 等于 `Dispatched` 或 `Dispatched+1`，差值即仍欠 Executor 的那一次（`State.Pending()`）；ledger 规则与 RUN-EXE-14 相同（Seq、CommitID 重放、写前折叠），fence 为 Session owner 的 Epoch（`Append(epoch, key, commit)`，更小的 epoch 为 `ErrFenced`）。对 `missing` 的判定：已放弃则处置；没有欠付的 attempt 且 `Planned` 达到预算（`MaxRedispatches`，默认 3）则记 `given_up` 并处置；否则取欠付的 attempt，没有则记 `dispatch_planned` 开新一次（`process.Plan`），再经 `loop.Redispatch` 从 Run 状态重建 Assignment 交给 Executor（Executor 按 key 识别重放，RUN-EXE-3），成功后记 `dispatched`（`process.MarkDispatched`）。先记后派、派后再记：记录与 Dispatch 之间的崩溃留下一次欠付的 attempt，下一次接管处置重做同一次而不开新的一次，不消耗预算，也不会有 ledger 不知道的 Dispatch。`Redispatch` 成功为 verdict `redispatch`，目标保持 Executing 并像 `keep` 一样等待 Outcome；`ErrDispatchRetryable` 或 `ErrDispatchUnknown` 为 `defer`，该次 attempt 保持欠付，下一次接管处置重做；其他错误（含 `ErrEffectNotExecuting`）记 `given_up` 并处置。是否重派由 `Reconciler.Missing`（`reconcile.MissingPolicy`）显式给出：`DisposeMissing`（零值）不需要 dispatch ledger，`missing` 只处置；`RedispatchMissing` 要求 `Redispatch` 端口与 dispatch ledger 同时装配，否则 Plan 返回 `ErrMissingPolicyPorts`（RUN-CMT-7）。local agent 与 cloud agent 运行同一个 Reconciler，差别只在 port：dispatch ledger 在本地 SQLite 文件或共享数据库，Executor 是进程内 Worker 或 HTTP 远端。

**RUN-EXE-16（Abort tombstone：同一 EffectID 的接受与关闭互斥）** `Attach` 的 `missing` 只证明 Executor 此刻没有该 key 的 record，不能证明之后不会有：旧 owner 迟到的 Dispatch、仍在网络中的重发，都可能在新 owner 已按 `missing` 处置之后到达并被接受，使 Run 已经关闭的 effect 真实执行一次；Session Epoch 只 fence Run 的 commit，Execution Store 的租约只 fence 已有 record 的后续写入，两者都不约束一条新 ledger 的打开。因此 Execution Store 同时是该 EffectID 的 inbox 与生命周期 authority：对一个 key，`Dispatch` 写入的 `execution_accepted` 与 `Abort` 写入的 `execution_aborted` 争同一条 ledger 的 Seq 0，store 的 Seq 规则保证恰有一个成立。`ExecutionPort.Abort(key)` 返回关闭后的 attachment：tombstone 成立（本次写入或此前已存在）为 `aborted`；一次 acceptance 抢先则返回它的实时状态（`active`/`orphaned`/`terminal`），调用方不得处置。`aborted` 的 key：`Dispatch` 为确定拒绝 `ErrExecutionAborted`（HTTP 409，客户端归为普通 error，Loop 按 Known 失败、Reconciler 的重派按明确拒绝记 `given_up`）；`Attach` 为 `aborted`；`GetOutcome` 为 `ErrExecutionAborted`（包装 `ErrOutcomeUnavailable`，Reconciler 视为确定答案，HTTP 410）；`Acknowledge`、`RecoverExecution`、`Cancel` 为无操作。tombstone 只有一条事实、不回收，与 RUN-EXE-13 一致。接管处置的顺序固定：对 `missing` 的目标先 `Abort`，得到 `aborted` 后才提交 Run 的恢复 command（RUN-CMT-7）；`RedispatchMissing` 的重派走 `Dispatch` 即 `AcceptIfMissing`，不写 tombstone，只有最终处置才写。`Reconciler.Abandon` 是调用方声明该 Scope 根本没有 Executor 在服务：既没有任何 attempt 被持有，也不会有迟到的 Dispatch 可被接受，所以不调用 `Abort`。它只适用于没有 execution 端口的场景（conformance 测试、Executor 已不存在的 Session 离线修复）；`Abandon` 与非 nil 的 `Executions` 同时设置为配置错误，Plan 返回 `ErrAbandonWithExecutor`，因此有 Executor 的正常路径无法绕过 `Abort`。Dispatch 不携带 Session Epoch：旧 owner 无法再提交新的 start 事实，其迟到的 Dispatch 只可能命中新 owner 必然会 reconcile 的 effect，tombstone 已覆盖这一类；按 Epoch 拒绝 Dispatch 是可选的后续加固。

**RUN-EXE-17（读取与通知分离）** `GetOutcome` 与"等待结算发生"是两个原语。读取：`GetOutcome` 折叠一次 ledger 并立即返回，未结算为 `ErrOutcomeNotReady`、已结算为 Outcome、无 ledger 为 `ErrExecutionNotFound`、永不可读为 `ErrOutcomeUnavailable`；它是幂等的，任何路径可以随时重放。通知：`effect.SettlementPort.Settlements(ctx, epoch, after, fn)` 是可选端口能力（与 `ProgressPort`、`Recoverer` 同一模式），Worker 每写入一条结算事实后在进程内 `SettlementHub` 记一条 `Settlement{Key, Epoch, Sequence}`，订阅者按 Sequence 顺序收到该 Worker 结算的全部 key 的通知；通知不携带 Outcome，收到后经 `GetOutcome` 读取。Epoch 标识 Worker 的一次 incarnation，Sequence 在其中递增；hub 只保留有界窗口（默认 4096 条），请求已淘汰的 Sequence 为 `ErrSettlementsEvicted`，未知 epoch 从当前头部订阅；两种情况下订阅者都重读它等待的每个 key，因此通知丢失不会造成结算丢失，流与读取互补而不需要协调。HTTP 以 `POST /settlements` 暴露为 server-sent events（410 为 evicted），Client 实现 `SettlementPort`；`/outcome` 对未结算回答 204。Owner 侧以 `effect.Watcher` 消费：每个 (owner, executor) 一条订阅加周期性读取（默认 30s），Loop 的阻塞驱动与 Reconciler 的 kept 目标都向同一个 Watcher 登记 key（`Reconciler.Watcher`、`loop.Settings.Watcher`，Driver 为二者提供同一个，`Driver.OutcomeWatcher()` 把它交给宿主组件），等待 N 个 effect 的代价是一条连接与 N 个登记项，没有一个请求在执行期间保持打开。单个 effect 的同步调用方（compaction 的 Summarizer）用同一个 Watcher 的 `Await(ctx, key)`，不另开订阅。同一模型贯穿 Worker 到 Backend 这一跳：`ExecutionBackend.Outcome` 是一次读取（未结算为 `ErrOutcomeNotReady`），Backend 可选实现 `notice.Source`，以按 Ref 的 `notice.Ref{Ref, Epoch, Sequence}` 通知何时去读；Worker 对每个 Backend 保持一条 `Settled` 订阅（`backendNotices`），`observe` 读一次后等待该 Ref 的通知，通知流未建立或 Backend 无通知源时按不超过 1 秒的间隔重读，建立后按租约间隔兜底，每次等待都重新核对所有权；读取得到 `ErrExecutionNotFound` 或 `ErrOutcomeUnavailable` 为确定答案，record 以 Unknown（`outcome_unavailable`）结算而不再等待。`LocalExecutor` 以进程内 `notice.RefHub` 实现 `Source`，`PortBackend` 把远端 `SettlementPort` 的 key 通知转为 Ref 通知转发，`backendhttp.Client` 以 `/notices` 流实现。全部通知源共用一个 `notice.Ring`：Worker 的 `SettlementHub` 与 Backend 的 `RefHub` 都是它的实例，协议里只有一个 ring 的实现。每次订阅建立时流先送出一帧公告（无主体：`Settlement.Key` 为零值、`notice.Ref.Ref` 为空），携带该 incarnation 的 epoch 与当前 head；带着旧 epoch 位置重连的订阅者由此得知换了 incarnation、订阅之前的通知不会重放，Watcher 据此立刻重读全部登记 key（而不是等 `Poll`），Worker 的 `backendNotices` 据此把通知流标为已建立。Watcher 还按 `Probe`（默认 `DefaultWatchProbe`=15s）对登记后仍未结算的 key 调用 `Attach`：`orphaned`（持有租约的 Worker 已死且租约过期）时经 `Recoverer.RecoverExecution` 交给应答的 Worker 接管，`missing` 与其他状态不处理；这是 owner 存活期间 worker 被替换时 live drive 的恢复路径，与接管处置里 Reconciler 的 `OrphanProbe` 是同一个动作的两个触发点（`driver.Driver.OrphanProbe` 统一配置）。

**RUN-EXE-10（Backend 选择与 ledger 的权威性）** backend 选择是 execution 创建的一部分：Worker 在 Dispatch 时按 `Route` 表评估一次（第一个 `Match` 为真的 provider，`Match` 为 nil 的 route 接受全部），结果以 `execution_bound` 事实持久化；此后 Attach、GetStatus、GetOutcome、Cancel、RecoverExecution、Dispose 只读折叠并按 Provider 找 backend，不再评估 Assignment 内容，也不询问任何 backend 是否认识某个 key；record 的 Provider 在本 Worker 没有对应 backend 时为 `ErrUnknownProvider`，record 不被改动。ledger 是 execution identity 的唯一来源：ledger 缺失即 execution 不存在（`missing`，RUN-EXE-3）。ledger store 是 durable 的（`agent/store/sqlite`，一个 SQLite 文件同时承载 execution ledger 与租约、artifact Binding 与 retention claim，事务提供跨进程互斥），没有内存实现；崩溃重启后 ledger 仍在，跨进程收养经 `RecoverExecution` 完成（RUN-EXE-6）。`Validate` 按同一 route 表选择 backend 但不持久化选择。

```go
type EffectContext struct { Session run.Scope; RunID run.RunID; StepID run.StepID; CallID run.CallID; Effect run.EffectID; Kind AssignmentKind; Tool run.ToolRef } // 待解析 target 的 effect 坐标
type TargetResolver interface { ResolveTarget(context.Context, EffectContext) (*run.TargetRef, error) } // 每个 effect 调用一次（RUN-LOP-9）
type Settings struct {
    Scheduling       run.ToolScheduling // 来自 AgentPreset：工具调用并行/串行与并发上限
    MalformedRetries uint8              // 来自 AgentPreset：畸形模型结果的重试上限
    TargetResolver   TargetResolver     // application 提供的 opaque target 解析器，按 effect 调用（RUN-LOP-9）
    BeforePrepare    PrepareHook        // Run 处于 Open、Prepare 之前的应用钩子（RUN-LOP-10）
}
type PrepareHook func(ctx context.Context, store runtime.RunStore, input plan.PromptInput) error
type LoopResult struct {
    Disposition LoopDisposition // LoopWaiting | LoopFinished | LoopDispatched | LoopDelivered | LoopDropped
    Reason WaitReason           // 仅 ExecutionRecovery 时为 execution_recovery；否则为空
    ExecutionRecovery bool
    Result *run.RunResult
    Dispatched []AssignmentKey  // Advance 交给 Executor 的 Assignment
}
func New(exec Executor, builder PromptBuilder, settings Settings) (*Loop, error)
func (*Loop) Advance(context.Context, runtime.RunStore, run.RunID, EventSink) (LoopResult, error)
func (*Loop) Deliver(context.Context, runtime.RunStore, Outcome, EventSink) (LoopResult, error)
func (*Loop) Run(context.Context, runtime.RunStore, run.RunID, EventSink) (LoopResult, error) // 阻塞封装；RunStore 由 SessionRunStore.Bind(w) 绑定
```

**RUN-LOP-1** `Settings` 是 Loop 从 AgentPreset 取得的执行参数（TRN-PST-1），不是独立的可插拔组件。`Scheduling` 在 `SubmitModelResult` 时写入 `ToolStepOpened.Scheduling` 并冻结在该 ToolStep 上；后续 Loop 必须按冻结值调度，不得改用当时进程的 Settings。未指定 Mode 时冻结为 `parallel`，`MaxParallel` 零值表示当前 Start 批次全部 Pending call 可并行。空 Mode 按 parallel 解释，不得在 normalize 时填入默认字符串。畸形模型结果的处置由 `MalformedRetries` 决定：该 ModelStep 已记录的 `Rejects` 少于该值时选择 `ModelRejectRetry`，否则 `ModelRejectFailRun`；零即首次失败。`streaming` 表示是否请求可用的流式模型端口；两种模式都产生同一完整 `sdk.ModelResult`。Loop 不管理 Executor lease 或 Worker heartbeat；这些属于 Executor/control plane。Loop 只以 Run 记录的 Effect 定位与结算执行，并通过 Session Writer 完成语义 settlement（RUN-CMT-6）。

**RUN-LOP-7** `ModelRef` 是冻结请求中的执行身份。`ModelCatalog.ResolveModel` 在同一 Run 生命周期内必须把同一 `ModelRef` 解析为等价的执行语义。provider 绑定不进入 frozen request，因此 Catalog 不得把同一 ref 改绑到不同实现。

**RUN-LOP-10（Prepare 之前的钩子）** `Settings.BeforePrepare` 在每次 `Next` 返回 `NeedModelRequest` 时、PromptBuilder 读取上下文之前调用一次，传入 Loop 绑定的 `RunStore` 与将交给 PromptBuilder 的 `PromptInput`。它是应用在两步之间改写上下文的位置（Turn 内 compaction，APP-CKP-1）：钩子经同一 Writer 提交的事实是随后 Build 读到的状态；它不写 Run 事实，因此 snapshot 的 Position 对随后的 Prepare 仍然有效。钩子返回错误时驱动停止，不写入任何事实；nil 为无钩子。Loop 不解释钩子做了什么。

**RUN-LOP-9（target 解析）** `TargetResolver` 按 effect 调用：Loop 在每个 model effect 与每个 tool call 的 effect 进入 start barrier 之前调用一次 `ResolveTarget`，传入该 effect 的坐标 `EffectContext{Session, RunID, StepID, CallID, Effect, Kind, Tool, Placement}`，其中 `Effect` 是该 effect 启动时使用的 EffectID，`Placement` 是工具的 placement 声明（model effect 为零值）。解析器只对 `Kind == tool` 且 `Placement == PlacementWorkspace` 的 effect 解析 workspace target；Worker 的 Route 表按 `ToolAssignment.Placement` 选 backend，不按 target 是否存在。返回值复制进该 effect 的 Assignment（tool effect 的 Validate probe 携带同一 target），Loop 不解释它。nil 返回值表示该 effect 没有资源 target；`Kind` 或 `ID` 为空的返回值是错误。解析器返回错误时该 effect 不启动，Run 不写入任何事实：ModelStep 保持 Prepared，tool call 保持 Pending。同一 Run 内的不同 effect 可以解析到不同 target。target 不进入 Run 事实，只存在于 Assignment 与 Execution Record 中；core 没有 target 事实也没有默认解析器，解析器及其 Session 到资源的映射属于 application 的资源层，映射的持久性由 application 保证（APP-TGT-1、agent-workspace.md）。

`LoopResult` 的语义固定为：`LoopWaiting` 时 `Result` 为 nil，表示没有可执行 action、Run 仍为 active。`ExecutionRecovery` 等于 `plan.NeedsRecovery(state)`。该值为 true 表示存在本进程未派发其 effect 的 Executing 目标（只在崩溃后、接管处置之前出现），`Reason` 为 `execution_recovery`；否则 `Reason` 为空。Waiting call 不进入 `LoopResult`；Application 通过投影状态上的 `plan.WaitingCalls` 读取。`LoopFinished` 时 `Result` 非 nil，并等于 terminal Run 的 `RunResult`。

`PromptBuilder` 从 `PromptInput` 接收 Run 边界事实；它从 Session 的 chatlog fold 读取对话结构（上一步的 assistant 与 tool_result 条目由 Run fact 折叠得到）并经 materializer 取回正文，并使用自己注入的 memory、attachments 与 product policy 组装 `sdk.Request`。Loop 冻结 prompt builder 返回的 request，RunStore 校验其 digest，PromptBuilder 管理 application context。

## 7. Loop execution

```text
Loop.Advance(ctx, runtime, sessionID, runID, sink):        // 不等待任何效果
  repeat:
    snapshot = store.Load(runID)            // store: SessionRunStore.Bind(w) 的 runtime.RunStore
    if terminal: emit observational run_finished; return Finished(snapshot.Result)
    action = plan.Next(snapshot.State)
    NeedModelRequest  → Plan、Freeze、Commit Prepare；continue
    WithdrawPrepared  → Commit Withdraw；continue
    StartModelCall    → Commit StartModelExecution{Effect}；Executor.Dispatch(Assignment{model})；return Dispatched
    StartToolCalls    → 对冻结 Scheduling 允许的每个 Pending call：Validate → 失败则 Commit DeclineToolCall；
                        否则 Commit StartToolCall{Effect}，Executor.Dispatch(Assignment{tool})；return Dispatched
    Idle              → return Waiting（NeedsRecovery 设 ExecutionRecovery）

Loop.Deliver(ctx, runtime, sessionID, outcome, sink):      // Outcome 到达时，来自任何地方
  snapshot = store.Load
  目标不再 Executing 或 Effect 不符 → return Dropped（不写）
  Commit Submit*{Effect: outcome.Key.Effect}（以该 Effect 派生结算 CommandID）
  终态 → emit run_finished；return Finished   否则 return Delivered（宿主接着 Advance）

Loop.Run(...):  // 阻塞封装：Advance → 等待本次 dispatch 的 Outcome → Deliver → Advance，直到 Waiting 或 Finished
```

模型结算（无 tool call 的 SubmitModelResult、SubmitModelFailure、FailRun 的 RejectModelResult）可能终结 Run；此时 CommitResult.Snapshot 已是终态，Loop 直接 emit run_finished 并返回 Finished，不再 Load。工具结算不会终结 Run。

每个 `Loop` 实例为每个 `(SessionID, RunID)` 分配一个本地 slot：同一 Run 的 `Advance` 与 `Deliver` 串行；`Run` 进行期间对同一 Run 的 `Advance` 或第二个 `Run` 返回 `ErrRunAlreadyRunning`；不同 Run 可以并行驱动。slot 只在有 `Advance`、`Deliver` 或 `Run` 持有它时存在，最后一个持有者返回后释放，因此 slot 表的大小随正在驱动的 Run 数变化，与 Loop 见过的 Run 总数无关。宿主必须保证一个 Session 在一个进程内只有一个 Loop 实例驱动它的 Run（与 `Writer` 一一对应）。

**RUN-LOP-2** `NeedModelRequest` 调用 PromptBuilder，冻结 sdk.Request，验证 model、ordered InputIDs 与 ToolSpecs，计算 request digest 和 derived CommandID/StepID，再提交 Prepare（command 携带本体）。prepare stale 后重新 Load；同 Position 的内容拒绝不得 livelock 重试。业务停止统一使用 `CancelRun`。

**RUN-LOP-8** `WithdrawPrepared` 时 Loop 提交 `WithdrawPreparedStep{StepID}`，随后重新 Load；被放弃请求的本体在 `frozen.Store` 中可立即释放。Loop 不为输入做任何其他事：Executing 与 ToolStep 期间到达的输入留在 `PendingInputs`，由随后 `Open` 的 `NeedModelRequest` 经 `PromptInput.Inputs` 交给 PromptBuilder。

**RUN-LOP-3** `StartModelCall` 在 Validate 成功后 Commit start barrier；`CommitAccepted` 或同 Effect 重放的 `CommitAlreadyApplied` 授予该 effect 的执行。Loop 将 Assignment 交给 Executor，冻结的 ModelRequest 在 Dispatch 时成为 Executor-owned payload。start fact 保存 Effect，Outcome 据此定位目标（RUN-EXE-4）。streaming 与 non-streaming 均产生完整 `sdk.ModelResult`，delta 发往 EventSink。

Validate 发现模型不可用时返回 `ErrModelUnavailable`，step 保持 Prepared。确定的 Dispatch 拒绝使 Loop 提交 `RecoverModelExecution` 并返回错误；`ErrDispatchRetryable` 先在同一 Advance 内有界重发，仍被拒绝时同上；`ErrDispatchUnknown` 保留 Executing、等待实际 Outcome（RUN-EXE-3）。Outcome 中的 provider 失败提交 `SubmitModelFailure`；Cancelled 与 Unknown（`Dispose`、接管未能收养，RUN-EXE-6）提交 `RecoverModelExecution` 回到 Open，与恢复路径对 missing 的处置同口径（RUN-CMT-7）；结构、binding 或 freeze 失败提交 `RejectModelResult`，按调用方 disposition 重试或结束；成功结果提交 `SubmitModelResult`。

**RUN-LOP-4** Tool execution 先经 `Executor.Validate` 按 frozen binding 验证 Ref、definition digest、response policy 与 arguments（RUN-EXE-5）。校验失败在 Pending 状态提交 `DeclineToolCall`（RUN-EXE-5）；通过后逐 call 提交 `StartToolCall{Effect}`，再 Dispatch Assignment。确定拒绝派发以该 effect 的 `SubmitToolFailure(Known)` 结算；`ErrDispatchUnknown` 保留 Executing 并等待实际 Outcome（RUN-EXE-3）。Executing 由当前执行者或接管流程结算，已终态 call 的结果保持稳定（TRN-DUR-4）。

一次 Advance 按冻结的 `ToolStep.Scheduling`（parallel / sequential、MaxParallel）分批 Start 与 Dispatch Pending call，每个 Outcome 经 Deliver、以原 Effect 派生的 CommandID 独立提交。外层 ctx 取消后停止新增 Start，已派发的执行经 Outcome 结算。`Next=Idle` 对应 `LoopWaiting`，`ExecutionRecovery` 取自 `NeedsRecovery(state)`。Application 从 `WaitingCalls` 读取请求，批准、拒绝或提交外部响应后再次驱动。

tool panic 或 effect 结果无法确定时提交该 call 的 `SubmitToolFailure(Unknown)`，同批 sibling 继续、Run 保持 Active。业务取消按 RUN-MCH-2 的 `CancelRun` 规则结算所有未完成调用。已接受 start 的 worker 响应取消并返回 Outcome；结算使用独立 control context。Outcome 读取失败经协调错误通道返回，Executing 保留至实际结果或接管处置。

**RUN-LOP-5** 效果在 Executor 的上下文里运行；Loop 对已接受 effect 的结算使用独立 control context（`Deliver` 内），调用方请求的取消不能丢弃一个已发生效果的 Outcome。Application 的业务停止顺序为先 Commit `CancelRun`，再取消驱动 ctx；阻塞式 `Run` 的 ctx 取消使它调用 `Executor.Cancel`，已 dispatch 的效果各自交回 Cancelled 的 Outcome 并被结算（模型步撤回到 Open、工具按其 outcome），之后 `Run` 以 `ctx.Err()` 返回。非 sentinel Commit error 以同 CommandID 重放一次；仍未知时返回错误，由后续 Load/Record 查询 authority。stale/terminal/conflict 触发 reload/drop，旧 external effect 保持单次执行尝试。`ErrOwnershipLost` 是终止性错误：第一个被 kernel 围栏的结算使 Loop 调用 `Executor.Cancel(runID)`，其后到达的 Outcome 不再提交（提交也会被 kernel 拒绝），Loop 以该错误返回，且该错误优先于同一步内其他错误；模型步的结算遇该错误同样不做一次重放。已发生的外部 effect 由接管者按 RUN-CMT-7 询问后处置。工具实现配合 context 返回；永久阻塞由 application 处理。

Waiting call 的批准与外部结果由 Application 提交。Loop 不生成、不返回、不解释 `ResponseRequest`。Application 以投影中的 stable ResponseID、derived CommandID 与 payload/decision digest 提交 `ApproveToolCall`、`RejectToolCall` 或 `SubmitToolResponse`；随后再次运行 Loop。

## 8. EventSink 与边界

```go
type EventSink interface { Emit(context.Context, Event) error }
type Event struct {
    Session session.SessionID
    RunID run.RunID
    StepID run.StepID
    CallID run.CallID
    Sequence uint64
    Kind EventKind
    Durability EventDurability
    Payload json.RawMessage
    Committed []session.Event // EventAgentCommitted 携带本次 command 的完整组
}
```

`Sequence` 仅用于同一临时观察流内的顺序（例如 ToolProgress），从 1 开始；committed observation 的权威顺序由 Session `Seq` 表达，未提供临时序号时保持 0。

**RUN-LOP-6** EventSink 提供 realtime observation，Loop 通过序列化调用向 sink 发送事件。`EventAgentCommitted` 携带 accepted 组；text/reasoning delta、tool progress 与 `progress_reset` 来自 Executor 的进度帧（RUN-EXE-12）：`Loop.Run` 对每个已派发的 key 订阅 `ProgressPort.Progress`，把帧翻译为带 `Effect`、`Generation`、`Sequence` 的 provisional Event 发往 sink，因此 delta 无论在哪个进程产生都到达宿主的观察流；只调用 `Advance` 的宿主自行订阅。这些观察可丢失、重复或断流。sink failure 保持 Commit 结果；恢复与审计读取 Session ledger，EventSink gap 通过 ledger 对账。

`AcceptInput` 在任意非终态提交，`PendingInputs` 就是回合中途输入的队列；Loop 不解释 queue 或 steer：`Open` 时立刻 `NeedModelRequest`，Prepare 一次消费全部 pending input。Application 负责 admission；Turn 创建、attempt、中途投递与结算由 [agent-turn.md](agent-turn.md) 定义。

## 9. compatibility 与 conformance

**RUN-CMP-1** canonical digest 与 derived ID 的预映像永久冻结，任何修改都是新的 domain 而不是新版本（RUN-CMT-8）；fact 的 wire fields 变化以该事实类型的 payload 版本 `v` 发布并增加 codec，Registry 继续 decode 全部已发布版本；Evolve 只有一份，面对 codec upcast 后的当前类型。Session kernel 与 Run 协议都没有版本号（SES-VER-2、RUN-CMT-8）。

**RUN-CMP-2** SessionRunStore conformance 只断言 Run 模块自己的语义；组原子性、所有权与 Epoch fencing、幂等索引、投影缓存复用由 Session kernel 与 Module Framework 的 conformance 覆盖（SES 第 7 节、EXT 第 7 节），本清单以引用代替重复。conformance 以 `session.Store` 为参数（`agentcore/session/run/runtimetest`），Memory 与文件 adapter 跑同一套。必须覆盖：

- 建立与寻址：Start 组建立 Run；同一 RunID 第二条 `created`（活动或已终结）被 `CreateRun` Part 以 `ErrRunExists` 拒绝且不写入；未知 RunID 的 Load、Commit、Record 返回 `ErrRunNotFound`；已终结 Run 的 Load 返回终态 snapshot 且与 Record 一致，Commit 返回 `ErrRunTerminal`（RUN-CMT-1）；`CommandEnvelope.SchemaVersion` 与该 Run 事实的版本不一致的 command 被拒绝且不可重试；
- 重放与 Base：同 CommandID 返回 `CommitAlreadyApplied` 与原组且不再 Decide；Run 已终结后对已接受 command 的重放仍返回 AlreadyApplied，新 command 返回 `ErrRunTerminal`；prepare 的 Base 不等于该 Run 的 Position 时返回 `ErrStaleRuntime`；非 Prepare command 接受零值或过期的 Base（call-local rebase）；
- 输入入队：`AcceptInput` 在 Open、Model Prepared、Model Executing、ToolStep 都被接受；Prepared 期间入队后 `Next` 返回 `WithdrawPrepared`，Withdraw 后重规划的 Prepare 包含该输入；Executing 期间入队的输入在无 tool call 的 `SubmitModelResult` 后使 Run 回到 Open 而不结束；
- start 与 effect：同 Effect 的 start 重放返回 AlreadyApplied；不同 Effect 的 start 在 target 已是 Executing 时返回 `ErrStaleRuntime`；缺少 Effect 的 start 或 settlement 返回 `ErrCommandConflict`；同一 effect 的 settlement 携带该 Effect 并以其派生 CommandID，重放返回 AlreadyApplied，Effect 与 Executing 目标记录的不符时返回 `ErrStaleRuntime`，CommandID 与 Effect 的派生不符时返回 `ErrCommandConflict`；
- decline：`DeclineToolCall` 以 call 坐标派生 CommandID，只写 `tool_call_failed`、不写 `tool_call_started`，被 decline 的 call 没有 Effect；重放返回 AlreadyApplied，其他 CommandID 返回 `ErrCommandConflict`，对 Executing call 的 decline 与对 Pending call 的 `SubmitToolFailure(Known)` 返回 `ErrStaleRuntime`；
- 组的组成：一 command 一组，同一 CommitID；组内只有 run 事实与同一 unit 中其他模块 Part 的事实，run 事实在前，没有对话或 Turn 的派生事件；chatlog Context 中的 assistant 条目 `ResultDigest` 等于同组 fact 记录值且 `CallIDs` 等于 `ToolStepOpened` 的 CallID，tool_result 条目 `OutputDigest` 等于 fact 记录值，两者的正文可从 `frozen.Store` 取回；另一模块的 Part 把 `twilight/run/` 事件写进其他 domain 的流被拒绝（EXT-STR-1）；同一 unit 中 chatlog Part 的 ReferencePart 经 admission，未注册 Binding 使 Commit 失败且无写入，合法 Binding 在 Append 之前建立 Active claim（EXT-WRT-3）；
- 结算返回值：`CommitResult.Snapshot` 是 Evolve 后状态；终结 Run 的结算其 `Snapshot.Status` 为终态且 `Result` 非空，与 Record 一致；
- Prepare hard CAS 只对该 Run 自己的事件敏感：同一 Session 内 chatlog、turn 或其他 Run 的写入不改变该 Run 的 Position，也不使 Prepare 失效；
- 投影：`SnapshotPolicy` 在 Run 回到 Open 或终结时写入投影缓存；终态 Run 不出现在 `Active`，投影不保留它；Record 对活动 Run 的 fold 与投影一致；非法 fact 序列使 FoldRun 报错；
- 隔离：同一 Session 内多 Run 互不影响 Position 与 Record；chatlog 与 turn 事件不影响 Run fold；全部 Run 由同一份 Decide 与 Evolve 处理（RUN-CMT-8）；
- 接管处置：关闭 Writer 后以新 Writer 打开（Epoch 加一）并调用 `RecoverInterrupted`：Executing model 被撤回，Run 回到 `Open`、`ModelSteps` 不计入该步、Executing 期间投递的输入仍在 `PendingInputs`；随后的 Prepare 产生新的 StepID 并消费这些输入，不重发原 RequestDigest；Executing tool 记 Unknown 且 chatlog Context 中该 call 的条目 status=`unknown`、无正文 digest，同 step 的 Pending 与 Waiting call 不受影响；Run 保持 Active；同一 Epoch 重复调用返回 0 且无新写入；没有 Executing 目标时返回 0；
- 接管重连：`RecoverInterrupted` 对每个 Executing 目标经 `Reconciler` 向 `ExecutionPort.Attach` 询问，AssignmentKey 携带 Scope、RunID 与 start 事实记录的 Effect；`active`、`terminal` 保持 Executing 与 Effect，随后以原 Effect 结算；`orphaned` 保持原状态并等待 Outcome，端口实现 `effect.Recoverer` 时调用一次 `RecoverExecution`；`missing` 按接管处置；`Executions` 为 nil 且未 `Abandon` 时返回 `ErrNoExecutionPort`，`Abandon` 不询问 executor 而全部处置；无 record 的 key 为 `missing`。
- 效果层：`Advance` 提交 start barrier 后把 Assignment 交给 Executor 并返回 `LoopDispatched`，不等待效果；`Deliver` 以 Key 定位 Executing 目标并结算，Run 终结时返回 `LoopFinished`；effect 已结算或处置、Effect 不符的迟到 Outcome 返回 `LoopDropped` 且不写入；`Cancelled` 的模型 Outcome 使 step 撤回到 Open；`LocalExecutor.Attach(ref)` 对执行中的 Ref 返回 `active`，完成后返回 `terminal`，无该 Ref 时返回 `missing`；超出保留上限的最早终态条目对 `Status` 返回 `ErrExecutionNotFound`、对 `Attach` 返回 `missing`，仍在上限内的条目返回 `terminal`；`Advance`、`Deliver`、`Run` 返回后 Loop 的 slot 表为空；HTTP Server 对非 POST 返回 405、对超过 `MaxBodyBytes` 的请求体返回 413、对非 JSON 请求体返回 400；GetOutcome 读取失败保持执行状态，真实结果稍后仍可结算；Dispatch 重放保持已有记录，显式 `RecoverExecution` 处理恢复；record 的 Provider 在本 Worker 无对应 backend 时返回 `ErrUnknownProvider` 且 record 不变。
- 所有权失效：绑定旧 Writer 的 RunStore 在被接管后 Commit 返回 `ErrOwnershipLost` 且 ledger 无新 commit（fencing 由 SES-OWN-2 保证，本层观察结果）；
- 执行身份（RUN-EXE-9/10）：同一 Assignment 两次 Dispatch 得到同一 record 与同一 `ExecutionRef`，第二次 Prepare 而不第二次 Start；record 的 Provider 无对应 backend 时 RecoverExecution 与 GetStatus 返回 `ErrUnknownProvider` 且 record 不变、backend 不被调用；接管时 backend `missing` 的模型 Assignment 经 Restart 取新 Ref 后 Start、旧 Ref 进入 `Superseded`、Prepare 不被再次调用，工具 Assignment 记 Unknown；backend `orphaned` 时记录保持 Running 并由接管方持有租约，不 Restart 也不 Start，backend 转为 `missing` 后按前述重派或记 Unknown、转为 `active` 后观察；Port 适配器对不能解析为 key 的 Ref 返回错误而非 `missing`；`Worker.Close` 在有 watcher 在途时返回且 record 非终态；匹配 spawn 工具的 Assignment 落到 `twilight/session` provider，其余落到默认 provider；已确认的终态 record 随确认回收，对 Attach 为 `terminal`、对 GetOutcome 为 `ErrOutcomeCollected`，同一 key 的 Dispatch 不再执行且 digest 不同为冲突，未确认的 record 不回收，执行中的 record 不能确认；已回收 record 不被删除；他人活租约下的 record 对 RecoverExecution 为无操作；
- `frozen.Store`：`Put` 幂等；未知 digest 的 `FrozenRequest` 返回 `frozen.ErrMissing`；Owner 侧本体可按策略回收，Assignment 被接受后执行 payload 由 Execution Store 管理；Worker takeover 不读取 Session；
- MachineState codec：每个 Current variant 与终态 round-trip、拒绝 unknown field / 非法判别式 / trailing data（`agentcore/run/wire` 单元测试）。

Loop conformance 必须覆盖：

- 单模型完成、tool round trip、approval/external response wait/resume；
- known failure 继续、Unknown 继续、tool panic、aliased ToolRef 与 validation；
- parallel/sequential 按冻结 `ToolStep.Scheduling` 调度，不得改用当时的 Settings；
- `ModelCatalog.ResolveModel` 失败或 nil 时 ModelStep 保持 Prepared，Run 保持 active，Loop 返回 `ErrModelUnavailable`；
- ctx cancellation 撤回 Executing 的模型步、随后的 Run 重新规划且只调用模型一次、`ModelSteps` 只计重规划的那一步；executor 取不到冻结本体时撤回并返回错误，后续驱动重新规划；explicit malformed-result disposition；
- Cancel 将 Executing tool/model 投影到 `UncertainCalls` / `UncertainModel`；ExternalResponse reject 为 `response_rejected`；
- streaming delta 与 nil result、EventSink committed observation 携带完整组；
- 非 sentinel commit error 的一次重放、prepare no-progress rejection 与无 livelock；
- 模型结算终结 Run 时 Loop 不再 Load，返回 `LoopFinished` 且 `Result` 等于 Record 的终态；
- Writer 返回 `ErrOwnershipLost` 时 Loop 取消 worker、不再提交 settlement、以该错误返回；随后新 owner 的 `RecoverInterrupted` 把该 Executing 目标记为 Unknown 或撤回到 Open。
