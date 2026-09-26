# Twilight Agent Decision Layer

状态：v1 设计规范。本文定义 Agent Core 的决策层。协议边界见 [Turn](agent-turn.md)、[Run](agent-run.md) 与 [Chatlog](agent-session-chatlog.md)。

本文定义 `agentcore/decision`（PromptBuilder seam：目录、`Sources`、v1 输入内容编解码）与参考 agent 的实现 `agent/prompt`（`ContextPromptBuilder` 与默认目录）：把已提交状态与 AgentPreset 变成下一条 prompt 的组件。Core 只规定 seam 与目录，不携带任何一种上下文策略。agent core 的三层按纯性划分——事实层（Session ledger、Writer、投影）、决策层（本文）、效果层（模型调用、工具执行）；决策层是三层里给定输入即确定的一层，它读投影、不做 IO，全部运行在 Session Owner 一侧。文中的"必须""不得""应该"是协议约束。

## 1. 范围与身份

```text
Facts → Decision → Assignment → Effect → Outcome → Facts
              ↑
        决策层：Machine.Next（run 模块）、PromptBuilder
```

决策层的 PromptBuilder 以 ref 命名并经目录解析。工具调度（`Scheduling`）、畸形结果重试上限（`MalformedRetries`）、模型与工具身份、SystemPrompt 是 AgentPreset 上的数据。每个 effect 的外部资源目标由宿主的 TargetResolver 按 effect 解析（RUN-LOP-9）；决策层不读取它。

**DEC-SCP-1** 决策层的每个组件是投影状态与 AgentPreset 的确定性函数，不做外部 IO：同一 AgentPreset、同一投影状态，任何进程得到同一结果。这是接管后新进程续跑同一 Turn 的前提。

**DEC-SCP-2** 决策组件有持久身份：`turn.PromptBuilderRef`。身份与全部数据字段一起进 AgentPreset 摘要（TRN-PST-1），因此 Turn 记录的 `PresetRef` 唯一决定了它的决策函数。效果组件（模型、工具）也有身份（`ModelRef`、`ToolRef` 与 definition digest），但它们的实现不在本层，可以位于另一个进程。

**DEC-SCP-3** 本层不 import 宿主装配包。宿主经目录（第 3 节）解析 AgentPreset 的 `Prompt`，把得到的 PromptBuilder 与 AgentPreset 上的 Settings 交给 Loop。

## 2. PromptBuilder

```go
type ProjectionSource interface {
    Load(ctx, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion) (any, session.Head, error)
}
type Sources struct {
    Projections ProjectionSource        // 结构投影
    Content     chatlog.ContentResolver // 按 digest 取回冻结正文（CHT-MAT-1）
}
type PromptBuilderFactory func(turn.AgentPreset, Sources) loop.PromptBuilder
const PromptContextV1 turn.PromptBuilderRef = "twilight/decision/prompt/context-v1"
```

`ContextPromptBuilder`（`agent/prompt`，参考 agent）是 `PromptContextV1` 的实现：读 `twilight/chatlog/context` 投影，经 `chatlog.Materializer` 取回正文，构造 `loop.Prompt`（模型、`sdk.Request`、消费的 InputID、context 新鲜度 token、冻结的 ToolSpec）。

**DEC-PMT-1** PromptBuilder 在每次 Build 时经 `ProjectionSource` 读取 `twilight/chatlog/context` 投影（含已应用的 compaction，CHT-EVT-3），再经 `Content` 物化条目命名的冻结正文；同一次 Build 内每个 digest 至多读取一次。折叠是纯函数，读正文是 IO：正文缺失使 Build 以 `frozen.ErrMissing` 失败，投影不受影响。owner 进程从 Session Writer 读，观察者从 Store 读，两者对同一 head 给出同一状态（EXT-PRJ-4）；两者共用同一 cas ContentStore。

**DEC-PMT-2** `sdk.Messages` 顺序：

1. `AgentPreset.SystemPrompt` 非空时一条 system message；
2. 按 fold 顺序：`input` → user；`assistant` → assistant（冻结 `ModelResult` 的 reasoning、text 与 tool calls；`Materialized.Calls[i].ProviderCallID` 写入 `sdk.ToolCallPart.ToolCallID`）；`tool_result` → tool（以同 Turn assistant 中同 CallID 的 `ProviderCallID` 配对；success 的正文为物化的输出，error 为 Failure 文本，`unknown` 渲染为标记 error 的说明文本）；`summary` → assistant text。

回合中途投递的输入在其之前尚未结算的工具结果之后排列：这类输入的 `input_delivered` 先于 `tool_result` 进入 ledger，provider 要求工具结果紧随发出调用的 assistant 消息。prompt 构造时暂存这类输入，待工具结果配对后写入。CancelRun 为全部未完成调用提交终态结果，后续 Turn 继承完整配对的上下文；构造器遇到未配对调用或结果时返回错误。

**DEC-PMT-3** `plan.PromptInput.Inputs` 与本 Turn 已 delivered 且属于本次 Prepare 的 Input 按 ID 对齐，包括回合中途经 Deliver 进入的输入。这些 Input 的 `input_delivered` 与 `input_accepted` 同 commit，Build 时一定已在 fold 中；PromptBuilder 只使用 fold。

**DEC-PMT-4** `Prompt.Model = AgentPreset.Model`；`Request.Tools` 与 `Prompt.Tools`（ToolSpec：Ref、DefinitionDigest、Policy）都由 `AgentPreset.Tools` 派生，顺序一致；`InputIDs` 为本次消费的 PendingInput IDs；`Token` 为投影 head 的 `Next`。PresetRef 独立标识冻结的决策配置。

**DEC-PMT-5** summary 的 TextPart 直接写入 sdk.Message；ReferencePart 在 context-v1 中以名字呈现，不物化。assistant 与 tool_result 的正文来自冻结值，不是 parts。

**DEC-PMT-6** 同一 Turn 有多个 Run attempt 时，context-v1 把全部 attempt 的 assistant 与 tool_result 按 commit 顺序纳入 prompt，包括失败 attempt 的部分输出与 status=`unknown` 的工具结果。其他策略以另一个 `PromptBuilderRef` 注册，不修改本实现。

## 3. 目录

```go
type PromptBuilders struct{ /* PromptBuilderRef → PromptBuilderFactory */ }
func NewPromptBuilders(map[turn.PromptBuilderRef]PromptBuilderFactory) (*PromptBuilders, error)
func (*PromptBuilders) Register(turn.PromptBuilderRef, PromptBuilderFactory) error
func (*PromptBuilders) Resolve(turn.AgentPreset, Sources) (loop.PromptBuilder, error)

// agent/prompt（参考 agent）
func DefaultPromptBuilders() *decision.PromptBuilders // 含 PromptContextV1
```

**DEC-CAT-1** 目录在装配期构建、运行期只读：空 ref、nil factory、重复 ref 被拒绝。目录是 Owner 侧的组件，不需要效果实现即可构建。Core 不提供默认目录：`owner.Ports.Decisions` 必填，参考 agent 传 `prompt.DefaultPromptBuilders()`，其他 agent 传自己的目录。

**DEC-CAT-2** `PromptBuilders.Resolve(preset, source)` 按 `preset.Prompt` 解析；未注册返回 `ErrUnknownPromptBuilder`，该 AgentPreset 不得被注册或驱动（PST-2 的 `preset_unavailable`）。接管进程以同一 AgentPreset 解析得到同一决策函数。

**DEC-CAT-3** 目录只承载需要代码的组件。工具调度与畸形结果重试上限是数据，直接作为 AgentPreset 的字段进摘要，Loop 从 AgentPreset 读取（RUN-LOP-1）；给数据加 ref 与目录不带来任何判定，只增加一层间接。

## 4. 用户正文

同一份 canonical JSON 同时是 `twilight/chatlog/input_submitted.Content` 与 `run.AgentInput.Payload`。

**DEC-INP-1** 输入内容的形状是 agent 的决定，core 只把 `Content` 当 opaque 的 canonical JSON 存储与摘要（`chatlog.Commands.Submit(ctx, w, id, content)`）。reference agent 的 v1 形状为 `{"text":"<用户字符串>"}`：`agent/input.Text(text)` 构造，`input.TextOf(content)` 还原，PromptBuilder 把它投影为 sdk user text。

## 5. conformance

- **DEC-SCP-1、DEC-CAT-2、DEC-PMT-1**：两个独立构建的目录对同一 AgentPreset、同一投影状态与同一冻结正文解析出的 PromptBuilder 给出逐字段相同的 `Prompt`；正文缺失使 Build 失败；未注册的 PromptBuilderRef 解析失败；nil 目录不可解析。
- **DEC-CAT-1**：空 ref、nil factory、重复注册被拒绝。
- **DEC-PMT-2**：中途输入排在未结算工具结果之后；`unknown` 工具结果标记 error；未配对调用或结果被拒绝；多 attempt 的条目全部进入 prompt（由 turn 与 host 的集成测试覆盖）。
- **DEC-INP-1**：`input.TextOf(input.Text(s)) == s`。
- **TRN-PST-1**：Prompt、Scheduling、MalformedRetries、SystemPrompt 任一变化改变 AgentPreset 摘要（turn 的 golden）。
