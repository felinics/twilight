# Test Cloud Agent

状态：Agent Core 的 reference application / E2E 验证规范。

本文只定义一个最小的 Owner/worker/recovery 场景及其验证方法，不重新定义
Session、Run、Turn、Executor 或 Workspace 协议。协议权威分别是：

- [Session](agent-session.md) 与 [Session Module Framework](agent-session-extension.md)：事实流、Writer、所有权、幂等与投影；
- [Run](agent-run.md)：执行状态机、Assignment、Outcome 与 recovery；
- [Turn](agent-turn.md)：回合、attempt、输入投递与结算；
- [Runtime](agent-runtime.md)：Owner 组装、Session 所有权与 app 策略；
- [Workspace](agent-workspace.md)：逻辑工作空间、RuntimeBinding 与 TargetRef。

如果本文与上述协议冲突，以上述协议为准。进程级验证 harness 于 2026-09-26 在
组件层重建为 `agent/component/cloudtest`（四个组件经 `httptest` 组成一套，CLD-CMP-4），
演示入口为 `cmd/owner` 等四个二进制与 `deploy/local/`；两者都不是本规范的 API 合同。

## 1. 目标

构造一个最小的远程目录问答 Agent：

1. Owner 接收一个输入并创建 Turn/Run；
2. worker 执行模型与只读目录工具；
3. Owner 只持有 Session、PromptBuilder 和决策身份，不持有模型或工具实现；
4. worker 只持有 Executor、模型/工具实现与 Execution Store，不读取 Owner 的 Session；
5. Owner 或 worker 退出后，新的 Owner 能根据持久事实恢复、重连或明确处置；
6. 测试通过 Session event、Execution Record、provider 调用次数和最终回复验证结果。

首个 fixture 固定为一个 Session、一个 AgentPreset、一个 worker 和一个只读
workspace。这个限制属于 reference application 的 admission policy，不属于 Core。

## 2. 组装与边界

```text
Owner                         worker                         client
Session Store                     Execution Store               HTTP/CLI
Frozen Content Store              LocalExecutor                 submit/status
Host + Loop                       ModelCatalog + ToolCatalog    observe only
Remote Executor client            HTTP Executor server
        |                                  |
        +---------- Assignment/Outcome ----+
```

### Owner

Owner 的组装只允许包含：

- `session.Store`、`artifact.ContentStore`；
- immutable `AgentPreset` 与 `PromptBuilder` 目录；
- `Host`、`Coordinator`、`Loop`；
- 远端 `loop.Executor` client；
- 应用层的 input admission、scheduler、查询 view 和生命周期。

Owner 不包含模型 client、工具实现或 provider SDK。

### worker

worker 的组装只允许包含：

- `ModelCatalog`、`ToolCatalog`；
- `LocalExecutor`、durable `executor.Worker`；
- worker 自己的 `Execution Store` 与 payload store；
- HTTP executor transport。

worker 不打开 Owner 的 Session Store，也不执行 `Turn`、`Run` 或 `Loop`。

### client

client 只负责提交稳定的 InputID、查询 view、读取事件和显示结果。客户端断开
不改变已接受输入的生命周期。

### workspace

测试请求使用稳定的 opaque target：

```text
TargetRef{Kind: "workspace", ID: "fixture"}
```

Owner 的 `TargetResolver` 对每个 tool effect 返回相同逻辑 target，恢复后亦然（RUN-LOP-9）。v1 fixture 可以把
它解析到 worker 上的固定只读目录，但这个路径只是 provider adapter 的实现细节；
Core 不保存路径，也不把 Workspace 放入 AgentPreset。

RuntimeBinding replacement、snapshot restore 和 fork 不属于首个 fixture，另列为
Workspace/Provider conformance。

## 3. Reference application 的最小合同

应用层只需要提供以下逻辑操作，具体 HTTP 路径不属于 Agent Core 合同：

```text
Submit(inputID, text)        接受或幂等重放一个输入
Status(inputID/session)      返回 Input、Turn、Run 和 execution view
Events(from)                 按 Session Seq 读取完整 commit group
Stop(turnID)                 明确停止一个 active Turn
ObserveUntilTerminal(...)    客户端观察，不拥有执行状态
```

### 输入 admission

应用在一个 Session 内串行执行：

1. 已存在 InputID 且正文相同：返回原请求；
2. 已存在 InputID 但正文不同：返回 input conflict；
3. 新 InputID 只有在应用允许时写入 `input_submitted`；
4. 提交响应未知时，先从事实流确认，再决定是否重试；不得生成新的 InputID；
5. Session 的 busy、queue 和 retry policy 由应用负责，Core 仍允许 Run 接收
   `PendingInputs`。

### view

应用 view 必须分别呈现：

```text
input:     accepted | delivered | terminal
turn:      active | attempt_failed | completed | failed | stopped
run:       active | completed | failed | stopped
execution: observed | unavailable | recovery_required
```

这些是应用层 view，不得直接复用 `RunStatus`、`TurnStatus`、`ExecutionStatus`
或 `AttachmentState`。`recovery_required` 只表示 control plane 仍需要动作，不表示
执行结果为 Unknown。

## 4. 故障窗口矩阵

每个场景必须注明故障发生的窗口、保持的 identity、允许的新事实和禁止的副作用。

| 故障窗口 | 必须保持 | 允许的新事实 | 禁止行为 | 主要 oracle |
|---|---|---|---|---|
| Owner 写入 Start 后、Dispatch 前退出 | Turn、Run、Attempt | 重新规划或继续驱动 | 重复旧 Assignment | Run facts、Assignment 数量 |
| worker 接受 Assignment 前退出 | AssignmentKey；若已有 binding 则保持 binding | 明确 missing/orphaned 观察 | 无依据地重发 provider job | Execution Record |
| worker 已持久化 accepted 后退出 | Assignment、Claim、Execution Record | takeover/attach | 创建第二个 execution | binding、provider 调用次数 |
| provider invocation 中 worker 退出 | AssignmentKey、已知 binding | active、terminal 或 orphaned | 自动把不确定执行当作未执行 | backend 状态、最终 facts |
| Outcome 已持久化、通知未送达 | Outcome、AssignmentKey | Owner 重新读取并结算 | 重复执行 provider | GetOutcome、event 数量 |
| Owner 读 Outcome 临时失败 | Run、Step、Claim | retry read | 写入 Unknown 或取消 Run | Run 仍为 Executing |
| settlement append 前失败 | CommitID、Claim | 同一 command replay | 生成第二个 commit | CommitID 索引 |
| settlement append 结果未知 | CommitID、Claim | reopen 后 AlreadyApplied/Applied | 继续使用失效 Writer | ledger、claim state |
| Owner 失去 Epoch 后迟到结算 | 新 owner 的事实 | 无旧 owner 新事实 | 旧 owner 修改 Session | Epoch、ledger head |
| client 断开 | InputID、已接受事实 | 后台继续处理 | 取消服务端执行 | 重连后的 reply |

矩阵中的“保持”是 identity 断言，不是内存对象断言。测试必须从重新打开的
Store/Execution Store 读取它们。

## 5. Recovery 验证

Recovery 的状态域和跨层映射以 [Runtime](agent-runtime.md) 的 DRV-3 表格和
[Run](agent-run.md) 的 RUN-CMT-7 为唯一来源。这个应用只验证映射后的行为：

- `active` / `terminal`：保留 Executing，读取并结算原 Outcome；
- `orphaned → deferred`：保留 Executing，等待显式 control-plane 决策；
- `missing`：才允许 Run recovery disposition；
- `recovery_required`：只是应用 view，不表示 Outcome 为 Unknown。

测试必须区分同一次执行的 attach/读取、显式 RecoverExecution/Dispose、语义
retry 和工具 Unknown。任何恢复路径都不得自动再次执行 Unknown tool call。

## 6. 测试分层

### 6.1 Core conformance

不启动进程，使用 memory/durable store 和 deterministic Executor，验证：

- Session atomic group、Writer 幂等、Epoch fencing；
- Run/Turn 的所有合法和非法状态转移；
- Prompt tool-call pairing、Preset digest 和 immutable projection；
- Dispatch、Outcome、Cancel、Unknown 和 recovery disposition；
- 同一 command/Assignment 的重放不产生第二次事实或副作用。

### 6.2 Adapter conformance

对 LocalExecutor、HTTP Executor 和后续 durable provider adapter 复用同一组
Executor contract，至少验证：

- dispatch replay 不重复调用 backend；
- persisted ExecutionRef 在 attach/status/outcome/cancel 中保持不变，backend 选择只发生在 Dispatch；
- read error 不改变 execution state；
- HTTP transport 的 4xx、5xx、取消和响应丢失分类稳定。

### 6.3 Process conformance

进程级 harness（待重建）通过真实子进程验证：

- Owner crash/restart；
- worker crash/restart；
- SIGSTOP/SIGCONT 造成的 takeover race；
- Owner 断连后的 attach 与迟到结果；
- client disconnect；
- observer 从无 ownership 的 Store 折出的 projection 与 owner 一致。

Owner、worker 的 Session/Execution 数据目录必须分离。若测试共享 fixture
文件，必须明确它是只读 workspace 或独立 payload store，不能把共享目录误当作
共享 Session Owner。

### 6.4 Provider smoke

真实 provider 只验证最小目录问答和工具边界：路径越界、symlink escape、文件类型、
输出上限和 provider 错误。真实 provider 不作为 Core conformance 的稳定 oracle。

## 7. 必须记录的 oracle

每个 E2E 场景至少记录：

```text
Session commit ledger：Seq、CommitID、RunID、StepID、CallID、Claim
Execution record：AssignmentKey、binding、owner、epoch、status、outcome
Side effects：model call count、tool call count、provider job count
Projection：Input/Turn/Run 状态、observedThrough
Result：最终回复、Failure/Unknown/Recovery disposition
```

“HTTP 返回 200”不能单独作为恢复成功的判据。

## 8. 实施顺序

1. 在 app 层重建唯一的 executable harness，先补齐故障窗口矩阵；
2. 抽取 Owner/worker 的最小组装函数，保证没有重复的 Host/Loop/Executor 组装；
3. 为测试应用接入稳定 `TargetRef` 和 `TargetResolver`；
4. 增加 orphaned/deferred 的 control-plane 测试；
5. 最后再实现正式的 client/API 和真实 provider smoke；
6. 每次 Core 修改先跑 Core/adapter conformance，再跑 process matrix。

本规范不要求生产 CLI、外部认证、数据库 schema 或多租户能力。它的唯一目标是：
用一个足够小、边界明确、可重复运行的应用证明 Agent Core 的架构和恢复语义。
