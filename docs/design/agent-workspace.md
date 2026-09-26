# Workspace / Environment

状态：v1 设计规范。Workspace 与 Environment 是 Agent Core 之外的 application 资源层（`agent/workspace`、`agent/environment`、`agent/tools`、`agent/executor/sandbox`），经 RUN-LOP-9 的 opaque target 接入执行；`agentcore` 不承载资源模型（APP-TGT-1）。

## 1. 概念

| 概念 | 含义 | 持久化 | 写入者 |
|---|---|---|---|
| Workspace | 逻辑工作世界的稳定身份：`ID`、`Project`、`Base`（起点 revision）、最近 `Snapshot`、当前 `Runtime *RuntimeBinding` | `workspace.Store` | application（创建、fork、snapshot 策略） |
| Snapshot | provider 产出的 durable 状态锚点（`StateRef`），可 Restore 为新 environment，可作为 fork 的来源；形成 Parent 链 | `workspace.Store` | provider / application adapter |
| Environment | Workspace 的一次物理 materialization（目录加进程、容器、VM、云 sandbox），生命周期 `Create` / `Restore` / `Attach` / `Close`；可选能力 `Executor`（运行命令）与 `FS`（读写文件） | 不持久化本体 | `environment.Provider` |
| RuntimeBinding | Workspace → 当前 Environment：`Backend`、`EnvironmentRef`、`Generation`（第几次 materialize） | 嵌在 Workspace 记录内 | sandbox backend |
| SessionBinding | Session → Workspace | Session ledger（APP-WSP-1） | application |
| ExecutionRef | executor 为一次 attempt 建立的 provider 句柄（RUN-EXE-9） | execution ledger | Worker |

三个名字不互换：Snapshot 是资源层的状态锚点；chatlog 的上下文压缩叫 Compaction（APP-CKP）；CQRS 消费位置叫 checkpoint（`checkpoint.Store`）。

## 2. Session 绑定

**APP-WSP-1（绑定是 Session 事实）** `agent/workspace` 是 `agent` 源的应用模块（EXT-REG-1）：singleton stream `workspace`（`LineageSession`），事实 `agent/workspace/bound{workspace, scope}` 与 `agent/workspace/unbound{scope, reason}`，投影 `agent/workspace/binding` 折叠为 `Binding{Bound, Workspace, Scope}`。`scope` 是写下该事实的 Session；子 Session 折叠 fork 前缀时读到 `scope ≠ 自身` 的绑定，`Binding.InheritedBy(sid)` 为真。绑定进 Session ledger 的收益：解析器与 Loop 读同一 epoch 视图；PromptBuilder 能告知模型所在 workspace（APP-WSP-4）；Turn 边界的 snapshot 引用（第二阶段）有落点。RuntimeBinding 与 Snapshot 不进 Session ledger：一个 Workspace 可被多个 Session 共享，environment 的替换不属于任一 Session 的历史。

**APP-WSP-2（命令）** `workspace.Commands.Bind(ctx, w, id)` 与 `Unbind(ctx, w, reason)` 经 Session Writer 提交：已由自身事实绑定到同一 id 的 Bind 与无绑定时的 Unbind 为 noop；继承的绑定经 Bind 转为自身事实，经 Unbind 结束。CommitID 带 head Seq（`workspace-bound/<id>/<seq>`），同一 head 的重试重放，之后的 Bind 是新事实。`app.Session.BindWorkspace` 先经 `workspace.Store.Get` 确认 Workspace 存在；`app.Application.AllocateWorkspace(project, base)` 创建记录；inbox 命令 `bind_workspace{workspaceId}` 与 `unbind_workspace{reason}` 走同一路径（APP-INB-2），未知 Workspace 为 rejected。

## 3. 解析、路由与执行

**APP-WSP-3（从 Placement 到 Environment）** 链路：

1. 工具声明 `Placement()`（RUN-LOP-9）。`agent/tools` 的 `Tool` 是环境感知的工具接口（`Run(ctx, env, req)`），`sandbox.PublicTools` 把它们冻结为带 `PlacementWorkspace` 的 `turn.PublicTool`，经 `app.WithPublicTools` 进入 preset；`loop.ExecutableTool` 仍是进程内工具（`PlacementProcess`）。首批工具：`shell`（ReplayForbidden）、`read_file`（ReplayAllowed）、`write_file`（ReplayForbidden）、`list_dir`（ReplayAllowed），路径相对 workspace 根且不得逃逸（`environment.ErrOutsideRoot` → `invalid_input`）。
2. `workspace.Resolver`（`loop.TargetResolver`）只对 `Kind == tool && Placement == PlacementWorkspace` 的 effect 读绑定投影，`Bound` 时返回 `TargetRef{Kind: "workspace", ID}`，自身与继承的绑定同样有效；未绑定返回 nil。
3. Worker 的 Route 表：`sandbox.Route` 的 `Match` 为 `ToolAssignment.Placement == PlacementWorkspace`，与 target 是否存在无关；`app.Build` 在 `Config.Workspaces.Provider` 非空时把它放在进程内路由之前。
4. `agent/executor/sandbox.Backend`（`executor.ExecutionBackend` + `notice.Source`）包装 `loop.LocalExecutor`：`Validate` 拒绝 model call、`PlacementProcess` 的工具（路由配置错误）与无 workspace target 的工具（Session 未绑定，`invalid_input` 的 Known 失败，效果未开始）；`Prepare` 为按 AssignmentKey 的纯派生；`Start` 时 manager 按 `Target.ID` 解析 Environment 后调用 `Tool.Run`；in-flight 表、Attach / Status / Outcome / Cancel 与结算通知由 `LocalExecutor` 提供。
5. manager 的解析：读 `workspace.Store.Get`；有 RuntimeBinding 且 Backend 相同则 `Provider.Attach`，`environment.ErrNotFound` 时重新 materialize；无绑定则 `Create(Spec{Subject: id, Base})`（有 Snapshot 则 `Restore`）；随后 `Store.UpdateRuntime(id, expected, Binding{Generation: expected + 1})` 以 Generation 为条件写入，`ErrGenerationConflict` 时关闭自己创建的 environment 并 Attach 赢者的。已 attach 的 environment 按 Workspace 缓存于进程内，`Backend.Close` 释放。

**APP-WSP-4（模型知道所在 workspace）** `prompt.ContextPromptBuilder.Preface` 是 system prompt 的附加段；`prompt.WorkspacePreface` 读绑定投影，`Bound` 时写入 workspace 身份，未绑定为空。`app.Build` 在 `Config.Workspaces` 非空且 `Config.Decisions` 为 nil 时选用 `prompt.PromptBuildersWith(prompt.WorkspacePreface)`。

## 4. fork 与 spawn

**APP-WSP-5（继承为默认，策略以事实覆盖）** Session fork（OWN-FRK-2）复制已提交事实，绑定事实随前缀被子 Session 读到并标为 inherited：这就是 Share 策略，不写任何事实。其他策略由 `SessionOptions.InheritedWorkspace`（`workspace.InheritedPolicy`）在子 Session 首次 Open 时对继承的绑定执行，写子自己的事实：`InheritNone` 写 `unbound`；`InheritAllocate` 创建带父 Project 与 Base 的空 Workspace 并 `bound`；`InheritClone` 以父 Workspace 的最新 Snapshot `Store.Fork` 出新 Workspace 并 `bound`；`InheritRestore` 以子历史上 fork 点处记录的 Snapshot（`Binding.Snapshot`，APP-WSP-7）`Store.Fork` 并 `bound`，缺少所需 Snapshot 时 Open 失败。策略只对 `InheritedBy(sid)` 为真的绑定运行，自身的绑定与无绑定不受影响；父的绑定不受子的事实影响。spawn 的子 Session 不经 `OpenSession`，默认 Share。对话 lineage 与资源 lineage 仍是两套：Workspace 的 fork（`Store.Fork`）由 Snapshot 产生新 ID，不复用 mutable 的 RuntimeBinding。

## 5. provider

**APP-WSP-6（local provider）** `agent/environment/local` 是参考 provider：root 下一个目录一个 environment，`Exec` 为 os/exec（工作目录限定在 environment 内，输出按 `MaxOutputBytes` 截断），`FS` 为宿主文件系统，`Snapshotter` 把目录复制到 `root/.snapshots/<state>`，`Restore` 从该副本 materialize 新目录；`Create` 只接受空 `Base`；只服务单进程，`Attach` 对其他宿主不可见。多副本 sandbox backend 需要任一副本都能 Attach 的 provider（云 sandbox）。

**APP-WSP-7（Snapshot 与 environment 重建）** `sandbox.Backend.Snapshot(id)` 经 environment 的 `Snapshotter` 取得 `StateRef`，写 `workspace.Snapshot{Ref, Workspace, Backend, StateRef, Parent: 上一个最新}` 并把它设为 Workspace 的最新；无 environment 的 Workspace 为 `ErrNothingToSnapshot`，无该能力的 provider 为 `environment.ErrUnsupported`。`app.Session.SnapshotWorkspace` 随后经 `Commands.RecordSnapshot` 写 Session 事实 `agent/workspace/snapshotted{workspace, snapshot, scope}`，投影的 `Binding.Snapshot` 即该 Session 历史上最新的 Snapshot，fork 的子读到的是 fork 点处的值。最新指针以 `Store.UpdateSnapshot(id, ref)` 写入（只改 Snapshot 字段），不会回滚并发的 `UpdateRuntime`。`WorkspaceConfig.SnapshotAfterTurn` 在每次排空积压的结算后在后台执行（同一 Session 串行，`Session.Wait` 覆盖它），结算不等待复制；未绑定与无 environment 为空操作，其他失败进 `Config.Warn`。sandbox backend 对已 attach 的 environment 按 workspace 缓存，materialize 按 workspace 加锁而非全局；工具以 `unavailable` / `execution_failed` / `internal` 失败时，backend 向 provider 核实该 environment，`ErrNotFound` 则丢弃缓存，下一次调用重建并标记，本次调用不重跑（由 Replay 声明决定）。environment 丢失后被重建（RuntimeBinding 的 Generation 递增）时，重建后第一次成功的工具结果附带 `workspaceRematerialized: true`，告知模型自最近 Snapshot 以来的写入已丢失。

## 6. 未做

远程 worker 形态下 sandbox backend 作为独立组件（CLD-TOL-1，随 cloud 的四个二进制）；spawn 子 Session 的 Share 以外策略（spawn 不经 `OpenSession`）；Snapshot 的回收策略。
