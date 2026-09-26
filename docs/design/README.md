# Agent Core 设计文档

这些文档共同描述 Twilight Agent Core 的 v1 架构。它们不是产品 API 文档，也不把
某个具体部署（CLI、HTTP、数据库或 provider）提升为 Core 协议。

## 目录边界

仓库把 kernel 与建立在它之上的第一个具体 agent 分开放：

```text
agentcore/     Agent Core：session、artifact、run、turn、decision（目录与 seam）、
               executor、owner、driver、preset、observe、store、environment、workspace
agent/         参考 agent：app（组装、Send/Submit/Drain、后台驱动）、prompt（context-v1
               PromptBuilder 与默认目录）、spawn（子代理 Responder 与工具）、
               context/compaction（compaction 策略与 compactor prompt）
```

Core 不携带任何默认的上下文策略、默认工具或默认组装：`owner.New` 要求调用方传入
PromptBuilder 目录；子代理只以 `Waiting(ExternalResponse)` 加 `Driver.Responders` 的
扩展点存在；compaction 只有机制与命令（chatlog），何时 compact、保留多少、用什么
prompt 属于 `agent/`。Memoh 这类 cloud agent 与参考 agent 并列建立在 `agentcore/`
上，不覆盖参考 agent 的任何决定。

## 权威边界

```text
agent-session.md              Session kernel：commit ledger、逻辑流、ownership、append、read
agent-session-extension.md    Writer、module registry、按事件类型的 payload 版本、projection、claim admission
agent-artifact.md             Artifact binding、content reference、retention claim
agent-session-chatlog.md      对话内容与 context projection
agent-run.md                  Run machine、Runtime、Loop、Executor contract
agent-turn.md                 Turn、attempt、input routing、结算投影
agent-decision.md             PromptBuilder seam、决策组件目录与参考实现
agent-runtime.md              Owner 组装与 Session 所有权、driver；参考 agent 的 spawn 与 app 策略
agent-workspace.md            可选 Workspace/Runtime/TargetRef domain
```

依赖方向是：

```text
agentcore/session (kernel)       agentcore/artifact (independent core)
          \                  /
           agentcore/session/extension（含 writer）
               ↓
   chatlog / session-run / attempt / turn
               ↓
             decision / owner（Owner 组合 turn / run）
                    ↓
   agent/（参考 agent：app、prompt、spawn、compaction）/ transport / provider adapter
```

`agentcore/run` 与 `agentcore/turn` 的协议核心保持独立；它们的 Session adapter 才依赖
Module Framework。Workspace 同样是可选 application domain，不是 Core 的依赖。

Workspace 是可选的 application domain。Agent Core 只携带 opaque `TargetRef`，不
解释 Workspace、Runtime 或 provider 的生命周期。

## 术语规则

- `RunStatus`、`TurnStatus`、`ExecutionStatus`、`AttachmentState` 属于不同状态域，
  不互换枚举。
- `AttachmentState=orphaned` 是 Executor 的观察结果；它在 `agentcore/run/reconcile`
  中映射为 `Verdict=defer`，并触发一次 `RecoverExecution` 请求。
- `recovery_required` 是应用/API view，表示恢复尚未完成，不表示执行结果为
  `Unknown`。
- Artifact 的“孤儿 claim”只表示 retention claim 没有对应 owner fact，与
  Executor 的 `orphaned` execution 无关。
- Commit ledger 是唯一事实权威；projection、snapshot、HTTP view 和 CLI 输出都
  是派生数据。
- 角色名按进程与某个 Session 的关系取：`Owner` 持有该 Session 的 lease 并在其 Epoch
  下写入；`Observer` 不持 lease、按 SessionID 读 Store；`Worker` 执行 Assignment、不接触
  Store；`Node` 是宿主进程，可同时是若干 Session 的 Owner 与另一些的 Observer。"authority"
  一词只用于持久的权威：ledger、Execution Store 与 artifact 的 `Authority`（逻辑 store 实例）。
- Session kernel 与 Run 协议都没有版本号（SES-VER-2、RUN-CMT-8）；版本只存在于 payload 的 `v`
  （每个事件类型自己的 codec 版本，SES-VER-1、EXT-REG-2）。段不携带任何模块层版本。

## 阅读与维护规则

1. 协议不变量只在拥有该概念的文档中定义一次；其他文档只引用条款。
2. 应用文档只描述组装、输入 admission、transport 和验证场景，不复制 Core 的
   state machine 或 wire contract。
3. 每个公共名称必须同时与 Go API、测试和文档一致；更名时更新所有三者。
4. 每个恢复场景必须注明：故障窗口、保持的 identity、允许的新事实、禁止的
   side effect 和验证 oracle。
5. 未实现的部署能力不得写成已经存在的 Core 能力；应放在 reference application
   或 adapter 的实施计划中。

`test-cloud-agent.md` 是 reference application 的 E2E 规范。进程级 harness 与演示
入口已于 2026-09-17 移除，待 owner / app 分层稳定后在 app 层重建；harness 验证上述
合同，但不重新定义合同。
