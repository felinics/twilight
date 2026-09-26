# Twilight Cloud Agent 组件边界

本文档定义 cloud agent 的部署组件：每个组件拥有什么状态、对外暴露什么协议、依赖 Agent Core 的哪些端口接口。它只划分边界，不规定实现语言之外的技术选型；`agent-runtime.md` §11 的两种部署形态中，本文档展开的是"云端"一栏。编号前缀 `CLD-`。

Agent Core 的协议规则（`agent-run.md`、`agent-runtime.md`）在所有组件中不变。组件之间只经这些规则已经定义的端口通信；本文档新增的协议只有一条：Worker 与 Backend 之间的 wire（CLD-WIR）。

## 1. 划分依据

**CLD-CMP-1（划分标准）** 组件按契约与状态归属划分，不按进程划分。两个组件可以部署在同一进程或同一 Pod，边界不变；一个组件不得跨两个 authority 持有状态。判定一个候选是否为独立组件的三个条件：它拥有其他组件不持有的状态或凭证；它的伸缩单位与相邻组件不同；它与相邻组件之间已有或需要一条 message-shaped 的接口。三者满足其一即分开。

**CLD-CMP-2（清单）** 共七个组件。

| 组件 | 拥有的状态 / 凭证 | 对外协议 | 依赖的 Core 端口 | 伸缩单位 |
|---|---|---|---|---|
| executor worker | execution ledger 的租约与 fencing（authority 在共享 store）；进程内 `ProgressHub`、`SettlementHub` | `effect.ExecutionPort` 的 HTTP 绑定（`executor/http`） | `executionstore.Store`；`ExecutionBackend`（经 CLD-WIR） | ledger 吞吐 |
| model backend | provider 凭证、base URL、限流配额；in-flight 表 | CLD-WIR 的 Backend 协议 | `sdk`、`provider/*`、`loop.ModelCatalog` | provider 并发 |
| tool sandbox backend | sandbox 生命周期、workspace materialization；in-flight 表 | CLD-WIR 的 Backend 协议 | `loop.ToolCatalog`、`agent/environment`、`agent/workspace` | sandbox 资源，按 session 或 tenant 隔离 |
| owner service | Session 租约（`session.OpenOptions`）；Writer 内存投影 | 命令面（Send、Turn 状态、Fork 等 `app.Application` 的方法）；观察流（`observe.Bus`） | `session.Backend`、`artifact.ContentStore`、`owner.Artifacts`、`process.Store`、`effect.ExecutionPort`（`executor/http.Client`）、`effect.SettlementPort` | Session 数 |
| controller | 无持久状态；策略参数（放弃时限、扫描间隔） | 内部 | `session.Store.ListLeases`/`ExpiredLeases`（SES-OWN-5）、`effect.ExecutionPort.Attach`、`effect.Recoverer`、Worker 的 `Dispose`、owner 的 Open | 单实例或 leader 选举 |
| gateway | 无持久状态；认证会话 | 面向用户的 HTTP/WebSocket | owner 的命令面与观察流 | 连接数 |
| 共享存储 | 全部 durable 事实：Session ledger 与 lineage、execution ledger 与租约、dispatch ledger、artifact binding 与 claim、CAS 正文 | SQL 与对象存储 | `session.Backend`、`executionstore.Store`、`process.Store`、artifact stores 的实现 | 存储容量 |

**CLD-CMP-4（组件的组装）** 每个组件是 `agent/component/<name>` 下的一个库包：类型化的 `Config`（`agent/config` 的 JSON 文档，未知字段与尾随内容为错误，凭证只以 `secrets.Dir` 目录名出现，进程身份为 `config.Identity{name | file}`，集群里由 downward API 挂成文件）、从就绪依赖组装的 `New`、从 `Config` 组装的 `Compose`，以及 `Handler()` / `Close(ctx)`。`cmd/worker`、`cmd/model-backend`、`cmd/tool-backend`、`cmd/owner` 只是 `agent/component/run.Main` 应用到各自的包；`agent/serve` 在组件 handler 旁提供 `/healthz`、`/readyz` 与 SIGTERM 下的 drain 加 `Close`（租约在 `Close` 里释放）。worker 的 Route 表按声明选 backend：`executor.MatchModel` → model backend，`executor.MatchTool(PlacementWorkspace)` → tool backend（CLD-WIR-0）。owner 在 remote 模式下不组装 sandbox，workspace snapshot 经 `agent/workspace/http.Client` 到 tool backend。进程级验证是 `agent/component/cloudtest`：四个组件各起一个 `httptest.Server`、worker 在一个可切换目标的反向代理（Service 地址）之后，覆盖 CLD-DEV-2 的一轮对话（model call 与 shell call 各经其 backend，文件落在 tool backend 的 environment，Turn 后 snapshot）、"worker 在 live drive 中被替换而 owner 不变"（Watcher 的 probe 把 orphaned record 交给新 worker，结算经新副本的公告帧后立刻读到）与"worker 与 owner 在模型调用进行中被替换"（新 owner 的接管处置经新 worker 把 orphaned 的 record 接回、attach 到仍在 model backend 里运行的执行，结算经新副本到达，旧 owner 的写入被围栏）。`deploy/local/` 有四份示例文档与单机运行说明。

**CLD-CMP-3（不在清单中的）** Reconciler、Loop、Driver、Watcher 都是 owner service 进程内的组成，不是组件：它们没有自己的状态归属，也没有 message-shaped 的对外接口。Responder（包括 spawn 子代理）同样在 owner 内。preset 注册表按 OWN-SCP-2 可以是进程内或共享服务，本文档把它归入 owner service，共享化留作后续。

## 2. 各组件

### 2.1 executor worker

**CLD-EXE-1** 组件即 `executor.Worker` 加 `agent/executor/http.Server`。它持有 `executionstore.Store` 的连接，路由表（`executor.Route`）的每个 provider 指向一个经 CLD-WIR 连接的 backend。进程内没有 provider 凭证，也没有工具实现；`loop.NewLocalExecutor` 不在此组件中。Route 表没有默认路由：model call 按 `executor.MatchModel` 到 model backend，`PlacementWorkspace` 的工具到 tool backend，其他 Assignment（`PlacementProcess` 的工具，spawn 除外——它由 Responder 应答、不经 Dispatch）在 Dispatch 时以确定拒绝返回。

**CLD-EXE-2** 对 owner 暴露的端点为现有的 `/validate`、`/dispatch`、`/attach`、`/abort`、`/status`、`/outcome`、`/cancel`、`/recover`、`/dispose`、`/acknowledge`、`/progress`、`/settlements`（RUN-EXE-3、RUN-EXE-12、RUN-EXE-17）。多副本时任一副本可应答任一 key：Attach、GetOutcome、Abort 只读 ledger；Dispatch 的重放与 RecoverExecution 由 ledger 的 Seq 0 竞争与租约裁决（RUN-EXE-14、RUN-EXE-16）。owner 对每个 Worker 副本保持一条 `/settlements` 订阅（CLD-OWN-3）。

**CLD-EXE-3** Worker 副本的 `WorkerOptions.ID` 在重启后不得复用（现有约束）；k8s 下取 Pod 名加启动时间戳。`SettlementHub` 的 epoch 随之变化，订阅者按 RUN-EXE-17 重读。

### 2.2 model backend

**CLD-MDL-1** 组件承载 `sdk` 与 `provider/*`：把 `effect.ModelAssignment` 内联的冻结请求交给 provider，流式 delta 作为进度帧上行，结果作为 Outcome。provider 凭证、base URL、region、配额只存在于此。今天这部分逻辑在 `loop.LocalExecutor.runModel` 与 `invokeModel` 中；组件化后它们移入 model backend，`LocalExecutor` 保留为单进程形态的 in-process backend。catalog 是 `agent/models`（不在 agentcore）：`Entry{Ref, Kind, Model, BaseURL, APIKeySecret | AuthTokenSecret}` 把 preset 冻结的逻辑 ModelRef 映射到一个 provider 的物理 model id；条目只命名密钥、不携带密钥值，`Build(ctx, entries, resolver)` 经 `secrets.Resolver.Lookup(name)` 解析，并按 (Kind, BaseURL, 凭证) 共享 provider 实例。`agent/secrets` 是部署层的密钥读取抽象，local 与 cloud 共用，models 只是消费者，后续需要凭证的工具、workspace、backend 同样消费它：cloud agent 用 `secrets.Dir`（k8s Secret 挂载为目录，每个 key 一个文件，值只去掉一个结尾换行，名字须为单个路径元素），local agent 用 `secrets.Static`（从本地配置装入），有 vault 的宿主自行实现 `Lookup`；catalog 与二进制都不读进程环境变量。catalog 文档本身是一个 JSON（`models.Load`，`{"models":[...]}`，恰好一个文档，含字面凭证或尾随内容即拒绝），在 k8s 中来自 ConfigMap，同一份文档在两种部署下不变。`*sdk.Model` 本身满足 `loop.ModelInvoker` 与 `StreamingModelInvoker`，适配只做一件事：把请求里的逻辑名清空，由 sdk 填入物理 id。catalog 只是查找表，不做路由、回退、配额与 key 轮换；需要它们时把 Entry 的 BaseURL 指向一个网关。

**CLD-MDL-2** Ref 是 backend 自己的 in-flight 标识。`Prepare` 按 AssignmentKey 派生（不分配任何资源，RUN-EXE-3）；`Start` 分配 in-flight 条目并发起 provider 调用；`Attach` 按 Ref 查 in-flight 表，条目存在为 active 或 terminal，不存在为 missing。model backend 无持久状态，重启后所有 in-flight 都是 missing，Worker 按 RUN-EXE-9 对模型 Assignment 重派，`Restart` 返回新 Ref。

**CLD-MDL-3** 限流、key 轮换、按 tenant 的配额是本组件的策略；Worker 与 owner 看到的只是 Outcome 里的失败分类与 `RetryDisposition`（RUN-EXE-11）。

### 2.3 tool sandbox backend

**CLD-TOL-1** 组件承载 `loop.ToolCatalog` 的实现与工具运行环境的 materialization：在 Assignment 携带的 Target 上 materialize 工具运行环境并执行。逻辑 workspace 与物理 environment 的模型在 `agent/workspace` 与 `agent/environment`（application 的资源层，APP-WSP，agent-workspace.md）；本组件即 `agent/executor/sandbox.Backend` 经 CLD-WIR-1 的 Backend 协议暴露，配一个任一副本都能 Attach 的 `environment.Provider`（`agent/environment/local` 只服务单进程）。Target 的解析在 owner 侧完成（`loop.TargetResolver`，APP-TGT），backend 只接收已解析的 `run.TargetRef`。工具定义、ResponsePolicy、Replay 与 Placement 在 `Validate` 时逐项与 preset 冻结的 ToolSpec 比对（`tool_definition_mismatch`），因此 owner 注册 preset 所用的工具集合与本组件服务的集合必须来自同一版本：RUN-EXE-8 的同构假设延伸到 tool backend。

**CLD-TOL-2** Ref 是 sandbox 内一次执行的标识；`Attach` 按 sandbox 状态回答，sandbox 仍在但执行记录不在为 missing，sandbox 本身不可达为 orphaned（不得回答 missing，RUN-EXE-3）。工具是否可重派由 Assignment 的 Replay 声明决定，Worker 在调用 `Restart` 前判定（RUN-EXE-9、TRN-DUR-4）；backend 不做这个判断。

**CLD-TOL-3** sandbox 的生命周期（按 session 创建、空闲回收、配额）是本组件的策略，与 Run 协议无关。sandbox 回收后其内的 Ref 全部 missing。

### 2.4 owner service

**CLD-OWN-1** 组件即 `app.Application`：`owner.New` 组装的 Owner、Driver、按 PresetRef 缓存的 Loop、每个 Session 的 Reconciler，加 `observe.Bus`。它持有 Session 租约（`session.OpenOptions.LeaseDuration`、Writer 心跳续租，SES-OWN-1）和 Writer 的内存投影；一切 durable 事实在共享存储。

**CLD-OWN-2** 端口实现：`session.Backend`、`artifact.ContentStore`、`owner.Artifacts`、`process.Store` 为共享存储的客户端；`effect.ExecutionPort` 为 `executor/http.Client`。owner 进程内没有模型客户端与工具实现（OWN-PRT-2）。

**CLD-OWN-3** Driver 持有一个 `effect.Watcher`（RUN-EXE-17）；多个 Worker 副本共用一个 owner 可见的地址时（k8s Service），一条订阅落在一个副本上，只收到该副本的结算通知。因此 Watcher 的周期性重读是必要路径而非兜底；或者 owner 对每个 Worker 副本各持一个 Watcher，`http.Client` 需要能枚举副本（headless Service）。本文档选后者作为目标，前者作为过渡。

**CLD-OWN-4** 多副本 owner：一个 Session 在任一时刻只有一个 owner 副本持有其租约，且只在有活动工作时持有（APP-ACT-1、APP-ACT-2）。哪个副本持有由到达命令的副本与 Acquire 裁决：gateway 可把命令发给任一副本，到达的副本在无持有者时 Open，在有存活持有者时命令由持有者应用；同一 Session 的相邻 Turn 因此可以落在不同副本，Session 空闲时没有 owner。owner 副本间不做协调，`OpenOptions.Takeover` 是唯一的强制接管开关，只在需要强制迁移时由 controller 使用（CLD-CTL-2）。

### 2.5 controller

**CLD-CTL-1** controller 承担 RUN-EXE-6 明确归部署的决定：何时、对哪些、由谁触发恢复与放弃。它没有持久状态，全部依据来自共享存储与端口的读取。

**CLD-CTL-2（强制迁移）** 普通的 Session 恢复不经 controller：租约过期后任一 owner 副本的扫描在 Acquire 的裁决下接管（APP-ACT-3），Open 内部的 `RecoverInterrupted` 完成该 Session 全部 Run 的效果处置（RUN-CMT-7）。controller 只负责强制迁移：对一个持有中且租约存活的 Session（副本下线、负载迁移、运维隔离），选择目标 owner 副本执行 `Open(sid, Takeover: true)`，这是 `Takeover` 的唯一使用方；以及集群级策略（副本数、扫描参数、放弃时限）。

**CLD-CTL-3（效果恢复与放弃）** owner 存活时，其 Reconciler 已按 `OrphanProbe` 对自己 Run 的 orphaned 效果调用 `RecoverExecution`；owner 死亡时随 APP-ACT-3 的接管 Open 一起完成。controller 只处理放弃：一个效果 orphaned 超过放弃时限后调用 Worker 的 `Dispose`，owner 下一次读到 Unknown 后按 RUN-CMT-7 处置。放弃时限是 controller 的参数，协议层无界。

**CLD-CTL-4** controller 的所有动作都是幂等的端口调用；两个 controller 实例同时运行只造成重复调用，不造成状态分歧。单实例部署即可，leader 选举为可选。

### 2.6 gateway

**CLD-GWY-1** gateway 是用户面：认证、把用户输入转为 Session 命令（submit、stop、withdraw、retry，APP-INB-2）、把 `observe.Bus` 的事件推送给客户端（SSE 或 WebSocket）。它不持有 Session 状态；对共享存储只做两件事：写命令 inbox（`inbox.Store.Enqueue`）与读租约（SES-OWN-5）。认证与限流只在 gateway；owner 的命令面（`agent/app/http`）不做认证，只在集群内网暴露。

**CLD-GWY-2（命令投递）** 命令的正确性来源是 inbox 与幂等应用（APP-INB-1/2），路由到当前 owner 是延迟优化。gateway 的路径：把命令 `Enqueue` 到任一 owner 副本，该副本无持有者时激活 Session（APP-ACT-1，Open 内部先应用 pending 命令，APP-INB-3），有存活持有者时命令由持有者的轮询应用；可选的延迟优化是读 `LeaseOf(sid)`，有存活租约则把命令直接发给该副本或向它发一次唤醒。唤醒失败不重试、不向客户端报错：owner 的轮询兜底。需要确认结果的命令（stop）由 gateway 以 `AwaitCommand` 等待 Result，超时返回"已受理"。`OpenOptions.Owner` 记录副本的可寻址名（headless Service 下的 pod DNS 名，由 downward API 挂载的文件提供），供唤醒定位。

**CLD-CMD-1（崩溃情形）** gateway 在 Enqueue 前崩溃：客户端以同一 CommandID 重发。gateway 在 Enqueue 后、唤醒前崩溃：命令已 durable，owner 轮询拾取。owner 读到命令后、提交前崩溃：条目仍 pending，租约过期后任一副本的扫描（APP-ACT-3）接管并在 Open 中应用，不依赖 controller。owner 提交后、Resolve 前崩溃：新 owner 重放，ledger 报已应用或冲突，写 Result。唤醒发到已被替代的副本：其 Append 被 Epoch 围栏拒绝，不 Resolve。gateway 读到过时租约：唤醒落空，轮询兜底。

**CLD-CMD-2（幂等键）** submit 与 withdraw 以 InputID 为键（`chatlog.Submit` 重放为 `CommitAlreadyApplied`）；stop、retry 以 TurnRef 加命令 ID 为键，状态已推进时返回 conflict，作为 rejected 关闭。Fork 尚未进入命令集合：需先确定子 SessionID 由命令 ID 派生。

**CLD-CMD-3（替代方案）** 把命令作为无围栏事件直接追加进 Session ledger（仿 execution store 的 `Fenced(eventType)` 区分）可以省去独立存储，但要修改 SES-OWN-2 的"所有 Append 携带 Epoch"，并引入 gateway 追加与 owner 提交的 Seq 竞争重试。当前选择独立 inbox；两者对 owner 侧应用逻辑的要求相同。

**CLD-CMD-4（激活索引）** `inbox.Store.Sessions(limit)` 返回有 pending 命令的 Session，是无 owner 的 Session 需要被 Open 的依据之一（另一个是 `ExpiredLeases`，SES-OWN-5）。`Application` 的扫描（`Activation.Scan`，APP-ACT-3）据此激活 Session；重复 Open 由 `ErrOwned` 与 Epoch 围栏裁决，索引允许最终一致。

### 2.7 共享存储

**CLD-STO-0（adapter 的位置与验证方式）** store adapter 是部署产物，放在部署侧目录（`agent/store/sqlite`、`agent/store/postgres`），不放在 `agentcore`。`agentcore` 内每个窄合同带一个参考实现与一份 conformance suite，位于该合同包下的 `xxxtest` 包：`executor/store/storetest`（`Map` + `Run`，含 `SeedCommits` 的 tombstone 检查）、`process/processtest`、`checkpoint/checkpointtest`、`artifact/artifacttest`（`MapBindings`、`MapLedger`、`Stores`）。kernel 自身的测试只依赖这些参考实现；adapter 在自己的包内以 `Run(t, factory)` 跑同一套 suite 证明合同。Session store 的合同宽（`LedgerStore` 10 个方法 + `SessionStore` 8 个方法，另有 projection cache 与 content store），不设内存参考实现：`agentcore/session/filestore` 是纯 Go、无外部依赖的 JSONL 实现，留在 `agentcore` 作为 Session 合同的参考实现，`sessiontest`、`runtimetest`、`turntest` 三份 suite 在其上运行。

**CLD-STO-1（共享存储实现）** durable store 有三组实现：SQLite（`agent/store/sqlite`：execution ledger 与租约、dispatch ledger、artifact binding 与 claim、checkpoint、inbox、workspace 记录）与 filestore（`agentcore/session/filestore`：Session ledger、CAS 正文）依赖本地文件与文件锁，只用于单机；Postgres（`agent/store/postgres`）是跨 Pod 共享的实现，承载全部合同：

| 接口 | 表 | 实现要点 |
|---|---|---|
| `session.Backend`（`LedgerStore` + `SessionStore` + `CreateSession`） | `session_segments`、`session_commits`、`session_roots` | `DB.Sessions()` 返回 `session.NewLedger(backend)`，kernel 的 Epoch 围栏、lineage、fork、stream 规则不在 adapter 内复制；`Append` 在 Session 的事务锁内重读 root 行校验 `owned` 与 epoch（SES-OWN-2）；`Committed`/`LookupCommit` 经 `(segment, commit_id)` 唯一索引点查询，`StreamHead` 经 `session_commit_streams`（每 commit 每流一行，与正文同事务写入）按流求和，Open 只取索引摘要（`COUNT/MIN/MAX`），全量 `Index` 只剩 `Collect` 使用，`PutIndex` 无需持久化 |
| `extension.ProjectionCache` | `projection_cache` | `SessionStore` 实现 `ProjectionCacheProvider`，`owner.New` 自动选用：Session 在另一副本重开时从保存的投影状态起折叠，只折叠尾部 commit（EXT-PRJ-3、EXT-PRJ-7 的 `CacheEvery` 定落后上限）；UPSERT 带 `WHERE projection_cache.through < EXCLUDED.through`，晚到的旧写入不回退条目；`Delete` 连带删除 |
| `executionstore.Store` | `executions`、`execution_commits`、`execution_leases` | `Acquire` 一个事务内 fold、读写租约行、追加 claimed；`ListOwned` 走 `execution_leases(owner)` 索引 |
| `process.Store`、`checkpoint.Store`、`inbox.Store` | `processes`/`process_commits`、`checkpoints`、`inbox` | 与 SQLite 同语义 |
| `artifact.BindingStore`、`artifact.RetentionLedger` | `bindings`、`claims` | 与 filestore 同语义；`ClaimsByOwner` 按 `claims(owner_kind, owner_authority, id)` 索引以页大小为批次读取（`LIMIT`），identity 规则在 Go 侧过滤，一批不满一页则读下一批；watermark 由同一索引倒序分批求得 |
| `artifact.ContentStore`（CAS 正文） | `content`（`BYTEA`） | `DB.Content(authority)`；digest 去重、durability 只升不降。正文上限为 `DefaultMaxContentBytes`，对象存储后端留作后续 |
| `workspace.Store` | `workspaces`、`workspace_snapshots` | `UpdateRuntime` 为 generation CAS，`UpdateSnapshot` 为部分写 |

SQL 保持简单：`SELECT`/`INSERT`/`UPDATE`/`DELETE`/`LIMIT` 与唯一约束、索引、条件 UPSERT；事务编排、fold、围栏与协议语义在 Go。不用 trigger、存储过程、大型 CTE，不以 JSONB 查询事件正文。技术栈为 pgx/v5 + sqlc + 手写 migration：`agent/store/postgres/queries/*.sql` 经 sqlc 生成 `internal/db`（只负责 SQL 到类型化调用与行映射），`agent/store/postgres/*.go` 持有事务边界、围栏、CAS 与幂等；生成代码不作为任何 store 合同暴露。每个写事务以 `pg_advisory_xact_lock(hashtext(key))` 按逻辑键（一个 Session、一个 execution、一个 workspace）串行化，同一键上的副本不会交错；Seq 唯一约束（23505）是第二道防线，映射为各合同的 ErrConflict。migration 内嵌于二进制，版本取自文件名前缀（`NNNN_name.sql`，从 1 连续，重复或跳号在 `Open` 时报错），`Open` 时按 `twilight_schema` 表补齐。首次部署之前 schema 变更直接并入 `0001_init.sql`，不保留中间版本；第一个共享数据库上线后再以新文件递增。conformance 由 `agent/store/postgres/postgres_test.go` 在 `-postgres.dsn` 指向的数据库上运行全部 suite（含 `sessiontest`、`runtimetest`、`turntest`），每个测试建独立 schema；无 DSN 时跳过。组件经 `config.Store{sqlite | postgres{dsn | dsnFile}}` 选择实现（`agent/component/stores`），owner 配置 `stores.postgres` 时 Session ledger 与 CAS 正文也来自该数据库，`sessions.root`/`content.root` 不再使用；DSN 含凭证，部署以 `dsnFile` 指向挂载的 Secret 文件。

**CLD-STO-2（接口审查结果）** 审查项全部在 Postgres 语义下满足：`executionstore.Store.Acquire` 的租约事务在一个 `pgx.BeginFunc` 内完成；Seq 0 的 acceptance 与 abort 竞争由 `execution_commits(key, seq)` 主键与事务锁共同裁决（RUN-EXE-16）；`ListOwned` 走租约行的 owner 索引；Session ledger 的 `Append` 以 `(segment, seq)` 与 `(segment, commit_id)` 两个唯一约束实现 ErrConflict；CAS 以 `(authority, key)` 主键去重。未做的项：内容大对象走对象存储、按 Session 的通知（跨副本的 inbox 唤醒仍靠轮询，APP-INB-3）。

**CLD-STO-4（重开路径基准）** `agent/store/postgres/bench_test.go` 在 `-postgres.dsn` 指向的数据库上测量激活模型的每 Turn 成本（APP-ACT-5），Session 的 tip 段含 1k / 10k / 100k 个单事件 commit。2026-09-27 本机（Apple Silicon，Postgres 18 容器，`-benchtime 3x`），句柄不装载 tip 段索引之后：

| 操作 | 1k | 10k | 100k | 说明 |
|---|---|---|---|---|
| `Store.Open` + `Close` | 5.6 ms | 5.1 ms | 17 ms | 索引摘要一次聚合、root 行两次写事务；改前 100k 为 67 ms |
| CommitIndex 全量扫描 | 0.9 ms | 6.3 ms | 48 ms | 只剩 `Collect` 使用 |
| Writer 打开，无缓存 | 14 ms | 44 ms | 318 ms | 折叠全部 commit |
| Writer 打开，缓存覆盖到 head | 6.6 ms | 7.5 ms | 38 ms | 折叠 0 个事件；100k 的余量为缓存状态解码（约 1.2 MB）；改前 112 ms |
| 投影缓存 Save（状态 n 行） | 2.2 ms | 3.5 ms | 10 ms | 每 `CacheEvery` 个 commit 一次 |
| claims 分页一页（1/16 匹配，页 64） | 2.8 ms | 3.9 ms | 4.7 ms | 与 claim 总数无关 |

结论：Open 与 tip 段长度无关；claims 分页与规模无关。100k commit 的暖重开剩余成本来自随历史增长的投影状态（chatlog surface）的解码，归档方案（让 fold 经 ledger 事实合法遗忘旧条目）待真实负载证明后再做。100k 的 `Store.Open` 高于 10k 的部分来自聚合 `COUNT/MIN/MAX` 走主键索引的行数。

**CLD-STO-3（租约读取）** controller、gateway 与 owner 扫描读取 Session 租约的接口为 `session.Store.LeaseOf`、`ListLeases` 与 `ExpiredLeases`，定义于 SES-OWN-5（`agent-session.md`），filestore 与 Postgres 均已实现；Postgres 的 `ExpiredLeases` 走 `session_roots(lease_until) WHERE owned` 部分索引。

## 3. Worker 与 Backend 的 wire

**CLD-WIR-0（绑定的位置）** 传输绑定与存储 adapter 同为部署产物（CLD-STO-0）：`effect.ExecutionPort` 的 HTTP server/client 对在 `agent/executor/http`，Backend 协议的适配器对在 `agent/executor/backendhttp`，owner 命令面（`app.Application` 的 Enqueue、AwaitCommand、Turn 状态、Fork、事件流）的 HTTP 绑定放 `agent/app/http`（尚未建立）。`agentcore` 只保留协议值类型与版本（`executor/protocol`，它们被写入 execution ledger）、通知环（`executor/notice`）与进程内实现（`Worker`、`LocalExecutor`、`PortBackend`）。`agentcore` 的测试经直接 port 覆盖 Worker 语义，绑定的测试随绑定包放在 `agent/` 下，用自己的测试替身。

**CLD-WIR-2（ExecutionPort 的 HTTP 绑定规则）** HTTP Server 只接受 POST：其他方法返回 405，请求体超过 `MaxBodyBytes`（默认 16 MiB）返回 413，非 JSON 请求体返回 400，三者都在 Worker 之前拒绝。`/dispatch` 的状态码承载上述三分类：202 接受（含 backend 侧 Unknown，Worker 已持久化 acceptance）；400 确定拒绝、409 内容冲突或 key 已 `aborted`（客户端返回普通 error）；503 为 `ErrDispatchRetryable`（客户端原样归类；未把请求转发到 server 的中间层同样回答 503）；server 的 dispatch handler 不写其他 4xx，客户端因此把其他 4xx（中间层的 429、408、404 等）归为 `ErrDispatchRetryable`：Assignment 未被 server 判定，同一 Dispatch 稍后可再发；其他 5xx 与传输失败客户端归为 `ErrDispatchUnknown`，server 自身从不返回 500。`GetOutcome` 未结算为 204，无 record 为 404，永不可读为 410；`Acknowledge` 对非终态 record 为 409（RUN-EXE-13）；结算通知流为 `/settlements`，Backend 侧通知流为 `/notices`（RUN-EXE-17）。

**CLD-WIR-1（Worker 与 Backend 的 wire，已定）** model backend 与 tool sandbox backend 都是远端后，Worker 进程内不再有任何 backend，`ExecutionBackend` 必须有一条 wire。两个方案曾被比较：

方案 A，Worker 链。backend 自身是一个 Worker，前置 Worker 经 `executor.PortBackend` 把远端 `ExecutionPort` 适配为 backend。不需要新协议，进度与结算通知经 `PortBackend` 中继。代价：同一 effect 在两级 Worker 各有一条 ledger 与租约，模型与工具都远端后为三份；backend 侧需要 `executionstore.Store`；`Restart` 对 Port-shaped backend 返回同一 Ref，RUN-EXE-9 的新一代 Ref 语义在链上退化。

方案 B，Backend 协议。把 `ExecutionBackend` 直接做成 HTTP 协议：`/prepare`、`/start`、`/restart`、`/attach`、`/status`、`/cancel` 按 Ref；`Outcome` 不再是阻塞读（现有接口注释"阻塞到终态"在网络上重现 RUN-EXE-17 解决过的问题），改为 `/outcome` 纯读加 backend 侧的结算通知流，Worker 的 watch 循环订阅它；进度帧由 Worker 从 backend 的 `/progress` 拉取并发布到自己的 `ProgressHub`。backend 无 ledger，只有 in-flight 表；ledger 只在 Worker 一处。

选定方案 B：`ExecutionBackend.Outcome` 在 Go 接口上即为一次读取（RUN-EXE-17），Backend 可选实现 `notice.Source` 提供按 Ref 的结算通知流，等待归 Worker；Backend 协议只是一对无状态适配器（`agent/executor/backendhttp`）。`Server` 把一个进程内 backend 暴露为 `/validate`、`/prepare`、`/start`、`/restart`、`/attach`、`/status`、`/outcome`、`/cancel`、`/progress`、`/notices`，每个端点都是对 backend 的一次调用，Server 自己不持有任何表：`/outcome` 转发 backend 的读取（未结算为 204，未知 Ref 为 404，永不可读为 410），`/notices` 把 backend 的 `Settled` 流以 SSE 转发（首个事件前的淘汰为 410；backend 无通知源时回答立即结束的空流）。`Client` 实现 `ExecutionBackend`、`effect.ProgressPort` 与 `notice.Source`，同样无状态：`Outcome` 是一次 `/outcome` 读，`Settled` 是一次 `/notices` 流，谁在等由驱动它的 Worker 决定，Worker 对每个 Backend 只保持一条 `Settled` 订阅。`Start` 的 400 为 backend 的确定拒绝，传输失败与其他状态为 `ErrDispatchUnknown`；`Attach` 的传输失败为 error 而非观察。local agent 不经这条 wire：Worker 进程内直接挂 `LocalExecutor`，一行不改。ledger 只在 Worker 一处（RUN-EXE-10），backend 无状态，Server 重启后 backend 对它持有过的每个 Ref 回答 missing，Worker 按 RUN-EXE-9 重派或按 Replay 声明结算。

**CLD-WIR-2** 无论哪个方案，`Prepare` 都不得在 backend 侧分配资源（RUN-EXE-3 与 RUN-EXE-16：Abort 可能抢先），资源分配在 `Start`。

## 4. 开发集群

**CLD-DEV-1** 开发与单机检验使用 k3d。一个 namespace 内：worker Deployment 2 副本、model-backend Deployment 1 副本、tool-backend Deployment 1 副本、owner Deployment 1 副本（多副本在 CLD-GWY-2 的路由完成后）、gateway 1 副本、controller 1 副本、Postgres StatefulSet 1 副本，对象存储可选（MinIO）。Worker 以 headless Service 暴露，供 owner 枚举副本（CLD-OWN-3）。

**CLD-DEV-2（故障检验）** 集群上要检验的三类故障，每类对应协议中已定义的恢复路径：

| 故障 | 期望路径 |
|---|---|
| 删除持有效果租约的 worker Pod | owner 的 Watcher 在 `OrphanProbe` 内观察到 orphaned，`RecoverExecution` 由另一副本接管；backend 侧执行仍在则 Attach 后继续观察，结算经新副本的 `/settlements` 到达 |
| 删除持有 Session 租约的 owner Pod | 租约过期后另一副本的扫描接管（APP-ACT-3，无 Takeover），`RecoverInterrupted` 对 Executing 效果 Attach 并 keep/defer/dispose |
| 删除 backend Pod | Worker 的 watch 读到 Attach missing：模型 Assignment 经 `Restart` 重派为新一代；工具 Assignment 按 Replay 声明重派或以 Unknown 结算 |

## 5. 顺序

1. CLD-WIR-1 的 Backend 协议适配器对（`agent/executor/backendhttp`）：已完成。
2. 四个二进制与单机多进程验证：已完成（CLD-CMP-4）。此步仍用 SQLite 与 filestore，各进程共享同一文件路径只用于单机验证。
3. CLD-STO 的 Postgres 实现：已完成（CLD-STO-1、CLD-STO-2）；对象存储后端未做。
4. k3d 清单与 CLD-DEV-2 的故障检验。
5. controller 与 gateway。

## 6. 未决


- `OpenOptions.Owner` 到网络地址的映射方式（CLD-GWY-2）：写入租约行，或由 owner 副本向注册表登记。
- preset 注册表是否共享化（CLD-CMP-3）。
- tool sandbox 的隔离粒度（按 session 还是按 tenant）与 workspace 的持久化位置。
