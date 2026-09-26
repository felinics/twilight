# 单机多进程运行

四个二进制各读一个 JSON 文档（`agent/config`，不读环境变量），共享 `./var` 下的文件：owner 与 tool-backend 共享 `owner.db` 的 workspaces 表，worker 独占 `executions.db`，Session ledger 与 CAS 正文在 filestore 目录。这只用于单机验证。跨进程与跨 Pod 共享时把各文档的 `{"sqlite": ...}` 换成 `{"postgres": {"dsn": "postgres://..."}}`（或 `dsnFile` 指向含 DSN 的文件）；owner 配置 Postgres 后不再需要 `sessions`/`content` 两个目录，Session ledger 与 CAS 正文都在该数据库（CLD-STO-1）。

准备：

```sh
mkdir -p var/secrets var/envs var/sessions var/content
# 模型目录：deploy/local/models.json 按 agent/models.File 的格式写，凭证名指向 var/secrets/<name> 文件
```

启动（四个终端，或加 `&`）：

```sh
go run ./cmd/model-backend -config deploy/local/model-backend.json
go run ./cmd/tool-backend  -config deploy/local/tool-backend.json
go run ./cmd/worker        -config deploy/local/worker.json
go run ./cmd/owner         -config deploy/local/owner.json
```

owner 配置了 `activation`（APP-ACT）：命令到达即打开 Session，Session 静止 `idleRelease` 后释放所有权，`scan` 周期拾取无 owner 的 pending 命令与过期租约。下面的 `open` 一步因此可以省略；多副本 owner 共享 Postgres 时，同一 Session 的相邻 Turn 可以落在不同副本。

驱动一轮对话（owner 的命令面，`agent/app/http`）：

```sh
O=http://127.0.0.1:8080
curl -X PUT  $O/sessions/s1
curl -X POST $O/sessions/s1/open -d '{"preset":"default"}'
WS=$(curl -s -X POST $O/workspaces -d '{"project":"demo"}' | sed 's/.*"id":"\([^"]*\)".*/\1/')
curl -X POST $O/sessions/s1/commands -d "{\"id\":\"c1\",\"kind\":\"bind_workspace\",\"payload\":{\"workspaceId\":\"$WS\"}}"
curl -X POST $O/sessions/s1/commands -d '{"id":"c2","kind":"submit","payload":{"inputId":"in-1","text":"list the files here"}}'
curl "$O/sessions/s1/commands/c2?wait=10s"
curl $O/sessions/s1/turns
curl -N $O/sessions/s1/events
```

同一套组装在进程内的检验是 `agent/component/cloudtest`：四个组件各起一个 `httptest.Server`，覆盖 Dispatch、GetOutcome、worker 与 owner 替换后的接管。
