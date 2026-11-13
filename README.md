# LIMS 仪器批次 CSV 导出采集 API

实验室数据员通过本服务把仪器门户（instrument portal）的批次原始检测 CSV
导入 LIMS。服务负责：提交门户网页导出表单、读取门户任务表、提供进度查询，
并在文件完整校验通过后提供下载。

## 设计目标与安全约定

1. **并发/重复调用共用任务**：以 `(portal_url, batch_no)` 为唯一键，
   数据库有唯一约束 + 进程内异步锁双重去重；同一门户同一批号永远只有一个任务。
2. **服务重启可接续**：所有状态机迁移先落 SQLite 再做网络动作，重启后
   启动恢复逻辑只依据数据库行重建后台工作，不依赖内存状态。
3. **受理后响应断开 / 提交途中重启**：绝不重提表单。转入**对账
   (reconciliation)** 流程查询门户任务表：
   - 该批号**恰好一笔**任务 → 接续该任务；
   - **查不到**或**存在多笔** → 标记 `MANUAL_REVIEW`（待人工核实），
     不自动重提，避免门户侧产生重复导出。
   - 门户暂时不可达（正在重启）时，对账只重试**查询**，带退避与上限。
4. **下载失败只重试原任务**：每次重试都使用数据库中存储的同一个
   `portal_task_id`；文件流式下载到临时目录，校验 Content-Length 与
   SHA-256 通过后才原子改名发布。不完整文件永不提供下载，每次下载前还会
   再次校验摘要与大小。

## 目录结构

```
.
├── portal/            # 本地仪器门户（无需账号，FastAPI 演示服务）
│   ├── app/main.py    # 导出表单、任务表、文件下载、故障注入开关
│   ├── Dockerfile
│   └── requirements.txt
├── collector/         # LIMS 采集 API（FastAPI + SQLite）
│   ├── app/
│   │   ├── main.py          # HTTP 路由
│   │   ├── manager.py       # 任务状态机（提交/对账/轮询/下载/人工核实）
│   │   ├── database.py      # SQLite 持久化（aiosqlite，WAL）
│   │   ├── portal_client.py # 门户 HTTP 客户端（httpx，流式下载）
│   │   ├── schemas.py       # 请求/响应模型
│   │   └── config.py        # 环境变量配置
│   ├── Dockerfile
│   └── requirements.txt
├── tests/             # 端到端 pytest（以独立子进程启动两个服务）
├── demo/              # 演示脚本
└── docker-compose.yml
```

## 快速开始

```bash
docker compose up --build
```

- 门户： http://localhost:8000 （浏览器可直接打开导出表单，无需登录）
- 采集 API： http://localhost:8001
- 交互文档： http://localhost:8001/docs

两个服务的数据都在命名卷中，重启容器不丢状态。

### API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/jobs` | 创建/复用采集任务，body 含 `portal_url`、`batch_no`、可选 `options` |
| GET | `/jobs` | 任务列表，可按 `status` 过滤 |
| GET | `/jobs/{id}` | 查询单个任务进度 |
| GET | `/jobs/{id}/attempts` | 全链路尝试日志（提交/对账/轮询/下载） |
| GET | `/jobs/{id}/download` | 下载完整校验后的结果文件（未完成返回 409） |
| POST | `/jobs/{id}/adopt` | 人工核实后指定要接续的门户任务 id |
| GET | `/health` | 健康检查与本次启动恢复的任务数 |

任务状态：`SUBMITTING → RECONCILING → POLLING → DOWNLOADING → COMPLETED`，
任何无法安全自动处理的情况进入 `MANUAL_REVIEW`（带 `review_code` 与
`review_reason`）。

### 门户故障注入开关（无需账号）

提交表单或 `options` 中可带：

| 开关 | 行为 |
| --- | --- |
| `slow` | 慢速生成 CSV（约 6 秒），可观察进度推进 |
| `disconnect` | 受理并写入任务表后丢弃 HTTP 响应、关闭连接 |
| `crash` | 提交途中 `SIGKILL` 门户进程（compose 会自动拉起） |
| `duplicate` | 为同一批号立即创建第二笔任务 |

## 演示

```bash
# 场景 1-3：慢生成、受理后断线接续、多笔任务人工核实
./demo/demo.sh

# 场景 4：采集服务重启后的持久化恢复
./demo/restart-demo.sh
```

### 手动示例

```bash
# 慢生成
curl -s -X POST localhost:8001/jobs -H 'Content-Type: application/json' \
  -d '{"portal_url":"http://portal:8000/","batch_no":"B2026-001","options":{"slow":true}}'

# 轮询
curl -s localhost:8001/jobs/<job_id> | jq .

# 下载（COMPLETED 前返回 409）
curl -OJ localhost:8001/jobs/<job_id>/download
```

## 状态机与恢复细节

- **SUBMITTING**：POST 门户 `/export`。
  - 正常响应拿到任务 id → `POLLING`；
  - 连接错误/协议错误/超时（响应可能已被受理）→ `RECONCILING`；
  - 门户明确 4xx/5xx 拒绝 → `MANUAL_REVIEW(PORTAL_ERROR)`。
- **RECONCILING**：GET 门户 `/tasks?batch_no=...`，只做查询、带退避重试。
  一笔 → 接续；零笔 → `TASK_NOT_FOUND`；多笔 → `DUPLICATE_TASKS`。
- **POLLING**：周期 GET `/tasks/{id}`，门户 `DONE` 后进入下载；
  门户 `FAILED` → `MANUAL_REVIEW(PORTAL_TASK_FAILED)`；连续查询失败超过
  上限 → `MANUAL_REVIEW(PORTAL_UNREACHABLE)`。
- **DOWNLOADING**：流式下载到 `*.part`，长度与 SHA-256 校验通过后原子
  改名；失败按退避重试且始终针对原任务 id，超过上限 →
  `MANUAL_REVIEW(DOWNLOAD_FAILED)`。
- **重启恢复**：启动时把所有非终态任务重新调度；卡在 `SUBMITTING` 的任务
  一律转为 `RECONCILING`（因为无法确认门户是否受理，禁止重提）。

## 测试

```bash
python -m pytest tests/ -q
```

测试以独立子进程启动门户与采集服务，覆盖：慢生成全流程与下载校验、
并发去重、断线对账不重提、多笔/零笔任务人工核实、采集服务重启恢复、
完成前禁止下载、人工 adopt 后续接。

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `COLLECTOR_DB` | `/data/collector.db` | SQLite 路径 |
| `RESULT_DIR` / `TMP_DIR` | `/data/results` / `/data/tmp` | 成品与临时目录 |
| `POLL_INTERVAL` | `1.0` | 门户状态轮询间隔（秒） |
| `RECONCILE_MAX_ATTEMPTS` / `RECONCILE_BACKOFF` | `10` / `2.0` | 对账查询重试上限与退避基数 |
| `DOWNLOAD_MAX_ATTEMPTS` / `DOWNLOAD_BACKOFF` | `5` / `2.0` | 下载重试上限与退避基数 |
| `POLL_ERROR_LIMIT` | `30` | 连续轮询失败多少次后转人工 |
| `JOB_TIMEOUT_SECONDS` | `3600` | 单任务墙钟保护 |
| `HTTP_TIMEOUT` | `15.0` | 门户 HTTP 超时 |
