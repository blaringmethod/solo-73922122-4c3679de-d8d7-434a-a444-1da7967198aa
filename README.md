# 机构库书目采集（OAI-PMH ListRecords / oai_dc）

一套可实际启动的采集演示：

| 服务 | 地址 | 说明 |
| --- | --- | --- |
| 采集网页 | http://localhost:8000 | 目录浏览/搜索、进度、开始/取消/取消并重开、轮次记录 |
| 演示 OAI 源 | http://localhost:8001 | OAI-PMH 端点 `/oai`，控制面板 `/`（慢页、令牌失效、撤回等） |

```bash
docker compose up --build
```

首次启动后打开采集网页，点“开始新一轮”即为首次全量采集（无 `from`，`until` 为源端 `Identify` 返回的当前 UTC 秒）。之后每轮都以“上次成功水位 → 源端当前时刻”的闭区间取数。

## 业务约定与实现

### 时间窗
- 每轮开始时调用 `Identify`，把 `<responseDate>` 规范化为秒级 UTC，作为 `until`。
- `from` 固定取“上次成功提交的水位”；首次全量为 `null`，请求中不带 `from`。
- 两端都是闭区间（OAI `from`/`until` 语义）。边界那一秒会被重复读到，但暂存区
  对同一标识只保留一条、提交写入是幂等的，因此不会产生重复或丢数据。
- 若源端当前时刻早于已发布水位（时钟回拨），本轮直接失败，不构造倒置时间窗。

### 分页与令牌
- 每轮从窗口第 1 页开始，沿 `resumptionToken` 取到没有令牌为止。
- 令牌请求只带 `resumptionToken`，不再带 `from`/`until`，分页严格绑定最初的时间窗。
- 一旦源端返回 `badResumptionToken`（过期/注入失效），或分页途中
  `completeListSize` 发生变化（结果集在翻页时变动会让游标分页漏记录）：
  1. **整次尝试的暂存记录全部删除**（`run_records` 按 `run_id` 清空），
     不会把已取到的旧分页与新结果混在一起；
  2. 用**原来的 `from`/`until`** 从第 1 页重新取；
  3. 最多 `HARVEST_MAX_ATTEMPTS`（默认 3）次；仍失败则整轮失败、不发布。
- 记录时间戳必须落在窗口内；越窗记录会让本轮失败（宁可不发布，也不污染目录）。
  分页中途出现 `noRecordsMatch` 同样按协议异常处理。

### 一条标识一条书目，撤回即删除
- 源端 `header/@status="deleted"` 的记录会从已发布目录中删除对应标识。
- 暂存表主键是 `(run_id, identifier)`，同一轮内重复出现时保留 `datestamp` 更新的
  一条（相同时间戳保留后出现的）。
- 成功提交时对非删除记录做 UPSERT，因此“同一标识只保留一条”在目录表层面也成立。

### 提交的原子性
- `commit_run()` 在**一个 `BEGIN IMMEDIATE` 事务**里：校验本轮仍是活动轮次 →
  把暂存记录写入/删除 `catalog` → 更新 `published_state.watermark` → 标记本轮成功
  → 清理暂存。任何一步抛错都会 `ROLLBACK`，目录、水位、成功计数都不变。
- 失败、取消、进程崩溃都不会改变已发布内容：
  - 失败/取消：`finish_failed` / `finish_cancelled` 只改本轮状态并清空暂存；
  - 进程崩溃：下次启动时 `Store` 把所有残留的 `running` 轮次标记为失败并清空暂存，
    同时发布状态完全不动。

### 取消：不等待慢页，也不等待外部写锁
- `POST /api/harvest/cancel`：先在内存里给该轮立”取消墓碑”（tombstone）并取消
  asyncio 任务——**从这一刻起该轮不能再取数、暂存或发布**，在途写入立即停止；
  随后尽力把 `cancelled` 写入 SQLite（本地事务，毫秒级）。
- 若另一 SQLite 连接正持有写锁，落库会快速失败（`busy_timeout` 默认 250ms，
  **不延长等待**），接口立即返回 `{“status”: “cancelling”}`；后台收尾任务每隔
  ~250ms 重试，**锁释放后自动完成**：清空暂存、状态落库，此后才视为取消完成
  （状态接口的 `phase` 从 `cancelling` 变为 `cancelled`）。无需重启进程即可重采。
- 锁错误以外的写入错误不重试、不吞掉：记录日志并留给下次启动时的恢复流程。
- `POST /api/harvest/restart`（网页上的”取消并立即重开”）：无锁时取消落库是即时的，
  新轮次立刻开始；有外部写锁时会短暂等待旧轮收尾落库（默认最多 5 秒，
  `HARVEST_RESTART_WAIT_SECONDS`），等不到则返回 409 提示稍候重试。
- 旧轮即便迟到：所有写入都以 `run_id` 为键；提交事务会再次校验
  `active_run_id == run_id`、状态仍为 `running` 且没有取消墓碑，否则直接
  `ROLLBACK` 拒绝发布，更不会触碰新轮次的状态/进度。
- 数据库层还有 `WHERE status='running'` 的唯一索引，跨进程也最多一个活动轮次。

### 外部写锁下的可用性
- 采集器用**两条 SQLite 连接**：写连接串行所有变更；目录、进度、轮次查询走
  独立的只读连接。WAL 模式下读者不等待写者，因此另一连接持有写锁时，
  网页的目录浏览、进度轮询和取消受理都不被拖住。
- 写连接的 `busy_timeout` 默认 250ms（`SQLITE_BUSY_TIMEOUT_MS`）：外部写锁让
  写入**快速失败并如实报错**，由后台收尾重试兜住，而不是把事件循环卡住数秒。
- 后台采集任务的写入若撞上外部写锁，本轮失败并记录 `database is locked`；
  失败收尾同样由后台任务重试到落库为止，之后即可重采。

### 进度可见性
- 网页每秒轮询 `/api/status`：阶段、时间窗、已取页数、已取记录 / `completeListSize`、
  令牌失效重取次数、当前尝试次数、已暂存唯一标识数、最近错误。
- 前端轮询用“代号”防止旧响应覆盖新渲染；轮次开始/结束时整页刷新，使目录和水位
  立即反映已发布结果。

## 演示源的能力
- `Identify` / `ListRecords`（`metadataPrefix=oai_dc`）/ `ListMetadataFormats`；
- 秒级 UTC `datestamp`，闭区间 `from`/`until`，`resumptionToken` 分页；
- 控制面板（`http://localhost:8001/`）：
  - **慢页**：`slow_seconds`，每个响应延迟 N 秒（取消演示）；
  - **令牌失效**：`token_failures`，接下来 N 次令牌调用返回
    `badResumptionToken`，并使所有未用令牌作废（重取演示）；
  - 分页大小、令牌 TTL（设为 0 可让令牌立即过期）；
  - 新增、更正（更新 `datestamp`）、撤回（`status="deleted"`）、恢复、撤回全部。
- 演示数据保存在独立 SQLite 卷 `demo-data`，采集网页保存在 `harvester-data`。

## 配置（docker-compose.yml）
| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `OAI_URL` | `http://demo-oai:8001/oai` | 源端地址 |
| `OAI_READ_TIMEOUT` | `20` | 单页读取超时（秒） |
| `HARVEST_MAX_ATTEMPTS` | `3` | 令牌失效后最多重取次数 |
| `HARVEST_RETRY_BACKOFF_MS` | `300` | 重取前等待 |
| `SQLITE_BUSY_TIMEOUT_MS` | `250` | 写连接遇外部写锁的等待上限（超时快速失败，后台收尾重试） |
| `HARVEST_RESTART_WAIT_SECONDS` | `5` | "取消并立即重开"等待旧轮收尾落库的上限 |
| `DEMO_PAGE_SIZE` | `3` | 演示源初始分页大小 |
| `APP_PORT` / `DEMO_OAI_PORT` | `8000` / `8001` | 宿主机端口 |

## 本地开发与测试
```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt

DATA_DIR=./data uvicorn demo_oai.main:app --port 8001
DATA_DIR=./data OAI_URL=http://127.0.0.1:8001/oai uvicorn app.main:app --port 8000

pytest -q
```
测试使用 `httpx.ASGITransport` 在同一进程内调用真实的演示 FastAPI 应用，
覆盖 XML 解析、时间窗、分页/令牌失效重取、撤回、原子提交、取消/重开、
迟到响应、进程崩溃恢复等。
