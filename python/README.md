### Python 稽核编排程序（`python/`）README

本目录是 **HDFS 稽核编排器**（Python 侧）。它负责“决定稽核什么、何时稽核、并发如何控制、结果写到哪里”，并调用 Java CLI 工具 `hdfs-counter.jar` 做实际计数。

---

### 核心原理（Python 侧做什么）

Python 程序本质是一个**批处理编排器**：

- **输入来源（任务清单）**（二选一或三选一）  
  - **ClickHouse**：查询调度平台在一个时间窗口内“已成功完成”的 `task_name` 列表（默认方式）  
  - **命令行 `--tasks`**：手动指定 task 列表（覆盖 ClickHouse）  
  - **跳过 ClickHouse `--skip-clickhouse`**：不查 ClickHouse，直接跑 `config.yaml` 里配置的所有任务/表

- **配置匹配**：用 `task_name` 去 `config.yaml` 中匹配对应的“产出表/分区/格式/路径模板”，生成稽核 job 列表

- **执行计数**：对每个 job 调用 `hdfs-counter.jar`（支持并发），拿到行数/文件数/大小等

- **落库**：写入 MySQL `audit_result` 表，便于后续对账、报表、告警

---

### 防漏设计：Watermark 增量拉取（推荐）

如果选择从 ClickHouse 拉任务，为了避免仅靠“now 往回 N 小时”的滑动窗口导致停跑/延迟时漏数，程序支持 **watermark**：

- 每次成功运行后，把本次 ClickHouse 查询窗口的 `end_time` 保存到 `clickhouse_watermark.json`
- 下次运行时，从 `last_end_time` 继续查（查询条件左闭右开：`end_time >= start_time AND end_time < end_time`），时间段无缝衔接
- 为抵抗 ClickHouse 落库/同步延迟，支持 **重叠回扫**（`watermark_overlap_seconds`，默认 600 秒）
- 若 watermark 距离现在太久（例如停跑 3 天），支持 **分片追赶**：单次窗口最多回补 `watermark_max_window_hours`（默认 24h），跑多次追平，避免一次性压力过大

---

### 目录结构（Python 侧）

- `main.py`：主入口，解析 CLI，编排全流程（任务获取 → job 生成 → 执行 → 落库）
- `task_fetcher.py`：ClickHouse 拉任务实现（支持自定义 SQL 模板、时区）
- `config_loader.py`：加载 `config.yaml` / `db_config.yaml`，并生成 job 配置
- `hdfs_counter_client.py`：封装 `hdfs-counter.jar` 调用与结果解析
- `db_writer.py`：写入 MySQL（连接池）
- `watermark_store.py`：watermark 文件读写（原子写入、容错读取）

---

### 环境与依赖

#### Python 版本

建议 **Python 3.10+**（使用了 `zoneinfo`）。

#### 安装依赖

在项目根目录执行：

```bash
pip install -r python/requirements.txt
```

依赖要点：

- `clickhouse-driver`：从 ClickHouse 拉取调度完成任务（如不使用 ClickHouse，可不装）
- `pymysql` + `DBUtils`：写 MySQL（连接池）
- `PyYAML`：解析配置

---

### 数据库初始化（MySQL）

先执行 `scripts/init_db.sql` 创建库表（默认库名 `data_audit`，表 `audit_result`）：

```bash
mysql -h <host> -P <port> -u <user> -p < scripts/init_db.sql
```

---

### 配置文件说明

程序运行依赖两个配置：

- **`config/config.yaml`**：稽核编排配置（调度任务与产出表关系、HDFS 路径模板、格式、分区等）
- **`config/db_config.yaml`**：数据库配置（MySQL、可选 ClickHouse）+ watermark 配置

#### `db_config.yaml` 的 ClickHouse 相关字段

在 `clickhouse:` 下（节选）：

- **`timezone`**：用于把 Python 侧构造的时间窗口与 ClickHouse `end_time` 对齐（如 `Asia/Shanghai`）
- **`query_template`**：查询已完成任务的 SQL 模板（支持 `{start_time}`, `{end_time}`, `{data_date}`）

#### `db_config.yaml` 的 watermark 配置（推荐开启）

- **`watermark_enabled`**：是否启用 watermark（默认 true）
- **`watermark_path`**：watermark 文件路径（相对路径按 `db_config.yaml` 所在目录解析）
- **`watermark_overlap_seconds`**：回扫秒数（默认 600）
- **`watermark_max_window_hours`**：追赶时单次最大窗口（默认 24.0）

watermark 文件内容示例：

```json
{
  "last_end_time": "2026-01-09T13:00:00+08:00",
  "updated_at": "2026-01-09T13:00:02+08:00"
}
```

---

### 运行流程（一步步）

程序执行 `python/main.py` 后：

1. 解析 CLI 参数（日期、并发、是否从 CK 拉任务、watermark 等）
2. 解析 `config.yaml` / `db_config.yaml`
3. 生成 `resolved_date`（默认昨天，或 `--date YYYYMMDD`）
4. 获取 `completed_tasks`（优先级：`--tasks` > `--skip-clickhouse` > ClickHouse > 全配置）
5. 根据 `completed_tasks + resolved_date` 生成 job 列表（表、分区路径、格式、线程等）
6. 并发调用 `hdfs-counter.jar` 统计（并发受 `--concurrency` 和配置 clamp 限制）
7. 将结果写入 MySQL
8. 成功时推进 watermark（若启用且走了 ClickHouse；失败则不推进）

日志输出：

- 控制台 stdout
- 当前工作目录下 `hdfs_audit.log`

---

### 常用命令（CLI）

以下命令均在项目根目录执行为例。

#### 1) 默认：从 ClickHouse 拉最近窗口完成的任务并稽核（默认回看 24 小时）

```bash
python python/main.py
```

#### 2) 指定数据日期（业务日期）

```bash
python python/main.py --date 20260109
```

#### 3) 手动指定任务（跳过 ClickHouse）

```bash
python python/main.py --date 20260109 --tasks dw_user_daily,dw_order_daily
```

#### 4) 跳过 ClickHouse，稽核配置里所有任务/表

```bash
python python/main.py --date 20260109 --skip-clickhouse
```

#### 5) 调整 ClickHouse 回看窗口（支持小数小时）

```bash
python python/main.py --hours-lookback 1.1
```

说明：仅在“使用 ClickHouse 拉任务 且没有 watermark 可用/被禁用”时作为 fallback。

#### 6) 启用/控制 watermark（推荐）

- 指定 watermark 文件路径：

```bash
python python/main.py --watermark-path /data/state/clickhouse_watermark.json
```

- 调整回扫重叠（默认 600 秒）：

```bash
python python/main.py --watermark-overlap-seconds 1800
```

- 限制追赶单次窗口（默认 24h；<=0 表示不限制）：

```bash
python python/main.py --watermark-max-window-hours 12
```

- 重置 watermark（删除文件后重建）：

```bash
python python/main.py --watermark-reset
```

- 冷启动只从“现在开始”，不回补历史（仅当 watermark 不存在时生效）：

```bash
python python/main.py --watermark-init-now
```

#### 7) 并发与 dry-run

- 并发（覆盖配置）：

```bash
python python/main.py --concurrency 10
```

- 只打印 jobs，不执行：

```bash
python python/main.py --dry-run
```

---

### crontab 样例

#### 每小时跑一次（推荐：ClickHouse + watermark）

```bash
0 * * * * cd /path/to/DataAuditOptimize && /usr/bin/python3 python/main.py >> logs/cron.log 2>&1
```

建议：

- 配置 `clickhouse.watermark_path` 到一个持久化目录（如 `/data/state/...`）
- 根据 ClickHouse 延迟调整 `watermark_overlap_seconds`

#### 每天固定跑当天/昨天（不依赖 ClickHouse）

如果你希望**只跑某个日期，不看调度完成清单**：

```bash
10 02 * * * cd /path/to/DataAuditOptimize && /usr/bin/python3 python/main.py --date $(date -v-1d +\%Y\%m\%d) --skip-clickhouse >> logs/cron.log 2>&1
```

---

### 常见问题与排查

#### 1) ClickHouse 一直拉不到任务

- 检查 `db_config.yaml` 的 `clickhouse.query_template` 是否正确（表名/字段/状态值）
- 检查时区：`clickhouse.timezone` 是否与 `task_instance.end_time` 口径一致
- 需要更严格过滤时，可在 SQL 模板中启用 `biz_date = '{data_date}'`（若调度表有该字段）

#### 2) 担心漏数/重复

- 建议开启 watermark（默认已支持）
- 如存在明显落库延迟，增大 `watermark_overlap_seconds`
- 若停跑很久，使用 `watermark_max_window_hours` 控制追赶节奏

#### 3) MySQL 写入失败 / 连接数不足

- 检查 `db_config.yaml` 的 MySQL 配置
- 检查 MySQL 权限/网络
- 视情况增大 MySQL 连接池（`db_writer.py` 默认 `pool_size=5`，可按需扩展）

#### 4) jar 执行失败

- 确认 `--jar` 路径正确（默认 `../java/hdfs-counter/target/hdfs-counter-1.0.0.jar`）
- 确认 `JAVA_HOME` / `HADOOP_CONF_DIR`（需要时用参数传入）
- 查看 `hdfs_audit.log` 中的错误栈与 jar 输出


