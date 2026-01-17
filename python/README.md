# HDFS 数据稽核编排程序（Python）

本目录是 **HDFS 数据稽核编排器**（Python 侧）。它负责"决定稽核什么、何时稽核、并发如何控制、结果写到哪里"，并调用 Java CLI 工具 `hdfs-counter.jar` 做实际计数。

---

## 目录

- [核心原理](#核心原理)
- [系统架构](#系统架构)
- [目录结构](#目录结构)
- [环境依赖](#环境依赖)
- [配置文件说明](#配置文件说明)
- [命令行参数](#命令行参数)
- [运行示例](#运行示例)
- [定时任务配置](#定时任务配置)
- [Watermark 机制详解](#watermark-机制详解)
- [数据库表结构](#数据库表结构)
- [常见问题排查](#常见问题排查)

---

## 核心原理

Python 程序本质是一个**批处理编排器**，完整流程如下：

```
┌──────────────────────────────────────────────────────────────────────┐
│                        HDFS 数据稽核流程                              │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  1. 获取任务清单                                                      │
│     ┌─────────────────┐                                              │
│     │   任务来源       │                                              │
│     │  (三选一)        │                                              │
│     ├─────────────────┤                                              │
│     │ • ClickHouse    │ ← 查询调度平台已完成任务（默认）              │
│     │ • --tasks 参数  │ ← 手动指定任务列表                           │
│     │ • --skip-clickhouse │ ← 跑配置中所有任务                       │
│     └────────┬────────┘                                              │
│              │                                                        │
│              ▼                                                        │
│  2. 配置匹配                                                          │
│     ┌─────────────────┐                                              │
│     │  config.yml     │ ← task_name 匹配表/分区/路径模板              │
│     └────────┬────────┘                                              │
│              │                                                        │
│              ▼                                                        │
│  3. 执行计数                                                          │
│     ┌─────────────────┐                                              │
│     │ hdfs-counter.jar│ ← 并发调用，统计行数/文件数/大小              │
│     │  (ORC/Parquet/  │                                              │
│     │   TextFile)     │                                              │
│     └────────┬────────┘                                              │
│              │                                                        │
│              ▼                                                        │
│  4. 结果落库                                                          │
│     ┌─────────────────┐                                              │
│     │     MySQL       │ ← 写入 audit_result 表                       │
│     └─────────────────┘                                              │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 系统架构

```
DataAuditOptimize/
├── python/                    # Python 编排程序
│   ├── main.py               # 主入口，CLI 解析与流程编排
│   ├── config_loader.py      # 配置加载与 Job 生成
│   ├── task_fetcher.py       # ClickHouse 任务拉取（支持多批次）
│   ├── hdfs_counter_client.py # Java CLI 调用封装
│   ├── db_writer.py          # MySQL 写入（连接池）
│   ├── watermark_store.py    # Watermark 文件存储
│   ├── generate_config.py    # 从模板生成 config.yml
│   └── requirements.txt      # Python 依赖
│
├── java/hdfs-counter/        # Java 计数工具
│   ├── src/                  # 源码
│   ├── pom.xml              # Maven 构建
│   └── target/              # 构建产物
│       └── hdfs-counter-1.0.0.jar
│
├── config/                   # 配置文件
│   ├── config_template.yml  # 配置模板（有展开变量）
│   ├── config.yml           # 生成的配置（勿手动修改）
│   └── db_config.yaml       # 数据库连接配置
│
├── scripts/
│   └── init_db.sql          # MySQL 初始化脚本
│
└── state/                    # 运行时状态
    └── clickhouse_watermark.json  # Watermark 文件
```

---

## 目录结构

| 文件 | 功能描述 |
|------|----------|
| `main.py` | 主入口，解析 CLI，编排全流程（任务获取 → job 生成 → 执行 → 落库） |
| `config_loader.py` | 加载 `config.yml` / `db_config.yaml`，生成 job 配置，支持多周期类型 |
| `task_fetcher.py` | ClickHouse 拉任务实现，支持多批次、batch_no 解析、自动去重 |
| `hdfs_counter_client.py` | 封装 `hdfs-counter.jar` 调用，JSON 结果解析，超时处理 |
| `db_writer.py` | MySQL 写入，使用 DBUtils 连接池，支持多种查询方法 |
| `watermark_store.py` | Watermark 文件读写，原子写入保证一致性 |
| `generate_config.py` | 从模板展开生成 `config.yml`（prov_id 等变量展开） |

---

## 环境依赖

### Python 版本

建议 **Python 3.10+**（使用了 `zoneinfo` 标准库）。

### 安装依赖

```bash
# 在项目根目录执行
pip install -r python/requirements.txt
```

### 依赖说明

| 包名 | 用途 |
|------|------|
| `PyYAML` | 解析 YAML 配置文件 |
| `pymysql` | MySQL 数据库连接 |
| `DBUtils` | 数据库连接池 |
| `clickhouse-driver` | 从 ClickHouse 拉取调度任务（如不使用可不装） |

### Java 环境

- **JDK 8+**：运行 `hdfs-counter.jar`
- **Hadoop 配置**：确保 `HADOOP_CONF_DIR` 或 `HADOOP_HOME` 已设置

---

## 配置文件说明

### 1. `config/config.yml` - 稽核任务配置

由 `generate_config.py` 从模板生成，**请勿手动修改**。

```yaml
defaults:
  data_date: ${yesterday}
  python_concurrency: 5        # Python 并发数
  jar_options:
    threads: 10                # jar 内部线程数
  limits:
    max_python_concurrency: 20
    max_jar_threads: 50
    max_effective_parallelism: 200  # 总并发上限

schedules:
  - task_name: P_TO_D_SVC_UR_USER_5G_TERM_SWITCH_BASIC_ASSET_10100
    interface_id: '03041'
    platform_id: '10100'
    partner_id: '2'
    period_type: daily         # hourly/daily/monthly
    tables:
      - name: ods_bss.TO_D_SVC_UR_USER_5G_TERM_SWITCH_BASIC_ASSET
        hdfs_path: hdfs://beh002/hive/warehouse/ods_bss.db/...
        format: orc            # orc/parquet/textfile
        partition_template: statis_ymd=${data_date}/prov_id=10100
```

**运行时变量：**
- `${data_date}` - 业务日期 (YYYYMMDD)
- `${data_month}` - 业务月份 (YYYYMM)
- `${data_hour}` - 业务小时 (HH)，用于 hourly 任务

### 2. `config/db_config.yaml` - 数据库配置

```yaml
# MySQL 配置
mysql:
  host: "localhost"
  port: 3306
  database: "data_audit"
  user: "audit_user"
  password: ""              # 建议通过 MYSQL_PASSWORD 环境变量设置
  charset: "utf8mb4"

# ClickHouse 配置
clickhouse:
  # 高可用配置（推荐）
  hosts:
    - "10.252.115.31"       # 主节点
    - "10.252.115.35"       # 备用节点
    - "10.252.115.52"
  port: 9000
  database: "uops_daas"
  user: "default"
  password: ""
  timezone: "Asia/Shanghai"
  
  # Watermark 配置
  watermark_enabled: true
  watermark_path: "../state/clickhouse_watermark.json"
  watermark_overlap_seconds: 600
  watermark_max_window_hours: 24.0
  watermark_advance_on_failure: false
  
  # 查询模板
  query_template: |
    SELECT job_code AS task_name, 
           batch_type AS period_type, 
           batch_no
    FROM uops_daas.ods_all_dacp_dataflow_job_trigger_rt
    WHERE state = 1
      AND complete_dt >= '{start_time}'
      AND complete_dt < '{end_time}'
```

**环境变量覆盖（优先级高于配置文件）：**

| 环境变量 | 配置项 |
|---------|--------|
| `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE` | MySQL 连接 |
| `MYSQL_USER`, `MYSQL_PASSWORD` | MySQL 认证 |
| `CLICKHOUSE_HOST` | ClickHouse 主机，支持逗号分隔多节点（如 `host1,host2,host3`） |
| `CLICKHOUSE_PORT`, `CLICKHOUSE_DATABASE` | ClickHouse 端口和数据库 |
| `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` | ClickHouse 认证 |
| `HDFS_COUNTER_JAR` | hdfs-counter.jar 路径 |

---

## 命令行参数

```bash
python python/main.py [OPTIONS]
```

### 常用参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--date, -d` | 业务日期 (YYYYMMDD) | 昨天 |
| `--tasks, -t` | 指定任务列表（逗号分隔） | 从 ClickHouse 获取 |
| `--skip-clickhouse` | 跳过 ClickHouse，稽核所有配置任务 | - |
| `--concurrency, -n` | Python 并发数 | 配置文件值 |
| `--dry-run` | 只打印 jobs，不执行 | - |

### ClickHouse 相关

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--hours-lookback` | 回看小时数（无 watermark 时） | 24.0 |
| `--watermark-path` | Watermark 文件路径 | 配置文件值 |
| `--watermark-overlap-seconds` | 回扫重叠秒数 | 600 |
| `--watermark-max-window-hours` | 追赶单次最大窗口 | 24.0 |
| `--watermark-init-now` | 初始化 watermark 为当前时间 | - |
| `--watermark-reset` | 重置 watermark 文件 | - |
| `--disable-watermark` | 禁用 watermark | - |

### 路径配置

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--config, -c` | config.yml 路径 | `../config/config.yml` |
| `--db-config` | db_config.yaml 路径 | `../config/db_config.yaml` |
| `--jar` | hdfs-counter.jar 路径 | `./hdfs-counter-1.0.0.jar` |
| `--java-home` | JAVA_HOME 路径 | 环境变量 |
| `--hadoop-conf-dir` | HADOOP_CONF_DIR 路径 | 环境变量 |

---

## 运行示例

### 基础用法

```bash
# 1. 默认：从 ClickHouse 拉取最近完成的任务并稽核
python python/main.py

# 2. 指定业务日期
python python/main.py --date 20260116

# 3. 手动指定任务（跳过 ClickHouse）
python python/main.py --date 20260116 --tasks dw_user_daily,dw_order_daily

# 4. 稽核配置中所有任务
python python/main.py --date 20260116 --skip-clickhouse

# 5. Dry-run 模式（只打印不执行）
python python/main.py --dry-run
```

### Watermark 相关

```bash
# 1. 首次部署：初始化 watermark 为当前时间（跳过历史数据）
python python/main.py --watermark-init-now

# 2. 重置 watermark（重新开始）
python python/main.py --watermark-reset

# 3. 调整回扫重叠时间
python python/main.py --watermark-overlap-seconds 1800

# 4. 限制追赶窗口为 12 小时
python python/main.py --watermark-max-window-hours 12
```

### 并发控制

```bash
# 设置 Python 并发数为 10
python python/main.py --concurrency 10
```

---

## 定时任务配置

### 每小时运行（推荐：ClickHouse + Watermark）

```bash
0 * * * * cd /path/to/DataAuditOptimize && /usr/bin/python3 python/main.py >> logs/cron.log 2>&1
```

### 每天固定时间运行

```bash
# 每天凌晨 2:10 稽核昨天数据（跳过 ClickHouse）
10 02 * * * cd /path/to/DataAuditOptimize && /usr/bin/python3 python/main.py --date $(date -v-1d +\%Y\%m\%d) --skip-clickhouse >> logs/cron.log 2>&1
```

### 建议配置

- 生产环境建议使用 `watermark` 模式，保证不漏数
- 根据 ClickHouse 落库延迟调整 `watermark_overlap_seconds`（默认 10 分钟）
- 设置 `watermark_max_window_hours` 避免长时间停跑后一次性压力过大

---

## Watermark 机制详解

### 为什么需要 Watermark

传统的"now 往回 N 小时"滑动窗口存在问题：
- 停跑/延迟时会漏数
- 窗口重叠导致重复处理

### Watermark 工作原理

```
时间轴：
─────────────────────────────────────────────────────────────►
        │                       │                        │
  last_end_time            overlap               现在(now)
        │◄──────────────────────►                        │
        │      重叠回扫区间      │                        │
        │                       │                        │
        └───────────────────────────────────────────────►│
                     本次查询窗口                         │
```

### 配置说明

| 配置项 | 说明 | 建议值 |
|--------|------|--------|
| `watermark_enabled` | 是否启用 | `true` |
| `watermark_path` | 存储路径 | 持久化目录 |
| `watermark_overlap_seconds` | 回扫重叠秒数 | 600（根据 CK 延迟调整） |
| `watermark_max_window_hours` | 单次最大窗口 | 24.0 |
| `watermark_advance_on_failure` | 失败时是否推进 | `false` |

### Watermark 文件格式

```json
{
  "last_end_time": "2026-01-17T13:00:00+08:00",
  "updated_at": "2026-01-17T13:00:02+08:00"
}
```

---

## 数据库表结构

执行 `scripts/init_db.sql` 初始化：

```bash
mysql -h <host> -P <port> -u <user> -p < scripts/init_db.sql
```

### audit_result 表

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | BIGINT | 自增主键 |
| `task_name` | VARCHAR(200) | 调度任务名称 |
| `interface_id` | VARCHAR(100) | 接口 ID |
| `platform_id` | VARCHAR(100) | 平台 ID |
| `partner_id` | VARCHAR(100) | 合作伙伴 ID |
| `table_name` | VARCHAR(200) | Hive 表名 |
| `hdfs_path` | VARCHAR(500) | HDFS 完整路径 |
| `period_type` | VARCHAR(20) | 周期类型 (hourly/daily/monthly) |
| `batch_no` | VARCHAR(50) | 批次号 |
| `data_date` | DATE | 数据日期 |
| `data_month` | VARCHAR(6) | 数据月份 (YYYYMM) |
| `data_hour` | VARCHAR(2) | 数据小时 (HH) |
| `row_count` | BIGINT | 行数 (-1 表示失败) |
| `file_count` | INT | 文件数量 |
| `total_size_bytes` | BIGINT | 总大小 (字节) |
| `status` | VARCHAR(20) | 状态 (success/partial/failed) |
| `error_msg` | TEXT | 错误信息 (JSON) |
| `duration_ms` | INT | 统计耗时 (毫秒) |
| `created_at` | DATETIME | 记录创建时间 |

---

## 常见问题排查

### 1. ClickHouse 一直拉不到任务

- 检查 `db_config.yaml` 的 `clickhouse.query_template` 是否正确
- 检查时区：`clickhouse.timezone` 是否与 `end_time` 口径一致
- 验证 SQL：

```bash
# 手动执行查询验证
clickhouse-client -h <host> -d <database> --query "SELECT ..."
```

### 2. 担心漏数/重复

- 确认开启 watermark（默认已支持）
- 如存在明显落库延迟，增大 `watermark_overlap_seconds`
- 若停跑很久，使用 `watermark_max_window_hours` 控制追赶节奏

### 3. MySQL 写入失败

- 检查 `db_config.yaml` 的 MySQL 配置
- 检查 MySQL 权限/网络
- 查看连接池日志，必要时增大 `pool_size`

### 4. jar 执行失败

- 确认 jar 路径正确（`--jar` 参数或 `HDFS_COUNTER_JAR` 环境变量）
- 确认 `JAVA_HOME` / `HADOOP_CONF_DIR` 已设置
- 检查 Kerberos 认证（如有）：

```bash
# 检查 ticket
klist

# 获取 ticket
kinit your_principal@REALM
```

### 5. 日志查看

- 控制台输出：stdout
- 文件日志：`hdfs_audit.log`（当前工作目录）

---

## 配置生成

如需修改任务配置，请编辑 `config/config_template.yml`，然后重新生成：

```bash
python python/generate_config.py

# 或 dry-run 预览
python python/generate_config.py --dry-run
```

---

## 退出码

| 退出码 | 含义 |
|--------|------|
| 0 | 全部成功 |
| 1 | 存在失败任务 |
