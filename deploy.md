# HDFS 数据稽核系统部署指南

---

## 一、前置准备

### 1.1 服务器环境要求

| 要求 | 说明 |
|------|------|
| Python | 3.10+ |
| JDK | 1.8+ |
| Hadoop 配置 | 需配置 `HADOOP_CONF_DIR` 指向有效的 Hadoop 配置目录 |
| Kerberos | 需通过 Kerberos 认证（现场自行完成认证配置） |

### 1.2 网络连通性

部署服务器需与以下服务网络联通：

- **ClickHouse**：用于查询调度平台已完成任务（默认端口 `9000`）
- **MySQL**：用于存储稽核结果（默认端口 `3306`）
- **HDFS**：用于读取数据文件

### 1.3 需准备的账号

| 数据库 | 用途 | 权限要求 |
|--------|------|----------|
| MySQL | 存储稽核结果 | 需要 `data_audit` 库的 `SELECT`, `INSERT` 权限 |
| ClickHouse | 查询调度任务 | 需要调度任务表的 `SELECT` 权限 |

---

## 二、部署步骤

### 2.1 上传部署包

将部署包上传至服务器，解压到指定目录：

```bash
# 示例：解压到 /opt 目录
cd /opt
tar -xzf DataAuditOptimize.tar.gz
cd DataAuditOptimize
```

### 2.2 安装 Python 依赖

```bash
# 创建虚拟环境
python3 -m venv venv
source venv/bin/activate

# 安装依赖
pip3 install -r python/requirements.txt
```

### 2.3 放置 JAR 包

将提供的 `hdfs-counter-1.0.0.jar` 放置到指定位置：

```bash
mkdir -p /opt/DataAuditOptimize/lib
cp hdfs-counter-1.0.0.jar /opt/DataAuditOptimize/lib/
```

### 2.4 配置环境变量

创建环境变量配置文件 `/opt/DataAuditOptimize/.env`：

```bash
# ============================================
# HDFS 数据稽核系统 - 环境变量配置
# ============================================

# ---------- JAR 包路径 ----------
export HDFS_COUNTER_JAR="/opt/DataAuditOptimize/lib/hdfs-counter-1.0.0.jar"

# ---------- Java & Hadoop 环境 ----------
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk"
export HADOOP_CONF_DIR="/etc/hadoop/conf"

# ---------- MySQL 连接配置 ----------
export MYSQL_HOST="your_mysql_host"
export MYSQL_PORT="3306"
export MYSQL_DATABASE="data_audit"
export MYSQL_USER="audit_user"
export MYSQL_PASSWORD="your_mysql_password"

# ---------- ClickHouse 连接配置 ----------
# 多节点高可用：用逗号分隔，如 "host1,host2,host3"
export CLICKHOUSE_HOST="10.252.115.31,10.252.115.35,10.252.115.52"
export CLICKHOUSE_PORT="9000"
export CLICKHOUSE_DATABASE="uops_daas"
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD="your_ck_password"
```

**环境变量说明：**

| 变量名 | 必填 | 说明 |
|--------|------|------|
| `HDFS_COUNTER_JAR` | ✅ | hdfs-counter.jar 文件路径 |
| `JAVA_HOME` | ✅ | Java 安装目录 |
| `HADOOP_CONF_DIR` | ✅ | Hadoop 配置目录（含 core-site.xml, hdfs-site.xml） |
| `MYSQL_HOST` | ✅ | MySQL 主机地址 |
| `MYSQL_PORT` | ✅ | MySQL 端口 |
| `MYSQL_DATABASE` | ✅ | MySQL 数据库名 |
| `MYSQL_USER` | ✅ | MySQL 用户名 |
| `MYSQL_PASSWORD` | ✅ | MySQL 密码 |
| `CLICKHOUSE_HOST` | ✅ | ClickHouse 主机地址，**多节点用逗号分隔** |
| `CLICKHOUSE_PORT` | ✅ | ClickHouse 端口 |
| `CLICKHOUSE_DATABASE` | ✅ | ClickHouse 数据库名 |
| `CLICKHOUSE_USER` | ✅ | ClickHouse 用户名 |
| `CLICKHOUSE_PASSWORD` | ✅ | ClickHouse 密码 |

### 2.5 初始化 MySQL 审计表

使用准备好的 MySQL 账号执行初始化脚本：

```bash
source /opt/DataAuditOptimize/.env
mysql -h ${MYSQL_HOST} -P ${MYSQL_PORT} -u ${MYSQL_USER} -p < scripts/init_db.sql
```

### 2.6 生成任务配置文件

**首次上线必须执行此步骤**，根据模板生成 `config.yml`：

```bash
source /opt/DataAuditOptimize/.env
source /opt/DataAuditOptimize/venv/bin/activate

cd /opt/DataAuditOptimize
python3 python/generate_config.py
```

执行成功后会生成 `config/config.yml` 文件。

---

## 三、运行测试

运行脚本 `bin/run_audit.sh` 已包含在部署包中。

```bash
# 测试运行（dry-run 模式，不实际执行）
/opt/DataAuditOptimize/bin/run_audit.sh --dry-run

# 实际运行
/opt/DataAuditOptimize/bin/run_audit.sh
```

---

## 四、配置定时任务

### 4.1 添加 Crontab

```bash
crontab -e
```

添加以下内容：

```bash
# HDFS 数据稽核任务 - 每小时执行一次
0 * * * * /opt/DataAuditOptimize/bin/run_audit.sh >> /opt/DataAuditOptimize/logs/cron.log 2>&1
```

### 4.2 创建日志目录

```bash
mkdir -p /opt/DataAuditOptimize/logs
```

---

## 五、验证部署

1. **检查日志**：

```bash
tail -f /opt/DataAuditOptimize/logs/cron.log
```

2. **检查 MySQL 结果**：

```sql
SELECT task_name, table_name, row_count, status, created_at
FROM data_audit.audit_result
ORDER BY created_at DESC
LIMIT 10;
```

---

## 六、常用命令

| 命令 | 说明 |
|------|------|
| `run_audit.sh` | 默认模式运行（从 ClickHouse 获取任务） |
| `run_audit.sh --dry-run` | 只打印任务，不执行 |
| `run_audit.sh --date 20260117` | 指定业务日期 |
| `run_audit.sh --skip-clickhouse` | 跳过 ClickHouse，稽核所有配置任务 |
| `run_audit.sh --concurrency 10` | 设置并发数 |

---

## 七、注意事项

1. **Kerberos 认证**：部署服务器需已完成 Kerberos 认证配置
2. **环境变量**：`.env` 文件包含敏感信息，请勿提交到版本控制
3. **配置更新**：如需修改任务配置，编辑 `config/config_template.yml` 后重新执行 `generate_config.py`
