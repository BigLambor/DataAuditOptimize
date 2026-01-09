# HDFS Row Counter 使用说明

一个用于统计 HDFS 文件行数的命令行工具，支持 ORC、Parquet 和 TextFile 格式。

## 功能特性

- 支持多种文件格式：ORC、Parquet、TextFile
- 多线程并行处理，提升统计效率
- 支持 Hadoop HA（多 NameService）配置
- 支持 Kerberos 认证
- 递归统计目录下所有文件
- JSON 格式输出，便于程序解析

## 命令行参数

| 参数 | 简写 | 必填 | 说明 |
|------|------|------|------|
| `--path` | `-p` | ✅ | HDFS 路径（支持 `hdfs://nameservice/path` 格式） |
| `--format` | `-f` | ✅ | 文件格式：`orc`、`parquet`、`textfile` |
| `--threads` | `-t` | ❌ | 并行线程数（默认：10） |
| `--delimiter` | `-d` | ❌ | 文本文件行分隔符（默认：`\n`） |
| `--hadoop-conf` | `-c` | ❌ | Hadoop 配置目录路径 |
| `--help` | `-h` | ❌ | 显示帮助信息 |

## 使用示例

### 基本用法

```bash
cd java/hdfs-counter
mvn clean package -DskipTests

# 生成的 jar 文件: target/hdfs-counter-1.0.0.jar

java -jar hdfs-counter-1.0.0.jar \
  --path hdfs://zw-ns1/warehouse/tablespace/managed/hive/mydb/mytable \
  --format parquet \
  --threads 10
```

### 指定 Hadoop 配置目录

```bash
java -jar hdfs-counter-1.0.0.jar \
  --hadoop-conf /etc/hadoop/conf \
  --path hdfs://zw-ns1/warehouse/tablespace/managed/hive/mydb/mytable \
  --format orc \
  --threads 20
```

### 统计文本文件

```bash
java -jar hdfs-counter-1.0.0.jar \
  --path hdfs://zw-ns1/data/logs \
  --format textfile \
  --delimiter "\n" \
  --threads 10
```

## Hadoop 配置加载

程序按以下优先级自动加载 Hadoop 配置文件（`core-site.xml`、`hdfs-site.xml`）：

1. `--hadoop-conf` 参数指定的目录
2. `HADOOP_CONF_DIR` 环境变量
3. `HADOOP_HOME/etc/hadoop` 目录

## Kerberos 认证

如果集群启用了 Kerberos 认证，运行前请确保已获取有效的 ticket：

```bash
# 检查当前 ticket
klist

# 获取 ticket（如果需要）
kinit your_principal@REALM

# 然后运行程序
java -jar hdfs-counter-1.0.0.jar --path hdfs://... --format parquet
```

如遇认证问题，可显式指定 Kerberos 配置：

```bash
java -Djava.security.krb5.conf=/etc/krb5.conf \
  -jar hdfs-counter-1.0.0.jar \
  --path hdfs://zw-ns1/... \
  --format parquet
```

## 输出格式

程序输出 JSON 格式的统计结果：

```json
{
  "path": "hdfs://zw-ns1/warehouse/tablespace/managed/hive/mydb/mytable",
  "status": "success",
  "errors": [],
  "row_count": 1234567,
  "file_count": 10,
  "success_file_count": 10,
  "total_size_bytes": 52428800,
  "duration_ms": 3456
}
```

### 字段说明

| 字段 | 说明 |
|------|------|
| `path` | 统计的 HDFS 路径 |
| `status` | 状态：`success`（全部成功）、`partial`（部分成功）、`failed`（全部失败） |
| `errors` | 错误列表，包含失败文件的路径和错误信息 |
| `row_count` | 总行数（失败时为 -1） |
| `file_count` | 文件总数 |
| `success_file_count` | 成功统计的文件数 |
| `total_size_bytes` | 成功统计文件的总大小（字节） |
| `duration_ms` | 统计耗时（毫秒） |

## 退出码

- `0` - 完全成功（status 为 success）
- `1` - 完全失败（status 为 failed）
- `2` - 部分成功（status 为 partial，部分文件统计失败）

## 注意事项

1. 程序会自动跳过以 `_` 或 `.` 开头的文件（如 `_SUCCESS`、`.metadata`）
2. 对于大目录，建议适当增加线程数以提升效率
3. 确保运行用户对目标 HDFS 路径有读取权限

