-- =============================================================================
-- HDFS 数据稽核系统 - 数据库初始化脚本
-- =============================================================================

-- 创建数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS data_audit 
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;

USE data_audit;

-- -----------------------------------------------------------------------------
-- 稽核结果表（追加模式，无主键/唯一键）
-- -----------------------------------------------------------------------------
-- 存储每次稽核的结果，采用 append 方式写入，支持同一表同一天多次稽核记录
-- 同一个任务同一天可能会有多条记录（如重跑、不同批次等）

CREATE TABLE IF NOT EXISTS audit_result (
    -- 任务标识信息
    task_name VARCHAR(200) NOT NULL COMMENT '调度任务名称',
    interface_id VARCHAR(100) NOT NULL DEFAULT '' COMMENT '接口ID',
    platform_id VARCHAR(100) NOT NULL DEFAULT '' COMMENT '平台ID（省份平台代码）',
    partner_id VARCHAR(100) NOT NULL DEFAULT '' COMMENT '合作伙伴ID',
    
    -- 表和路径信息
    table_name VARCHAR(200) NOT NULL COMMENT 'Hive表名 (database.table)',
    hdfs_path VARCHAR(500) NOT NULL COMMENT 'HDFS完整路径（含分区）',
    
    -- 时间周期信息
    period_type VARCHAR(20) NOT NULL DEFAULT 'daily' COMMENT '周期类型: hourly/daily/monthly',
    batch_no VARCHAR(50) NOT NULL DEFAULT '' COMMENT '批次号（来自调度平台）',
    data_date DATE DEFAULT NULL COMMENT '数据日期（daily/hourly 任务）',
    data_month VARCHAR(6) DEFAULT NULL COMMENT '数据月份 YYYYMM（monthly 任务必填）',
    data_hour VARCHAR(2) DEFAULT NULL COMMENT '数据小时 HH（hourly 任务）',
    
    -- 统计结果
    row_count BIGINT DEFAULT -1 COMMENT '行数，-1表示统计失败',
    file_count INT DEFAULT 0 COMMENT '文件数量',
    total_size_bytes BIGINT DEFAULT 0 COMMENT '总大小（字节）',
    
    -- 状态信息
    status VARCHAR(20) NOT NULL COMMENT '状态: success/partial/failed',
    error_msg TEXT COMMENT '错误信息（JSON格式）',
    duration_ms INT DEFAULT 0 COMMENT '统计耗时（毫秒）',
    
    -- 记录时间
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    
    -- 索引（用于查询优化，不强制唯一性）
    INDEX idx_task_date (task_name, data_date),
    INDEX idx_task_month (task_name, data_month),
    INDEX idx_interface_id (interface_id),
    INDEX idx_platform_id (platform_id),
    INDEX idx_partner_id (partner_id),
    INDEX idx_table_date (table_name, data_date),
    INDEX idx_period_type (period_type),
    INDEX idx_batch_no (batch_no),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at),
    INDEX idx_data_month (data_month)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='数据稽核结果表（追加模式）';


-- -----------------------------------------------------------------------------
-- 常用查询示例
-- -----------------------------------------------------------------------------

-- 查询某天所有表的稽核结果（取最新一条）
-- SELECT * FROM (
--     SELECT *, ROW_NUMBER() OVER (PARTITION BY table_name, data_date ORDER BY created_at DESC) as rn
--     FROM audit_result 
--     WHERE data_date = '2026-01-15'
-- ) t WHERE rn = 1;

-- 查询某个表最近7天的稽核结果
-- SELECT * FROM audit_result 
-- WHERE table_name = 'dw.user_behavior' 
--   AND data_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
-- ORDER BY data_date DESC, created_at DESC;

-- 查询失败的稽核任务
-- SELECT * FROM audit_result WHERE status = 'failed' ORDER BY created_at DESC;

-- 按平台ID查询稽核结果
-- SELECT * FROM audit_result WHERE platform_id = '10100' ORDER BY created_at DESC;

-- 按周期类型查询
-- SELECT * FROM audit_result WHERE period_type = 'monthly' AND data_month = '202601';

-- 查询某个批次的所有结果
-- SELECT * FROM audit_result WHERE batch_no = '20260115' ORDER BY task_name;

-- 统计每天的稽核情况（取每个表最新一条）
-- SELECT 
--     data_date,
--     COUNT(*) as total_tables,
--     SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_count,
--     SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_count,
--     SUM(row_count) as total_rows
-- FROM (
--     SELECT *, ROW_NUMBER() OVER (PARTITION BY table_name, platform_id, data_date ORDER BY created_at DESC) as rn
--     FROM audit_result
--     WHERE data_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
-- ) t WHERE rn = 1
-- GROUP BY data_date
-- ORDER BY data_date DESC;

-- 统计 monthly 任务按月汇总
-- SELECT 
--     data_month,
--     COUNT(*) as total_tasks,
--     SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_count,
--     SUM(row_count) as total_rows
-- FROM audit_result
-- WHERE period_type = 'monthly'
-- GROUP BY data_month
-- ORDER BY data_month DESC;

-- =============================================================================
-- 用户和权限配置（可选）
-- =============================================================================
-- 如果需要创建专用用户，取消以下注释并修改密码

-- CREATE USER IF NOT EXISTS 'audit_user'@'%' IDENTIFIED BY 'your_secure_password';
-- GRANT SELECT, INSERT ON data_audit.* TO 'audit_user'@'%';
-- FLUSH PRIVILEGES;
