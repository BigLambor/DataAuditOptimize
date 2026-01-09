-- =============================================================================
-- HDFS 数据稽核系统 - 数据库初始化脚本
-- =============================================================================

-- 创建数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS data_audit 
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;

USE data_audit;

-- -----------------------------------------------------------------------------
-- 稽核结果表
-- -----------------------------------------------------------------------------
-- 存储每次稽核的结果，包括行数、文件数、状态等信息

CREATE TABLE IF NOT EXISTS audit_result (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    task_name VARCHAR(200) NOT NULL COMMENT '调度任务名称',
    interface_id VARCHAR(100) NOT NULL DEFAULT '' COMMENT '接口ID',
    partner_id VARCHAR(100) NOT NULL DEFAULT '' COMMENT '合作伙伴ID',
    province_id VARCHAR(100) NOT NULL DEFAULT '' COMMENT '省份ID',
    table_name VARCHAR(200) NOT NULL COMMENT 'Hive表名 (database.table)',
    hdfs_path VARCHAR(500) NOT NULL COMMENT 'HDFS完整路径（含分区）',
    data_date DATE NOT NULL COMMENT '数据日期',
    row_count BIGINT DEFAULT -1 COMMENT '行数，-1表示统计失败',
    file_count INT DEFAULT 0 COMMENT '文件数量',
    total_size_bytes BIGINT DEFAULT 0 COMMENT '总大小（字节）',
    status VARCHAR(20) NOT NULL COMMENT '状态: success/partial/failed',
    error_msg TEXT COMMENT '错误信息（JSON格式）',
    duration_ms INT DEFAULT 0 COMMENT '统计耗时（毫秒）',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    
    -- 索引
    INDEX idx_task_date (task_name, data_date),
    INDEX idx_interface_id (interface_id),
    INDEX idx_partner_id (partner_id),
    INDEX idx_province_id (province_id),
    INDEX idx_table_date (table_name, data_date),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at),
    
    -- 唯一约束：同一表+同一天+同一省份 只保留一条记录
    -- province_id 为空字符串表示不区分省份
    UNIQUE KEY uk_table_date_province (table_name, data_date, province_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='数据稽核结果表';

-- -----------------------------------------------------------------------------
-- 稽核历史表（可选）
-- -----------------------------------------------------------------------------
-- 如果需要保留历史记录（每次稽核都插入新记录），可以使用此表
-- 同一表同一天可以有多条记录

CREATE TABLE IF NOT EXISTS audit_result_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    task_name VARCHAR(200) NOT NULL COMMENT '调度任务名称',
    interface_id VARCHAR(100) NOT NULL DEFAULT '' COMMENT '接口ID',
    partner_id VARCHAR(100) NOT NULL DEFAULT '' COMMENT '合作伙伴ID',
    province_id VARCHAR(100) NOT NULL DEFAULT '' COMMENT '省份ID',
    table_name VARCHAR(200) NOT NULL COMMENT 'Hive表名',
    hdfs_path VARCHAR(500) NOT NULL COMMENT 'HDFS完整路径',
    data_date DATE NOT NULL COMMENT '数据日期',
    row_count BIGINT DEFAULT -1 COMMENT '行数',
    file_count INT DEFAULT 0 COMMENT '文件数量',
    total_size_bytes BIGINT DEFAULT 0 COMMENT '总大小（字节）',
    status VARCHAR(20) NOT NULL COMMENT '状态',
    error_msg TEXT COMMENT '错误信息',
    duration_ms INT DEFAULT 0 COMMENT '统计耗时（毫秒）',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    
    INDEX idx_task_date (task_name, data_date),
    INDEX idx_interface_id (interface_id),
    INDEX idx_partner_id (partner_id),
    INDEX idx_province_id (province_id),
    INDEX idx_table_date (table_name, data_date),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='数据稽核历史记录表';

-- -----------------------------------------------------------------------------
-- 常用查询示例
-- -----------------------------------------------------------------------------

-- 查询某天所有表的稽核结果
-- SELECT * FROM audit_result WHERE data_date = '2026-01-15' ORDER BY table_name;

-- 查询某个表最近7天的稽核结果
-- SELECT * FROM audit_result 
-- WHERE table_name = 'dw.user_behavior' 
--   AND data_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
-- ORDER BY data_date DESC;

-- 查询失败的稽核任务
-- SELECT * FROM audit_result WHERE status = 'failed' ORDER BY created_at DESC;

-- 按合作伙伴查询稽核结果
-- SELECT * FROM audit_result WHERE partner_id = '1' ORDER BY created_at DESC;

-- 按省份查询稽核结果
-- SELECT * FROM audit_result WHERE province_id = '1' ORDER BY created_at DESC;

-- 统计每天的稽核情况
-- SELECT 
--     data_date,
--     COUNT(*) as total_tables,
--     SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_count,
--     SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_count,
--     SUM(row_count) as total_rows
-- FROM audit_result
-- WHERE data_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
-- GROUP BY data_date
-- ORDER BY data_date DESC;

-- =============================================================================
-- 用户和权限配置（可选）
-- =============================================================================
-- 如果需要创建专用用户，取消以下注释并修改密码

-- CREATE USER IF NOT EXISTS 'audit_user'@'%' IDENTIFIED BY 'your_secure_password';
-- GRANT SELECT, INSERT, UPDATE, DELETE ON data_audit.* TO 'audit_user'@'%';
-- FLUSH PRIVILEGES;
