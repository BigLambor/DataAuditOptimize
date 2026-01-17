#!/bin/bash
# =============================================================================
# HDFS 数据稽核运行脚本
# =============================================================================
set -e

# 获取脚本所在目录的父目录（即项目根目录）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "${SCRIPT_DIR}")"

# 加载环境变量（如果存在）
if [ -f "${PROJECT_DIR}/.env" ]; then
    source "${PROJECT_DIR}/.env"
fi

# 激活 Python 虚拟环境（如果存在）
if [ -f "${PROJECT_DIR}/venv/bin/activate" ]; then
    source "${PROJECT_DIR}/venv/bin/activate"
fi

# 切换到项目目录
cd "${PROJECT_DIR}"

# 执行稽核
python3 python/main.py "$@"
