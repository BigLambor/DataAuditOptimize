#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_config.py - 从模板生成稽核配置文件

功能：
  1. 读取 config_template.yml 模板文件
  2. 根据 template_vars 定义展开 loop_var 变量（如 prov_id, data_hour）
  3. 保留运行时变量占位符（如 ${data_date}, ${data_month}），由 main.py 动态替换
  4. 生成最终的 config.yml 文件

设计说明：
  - 展开变量：prov_id, data_hour 等 → 在此脚本展开，决定任务数量
  - 运行时变量：data_date, data_month → 保留占位符，main.py 执行时替换

用法：
  python generate_config.py                          # 使用默认路径
  python generate_config.py -t ../config/config_template.yml -o ../config/config.yml
  python generate_config.py --dry-run                # 只打印不写文件
"""

import argparse
import copy
import itertools
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import yaml
import re

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 运行时变量（保留占位符，不在生成阶段替换）
RUNTIME_VARS = {'data_date', 'data_month'}


class TemplateVarGenerator:
    """模板变量值生成器（仅处理展开变量）"""

    def __init__(self, template_vars: Dict[str, Any]):
        """
        初始化生成器

        Args:
            template_vars: 模板变量定义
        """
        self.template_vars = template_vars
        self._cache: Dict[str, List[Any]] = {}

    def get_values(self, var_name: str) -> List[Any]:
        """
        获取变量的所有可能值

        Args:
            var_name: 变量名

        Returns:
            变量值列表
        """
        if var_name in self._cache:
            return self._cache[var_name]

        if var_name not in self.template_vars:
            raise ValueError(f"Unknown template variable: {var_name}")

        var_def = self.template_vars[var_name]
        var_type = var_def.get('type', 'range')

        if var_type == 'range':
            values = self._generate_range(var_def)
        elif var_type == 'enum':
            values = self._generate_enum(var_def)
        elif var_type == 'computed':
            # computed 类型是运行时变量，不在此展开
            raise ValueError(f"Variable '{var_name}' is a runtime variable (computed), cannot be used in loop_var")
        else:
            raise ValueError(f"Unknown variable type: {var_type} for {var_name}")

        self._cache[var_name] = values
        logger.debug(f"Generated {len(values)} values for {var_name}: {values[:5]}...")
        return values

    def _generate_range(self, var_def: Dict[str, Any]) -> List[Any]:
        """生成范围型变量值"""
        start = var_def.get('start', 0)
        end = var_def.get('end', 0)
        step = var_def.get('step', 1)
        pad = var_def.get('pad', 0)  # 零填充宽度

        # 支持字符串格式的数字
        if isinstance(start, str):
            start = int(start)
        if isinstance(end, str):
            end = int(end)
        if isinstance(step, str):
            step = int(step)

        values = list(range(start, end + 1, step))

        # 如果指定了 pad，则进行零填充（如 pad=2 则 0 -> "00", 1 -> "01"）
        if pad > 0:
            values = [str(v).zfill(pad) for v in values]

        return values

    def _generate_enum(self, var_def: Dict[str, Any]) -> List[Any]:
        """生成枚举型变量值"""
        values = var_def.get('values', [])
        if not values:
            raise ValueError("Enum type requires 'values' list")
        return list(values)


class ConfigGenerator:
    """配置文件生成器"""

    def __init__(self, template_path: str):
        """
        初始化生成器

        Args:
            template_path: 模板文件路径
        """
        self.template_path = template_path
        self.template: Dict[str, Any] = {}
        self.var_generator: Optional[TemplateVarGenerator] = None
        self._original_schedules: List[Dict[str, Any]] = []  # 保存原始 schedule 信息用于注释

        self._load_template()

    def _load_template(self) -> None:
        """加载模板文件"""
        if not os.path.exists(self.template_path):
            raise FileNotFoundError(f"Template file not found: {self.template_path}")

        with open(self.template_path, 'r', encoding='utf-8') as f:
            self.template = yaml.safe_load(f)

        # 初始化变量生成器
        template_vars = self.template.get('template_vars', {})
        self.var_generator = TemplateVarGenerator(template_vars)

        # 保存原始 schedules 信息
        self._original_schedules = self.template.get('schedules', [])

        logger.info(f"Loaded template from {self.template_path}")

    def _replace_variables(self, text: str, var_values: Dict[str, Any]) -> str:
        """
        替换文本中的展开变量占位符（保留运行时变量）

        Args:
            text: 包含 ${var} 的文本
            var_values: 变量名 -> 值的映射（仅展开变量）

        Returns:
            替换后的文本
        """
        if not isinstance(text, str):
            return text

        result = text
        # 只替换 loop_var 中的展开变量
        for var_name, var_value in var_values.items():
            if var_name not in RUNTIME_VARS:
                result = result.replace(f'${{{var_name}}}', str(var_value))

        return result

    def _replace_in_dict(self, obj: Any, var_values: Dict[str, Any]) -> Any:
        """递归替换字典/列表中的变量"""
        if isinstance(obj, str):
            return self._replace_variables(obj, var_values)
        elif isinstance(obj, dict):
            return {k: self._replace_in_dict(v, var_values) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._replace_in_dict(item, var_values) for item in obj]
        else:
            return obj

    def _expand_schedule(self, schedule: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        展开单个 schedule

        Args:
            schedule: 包含 loop_var 的 schedule 配置

        Returns:
            展开后的 schedule 列表
        """
        loop_var = schedule.get('loop_var', '')

        if not loop_var:
            # 无 loop_var，保持原样（运行时变量保留）
            expanded = copy.deepcopy(schedule)
            return [expanded]

        # 解析 loop_var（支持 & 分隔的多变量）
        var_names = [v.strip() for v in loop_var.split('&')]

        # 检查是否有运行时变量被错误地放入 loop_var
        for var_name in var_names:
            if var_name in RUNTIME_VARS:
                raise ValueError(
                    f"Runtime variable '{var_name}' cannot be used in loop_var. "
                    f"Runtime variables ({RUNTIME_VARS}) are resolved at execution time by main.py"
                )

        # 获取每个变量的值列表
        var_values_list = []
        for var_name in var_names:
            values = self.var_generator.get_values(var_name)
            var_values_list.append([(var_name, v) for v in values])

        # 笛卡尔积展开
        expanded_schedules = []
        for combination in itertools.product(*var_values_list):
            var_values = dict(combination)
            expanded = self._replace_in_dict(copy.deepcopy(schedule), var_values)
            # 移除 loop_var 字段（生成后不需要）
            expanded.pop('loop_var', None)
            expanded_schedules.append(expanded)

        return expanded_schedules

    def generate(self) -> tuple:
        """
        生成最终配置

        Returns:
            (展开后的配置字典, 分组信息列表)
        """
        output = copy.deepcopy(self.template)

        # 移除 template_vars（生成后不需要）
        output.pop('template_vars', None)

        # 展开 schedules，并记录分组信息
        schedules = self.template.get('schedules', [])
        expanded_schedules = []
        group_info = []  # [(start_idx, count, original_task_name, interface_id), ...]

        for schedule in schedules:
            original_task_name = schedule.get('task_name', 'unknown')
            interface_id = schedule.get('interface_id', '')
            period_type = schedule.get('period_type', '')
            loop_var = schedule.get('loop_var', '')

            start_idx = len(expanded_schedules)
            expanded = self._expand_schedule(schedule)
            expanded_schedules.extend(expanded)
            count = len(expanded)

            group_info.append({
                'start_idx': start_idx,
                'count': count,
                'original_task_name': original_task_name,
                'interface_id': interface_id,
                'period_type': period_type,
                'loop_var': loop_var
            })

            logger.info(f"Expanded '{original_task_name}' -> {count} tasks")

        output['schedules'] = expanded_schedules

        logger.info(f"Total expanded schedules: {len(expanded_schedules)}")
        return output, group_info

    def _build_yaml_dumper(self) -> type:
        """Create a YAML dumper that quotes numeric-like strings."""
        class QuotedStringDumper(yaml.SafeDumper):
            pass

        def _represent_str(dumper, data):
            # Quote numeric-like strings to avoid YAML parsing as int/float
            if re.fullmatch(r"\d+", data or ""):
                return dumper.represent_scalar('tag:yaml.org,2002:str', data, style="'")
            if re.fullmatch(r"0\d+", data or ""):
                return dumper.represent_scalar('tag:yaml.org,2002:str', data, style="'")
            return dumper.represent_scalar('tag:yaml.org,2002:str', data)

        QuotedStringDumper.add_representer(str, _represent_str)
        return QuotedStringDumper

    def write(self, output_path: str) -> None:
        """
        生成并写入配置文件（带分组注释，增强可读性）

        Args:
            output_path: 输出文件路径
        """
        config, group_info = self.generate()

        # 生成文件头
        lines = []
        lines.append("# =============================================================================")
        lines.append("# HDFS 数据稽核配置文件（自动生成）")
        lines.append("# =============================================================================")
        lines.append(f"# 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"# 模板文件: {os.path.basename(self.template_path)}")
        lines.append(f"# 任务总数: {len(config.get('schedules', []))}")
        lines.append("#")
        lines.append("# 运行时变量说明（由 main.py 动态替换）：")
        lines.append("#   ${data_date}  - 业务日期，格式 YYYYMMDD")
        lines.append("#   ${data_month} - 业务月份（数据日期所在月），格式 YYYYMM")
        lines.append("#")
        lines.append("# 注意: 此文件由 generate_config.py 自动生成，请勿手动修改")
        lines.append("# =============================================================================")
        lines.append("")

        # defaults 部分
        lines.append("# -----------------------------------------------------------------------------")
        lines.append("# 运行时默认配置")
        lines.append("# -----------------------------------------------------------------------------")
        dumper = self._build_yaml_dumper()
        defaults_yaml = yaml.dump(
            {'defaults': config.get('defaults', {})},
            allow_unicode=True,
            default_flow_style=False,
            sort_keys=False,
            Dumper=dumper
        )
        lines.append(defaults_yaml.strip())
        lines.append("")

        # schedules 部分（按分组添加注释）
        lines.append("# -----------------------------------------------------------------------------")
        lines.append("# 稽核任务列表")
        lines.append("# -----------------------------------------------------------------------------")
        lines.append("schedules:")

        schedules = config.get('schedules', [])
        current_group_idx = 0

        for idx, schedule in enumerate(schedules):
            # 检查是否是新分组的开始
            if current_group_idx < len(group_info):
                group = group_info[current_group_idx]
                if idx == group['start_idx']:
                    # 添加分组注释
                    lines.append("")
                    lines.append("  # =========================================================================")
                    lines.append(f"  # 任务组 {current_group_idx + 1}: {group['interface_id']} - {group['period_type']}")
                    lines.append(f"  # 原始模板: {group['original_task_name']}")
                    if group['loop_var']:
                        lines.append(f"  # 展开变量: {group['loop_var']} -> {group['count']} 个任务")
                    else:
                        lines.append(f"  # 无展开变量，单任务")
                    lines.append("  # =========================================================================")

                    current_group_idx += 1

            # 输出单个 schedule
            schedule_yaml = yaml.dump(
                [schedule],
                allow_unicode=True,
                default_flow_style=False,
                sort_keys=False,
                Dumper=dumper
            )
            # 调整缩进
            for line in schedule_yaml.strip().split('\n'):
                lines.append("  " + line)
            # 每个任务之间添加空行，提高可读性
            lines.append("")

        # 写入文件
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
            f.write('\n')

        logger.info(f"Generated config written to: {output_path}")


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='从模板生成稽核配置文件',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
设计说明：
  - 展开变量（prov_id, data_hour）: 在此脚本展开，决定任务数量
  - 运行时变量（data_date, data_month）: 保留占位符，main.py 执行时替换

示例：
  python generate_config.py
  python generate_config.py -t ../config/config_template.yml -o ../config/config.yml
  python generate_config.py --dry-run
        """
    )

    parser.add_argument(
        '-t', '--template',
        default='../config/config_template.yml',
        help='模板文件路径 (默认: ../config/config_template.yml)'
    )

    parser.add_argument(
        '-o', '--output',
        default='../config/config.yml',
        help='输出文件路径 (默认: ../config/config.yml)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='只打印生成结果，不写入文件'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='显示详细日志'
    )

    return parser.parse_args()


def main():
    """主函数"""
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        # 处理相对路径
        script_dir = os.path.dirname(os.path.abspath(__file__))
        template_path = args.template
        output_path = args.output

        if not os.path.isabs(template_path):
            template_path = os.path.join(script_dir, template_path)
        if not os.path.isabs(output_path):
            output_path = os.path.join(script_dir, output_path)

        # 生成配置
        generator = ConfigGenerator(template_path)

        if args.dry_run:
            config, group_info = generator.generate()
            print("\n" + "=" * 60)
            print("DRY RUN - Generated Config Preview:")
            print("=" * 60)

            # 打印分组信息
            print("\n任务分组统计：")
            for i, group in enumerate(group_info):
                print(f"  {i+1}. [{group['interface_id']}] {group['original_task_name']}")
                print(f"     period: {group['period_type']}, loop_var: {group['loop_var'] or '无'}, count: {group['count']}")

            print(f"\n总任务数: {len(config.get('schedules', []))}")
            print("\n" + "-" * 60)
            print("配置预览（前3个任务）：")
            print("-" * 60)
            preview_schedules = config.get('schedules', [])[:3]
            print(yaml.dump({'schedules': preview_schedules}, allow_unicode=True, default_flow_style=False, sort_keys=False))
        else:
            generator.write(output_path)
            print(f"\n✅ Config generated successfully: {output_path}")

    except Exception as e:
        logger.error(f"Failed to generate config: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
