# -*- coding: utf-8 -*-
"""
数据存储模块
功能：纯 HDFS/Parquet 数据流程检查。
说明：读取清洗后 Parquet，做基础一致性校验并返回可视化所需元信息。
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from src import config
from src.hdfs_io import is_hdfs_uri, read_parquet_to_pandas


def _load_parquet_dataframe(parquet_path):
    """按路径类型读取 Parquet（支持本地/HDFS）。"""
    if is_hdfs_uri(parquet_path):
        return read_parquet_to_pandas(parquet_path)
    return pd.read_parquet(parquet_path)


def _month_stats(pdf):
    """统计可用月份分布（若存在统计月份列）。"""
    month_col = config.COL_STATS_MONTH
    if month_col not in pdf.columns:
        return []
    month_series = (
        pdf[month_col]
        .astype(str)
        .str.strip()
        .replace({"": None, "nan": None, "None": None})
        .dropna()
    )
    if month_series.empty:
        return []
    grouped = month_series.value_counts().sort_index()
    return [
        {"stats_month": str(month), "row_cnt": int(cnt)}
        for month, cnt in grouped.items()
    ]


def validate_storage_inputs(parquet_path=None):
    """
    执行纯存储阶段检查：确认 Parquet 可读、字段齐全，并返回基础元信息。
    不进行本地库落地。
    """
    parquet_path = parquet_path or config.clean_parquet_uri()
    config.ensure_dirs()

    pdf = _load_parquet_dataframe(parquet_path)
    required_cols = [config.COL_MONTHLY_SALES, config.COL_CITY, config.COL_CATEGORY]
    missing_cols = [c for c in required_cols if c not in pdf.columns]

    return {
        "mode": "hdfs" if is_hdfs_uri(parquet_path) else "local",
        "parquet_path": parquet_path,
        "rows": int(len(pdf)),
        "columns": int(len(pdf.columns)),
        "missing_required_columns": missing_cols,
        "has_stats_month_col": config.COL_STATS_MONTH in pdf.columns,
        "available_months": _month_stats(pdf),
    }


def parquet_to_sqlite(parquet_path=None, sqlite_path=None):
    """
    兼容旧接口：历史函数名保留，但行为已改为纯 Parquet/HDFS 校验。
    sqlite_path 参数保留但不再使用。
    """
    _ = sqlite_path
    return validate_storage_inputs(parquet_path)


def run(parquet_path=None, sqlite_path=None):
    """执行存储阶段：Parquet/HDFS 一致性检查与元信息输出。"""
    return parquet_to_sqlite(parquet_path, sqlite_path)


if __name__ == "__main__":
    result = run()
    print("存储完成:", result)
