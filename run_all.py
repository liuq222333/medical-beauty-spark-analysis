# -*- coding: utf-8 -*-
"""
一键运行全流程：
  1. 数据采集与预处理
  2. Parquet/HDFS 存储一致性检查
  3. K-Means 聚类 + Spark SQL 趋势统计
  4. 聚类主题宽表派生（DWS 层：基于聚类结果的 8 张派生表）
"""

import os
import sys

# 项目根目录为 project/
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from src import config
from src.preprocess import run as run_preprocess
from src.storage import run as run_storage
from src.analysis import (
    run_clustering,
    run_trend_analysis,
    run_cluster_warehouse,
)


def main():
    config.ensure_dirs()

    print("=" * 60)
    print("1. 数据采集与预处理")
    print("=" * 60)
    r1 = run_preprocess()
    print(r1)

    print("\n" + "=" * 60)
    print("2. 数据存储检查（纯 Parquet/HDFS）")
    print("=" * 60)
    r2 = run_storage()
    print(r2)

    print("\n" + "=" * 60)
    print("3. 数据分析 —— K-Means 聚类 + Spark SQL 趋势统计")
    print("=" * 60)
    r3a = run_clustering()
    print("  3.1 K-Means 聚类完成:", r3a)
    r3b = run_trend_analysis(use_clustered_data=True)
    print("  3.2 趋势统计完成:", r3b)

    print("\n" + "=" * 60)
    print("4. 聚类数据仓库派生 —— 基于 K-Means 结果的 8 张主题宽表")
    print("=" * 60)
    r4 = run_cluster_warehouse()
    print(r4)

    print("\n" + "=" * 60)
    print("全流程完成。启动可视化看板请执行：")
    print("=" * 60)
    print("  python -m src.visualize")
    print("  或: cd src && python visualize.py")
    print("  浏览器访问 http://127.0.0.1:5000/")


if __name__ == "__main__":
    main()
