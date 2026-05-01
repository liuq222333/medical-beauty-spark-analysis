# -*- coding: utf-8 -*-
"""
数据分析模块
功能：基于 Spark 的聚类分析（K-Means）、销售趋势统计、聚类主题宽表派生（DWS 层）
"""

import os
import sys
import json
from datetime import datetime
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

from src import config
from src.hdfs_io import is_hdfs_uri, write_parquet_from_pandas


def _get_spark():
    from src import config as cfg
    cfg.ensure_hadoop_home()
    b = (
        SparkSession.builder.appName("MedicalBeauty-Analysis")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )
    if cfg.USE_HDFS:
        b = b.config("spark.hadoop.fs.defaultFS", cfg.spark_default_fs()).config(
            "spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem"
        ).config(
            "spark.hadoop.dfs.client.use.datanode.hostname", "true"
        )
    return b.getOrCreate()


# 用于聚类的数值特征
FEATURE_COLS = [
    config.COL_PRICE_ORIGINAL,
    config.COL_PRICE_ACTUAL,
    config.COL_DISCOUNT,
    config.COL_MONTHLY_SALES,
    config.COL_REVIEW_COUNT,
    config.COL_RATING,
]


def run_clustering(parquet_path=None, k=None, output_dir=None):
    """
    对清洗后的数据进行 K-Means 聚类，输出带 cluster 标签的 Parquet 与聚类中心。
    """
    parquet_path = parquet_path or config.clean_parquet_uri()
    k = k or config.KMEANS_K
    output_dir = output_dir or config.OUTPUT_DIR
    config.ensure_dirs()

    spark = _get_spark()
    df = spark.read.parquet(parquet_path)

    # 特征列存在且为数值
    available = [c for c in FEATURE_COLS if c in df.columns]
    for c in available:
        df = df.withColumn(c, F.col(c).cast("double"))
    df = df.dropna(subset=available)

    assembler = VectorAssembler(inputCols=available, outputCol="features", handleInvalid="skip")
    df = assembler.transform(df)
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
    scaler_model = scaler.fit(df)
    df = scaler_model.transform(df)

    kmeans = KMeans(k=k, seed=42, featuresCol="scaledFeatures", predictionCol="cluster")
    model = kmeans.fit(df)
    df = model.transform(df)

    cluster_centers = model.clusterCenters()
    cluster_path = config.clustered_parquet_uri(output_dir)
    to_drop = [c for c in ("features", "scaledFeatures") if c in df.columns]
    df_out = df.drop(*to_drop) if to_drop else df
    try:
        df_out.write.mode("overwrite").parquet(cluster_path)
    except Exception:
        import pandas as pd

        pdf = df_out.toPandas()
        if is_hdfs_uri(cluster_path):
            write_parquet_from_pandas(pdf, cluster_path)
        else:
            os.makedirs(output_dir, exist_ok=True)
            pdf.to_parquet(
                cluster_path,
                index=False,
                engine="pyarrow",
                coerce_timestamps="us",
                allow_truncated_timestamps=True,
            )

    # 保存聚类中心与特征列名，供可视化使用
    centers_json = os.path.join(output_dir, "cluster_centers.json")
    with open(centers_json, "w", encoding="utf-8") as f:
        json.dump(
            {
                "feature_cols": available,
                "centers": [[float(x) for x in c] for c in cluster_centers],
                "k": k,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )
    spark.stop()
    return {"clustered_parquet": cluster_path, "centers_json": centers_json, "k": k}


def _row_to_dict(row):
    """将 Spark Row 转为可 JSON 序列化的 dict。"""
    d = row.asDict()
    out = {}
    for k, v in d.items():
        if v is None:
            out[k] = None
        elif isinstance(v, (Decimal, float)):
            out[k] = float(v) if str(v) != "nan" else None
        elif hasattr(v, "item"):
            out[k] = v.item()
        else:
            out[k] = v
    return out


def _quote_ident(name):
    """Spark SQL 标识符转义（支持中文列名）。"""
    return "`{}`".format(str(name).replace("`", ""))


def run_trend_analysis(parquet_path=None, output_dir=None, use_clustered_data=False):
    """
    多维度趋势与结构分析：使用 Spark SQL + DataFrame API，输出 JSON。
    覆盖月份趋势、城市/商圈/品类/子类/门店/平台/项目名/医生头衔及价格、折扣、评价等字段。
    
    Args:
        parquet_path: 数据文件路径
        output_dir: 输出目录
        use_clustered_data: 是否使用聚类后的数据进行分析
    """
    parquet_path = parquet_path or config.clean_parquet_uri()
    output_dir = output_dir or config.OUTPUT_DIR
    config.ensure_dirs()

    spark = _get_spark()
    
    if use_clustered_data:
        clustered_path = config.clustered_parquet_uri(output_dir)
        if os.path.exists(clustered_path.replace("hdfs://localhost:9000", "")) or clustered_path.startswith("hdfs://"):
            df = spark.read.parquet(clustered_path)
        else:
            df = spark.read.parquet(parquet_path)
    else:
        df = spark.read.parquet(parquet_path)

    num_cols = [
        config.COL_PRICE_ORIGINAL,
        config.COL_PRICE_ACTUAL,
        config.COL_DISCOUNT,
        config.COL_MONTHLY_SALES,
        config.COL_REVIEW_COUNT,
        config.COL_RATING,
    ]
    for c in num_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))

    df.createOrReplaceTempView("sales")
    results = {}
    q = _quote_ident

    # 1) 可选统计月份列表（由采集日期派生）
    if config.COL_STATS_MONTH in df.columns:
        sql_months = (
            "SELECT {m} AS stats_month, COUNT(*) AS row_cnt "
            "FROM sales GROUP BY {m} ORDER BY stats_month"
        ).format(m=q(config.COL_STATS_MONTH))
        results["available_months"] = [_row_to_dict(row) for row in spark.sql(sql_months).collect()]

    # 2) 按统计月份的销售趋势（总销量、项目条数、均价）
    if config.COL_STATS_MONTH in df.columns and config.COL_MONTHLY_SALES in df.columns:
        sql_trend = (
            "SELECT {m} AS stats_month, "
            "SUM({sales}) AS total_sales, COUNT(*) AS project_count, "
            "AVG({rating}) AS avg_rating, AVG({actual}) AS avg_actual_price, "
            "AVG({orig}) AS avg_list_price, AVG({disc}) AS avg_discount "
            "FROM sales GROUP BY {m} ORDER BY stats_month"
        ).format(
            m=q(config.COL_STATS_MONTH),
            sales=q(config.COL_MONTHLY_SALES),
            rating=q(config.COL_RATING),
            actual=q(config.COL_PRICE_ACTUAL),
            orig=q(config.COL_PRICE_ORIGINAL),
            disc=q(config.COL_DISCOUNT),
        )
        results["sales_trend_by_month"] = [_row_to_dict(row) for row in spark.sql(sql_trend).collect()]

    # 3) 全局汇总（理解数据规模与价格带）
    sql_summary = (
        "SELECT COUNT(*) AS row_cnt, SUM({sales}) AS total_sales, "
        "AVG({rating}) AS avg_rating, AVG({actual}) AS avg_actual_price, "
        "AVG({orig}) AS avg_list_price, AVG({disc}) AS avg_discount, "
        "AVG({rev}) AS avg_review_count "
        "FROM sales"
    ).format(
        sales=q(config.COL_MONTHLY_SALES),
        rating=q(config.COL_RATING),
        actual=q(config.COL_PRICE_ACTUAL),
        orig=q(config.COL_PRICE_ORIGINAL),
        disc=q(config.COL_DISCOUNT),
        rev=q(config.COL_REVIEW_COUNT),
    )
    summary_rows = spark.sql(sql_summary).collect()
    results["summary"] = _row_to_dict(summary_rows[0]) if summary_rows else {}

    # 4) 城市：销量 + 评分 + 价格
    if config.COL_CITY in df.columns:
        sql_city = (
            "SELECT {city} AS city, SUM({sales}) AS total_sales, COUNT(*) AS project_count, "
            "AVG({rating}) AS avg_rating, AVG({actual}) AS avg_actual_price, "
            "AVG({disc}) AS avg_discount "
            "FROM sales GROUP BY {city} ORDER BY total_sales DESC LIMIT 30"
        ).format(
            city=q(config.COL_CITY),
            sales=q(config.COL_MONTHLY_SALES),
            rating=q(config.COL_RATING),
            actual=q(config.COL_PRICE_ACTUAL),
            disc=q(config.COL_DISCOUNT),
        )
        results["by_city"] = [_row_to_dict(row) for row in spark.sql(sql_city).collect()]

    # 5) 品类
    if config.COL_CATEGORY in df.columns:
        sql_cat = (
            "SELECT {cat} AS category, SUM({sales}) AS total_sales, COUNT(*) AS project_count, "
            "AVG({actual}) AS avg_actual_price, AVG({disc}) AS avg_discount, AVG({rating}) AS avg_rating "
            "FROM sales GROUP BY {cat} ORDER BY total_sales DESC"
        ).format(
            cat=q(config.COL_CATEGORY),
            sales=q(config.COL_MONTHLY_SALES),
            actual=q(config.COL_PRICE_ACTUAL),
            disc=q(config.COL_DISCOUNT),
            rating=q(config.COL_RATING),
        )
        results["by_category"] = [_row_to_dict(row) for row in spark.sql(sql_cat).collect()]

    # 6) 渠道
    if config.COL_CHANNEL in df.columns:
        sql_ch = (
            "SELECT {ch} AS channel, SUM({sales}) AS total_sales, COUNT(*) AS count, "
            "AVG({rating}) AS avg_rating "
            "FROM sales GROUP BY {ch} ORDER BY total_sales DESC"
        ).format(
            ch=q(config.COL_CHANNEL),
            sales=q(config.COL_MONTHLY_SALES),
            rating=q(config.COL_RATING),
        )
        results["by_channel"] = [_row_to_dict(row) for row in spark.sql(sql_ch).collect()]

    # 7) 商圈/区县 — 地区消费结构
    if config.COL_DISTRICT in df.columns:
        sql_dist = (
            "SELECT {d} AS district, SUM({sales}) AS total_sales, COUNT(*) AS project_count, "
            "AVG({actual}) AS avg_actual_price, AVG({rating}) AS avg_rating "
            "FROM sales GROUP BY {d} ORDER BY total_sales DESC LIMIT 40"
        ).format(
            d=q(config.COL_DISTRICT),
            sales=q(config.COL_MONTHLY_SALES),
            actual=q(config.COL_PRICE_ACTUAL),
            rating=q(config.COL_RATING),
        )
        results["by_district"] = [_row_to_dict(row) for row in spark.sql(sql_dist).collect()]

    # 8) 项目子类
    if config.COL_SUBCATEGORY in df.columns:
        sql_sub = (
            "SELECT {s} AS subcategory, SUM({sales}) AS total_sales, COUNT(*) AS project_count "
            "FROM sales GROUP BY {s} ORDER BY total_sales DESC LIMIT 30"
        ).format(s=q(config.COL_SUBCATEGORY), sales=q(config.COL_MONTHLY_SALES))
        results["by_subcategory"] = [_row_to_dict(row) for row in spark.sql(sql_sub).collect()]

    # 9) 门店类型
    if config.COL_STORE_TYPE in df.columns:
        sql_st = (
            "SELECT {st} AS store_type, SUM({sales}) AS total_sales, COUNT(*) AS project_count, "
            "AVG({actual}) AS avg_actual_price "
            "FROM sales GROUP BY {st} ORDER BY total_sales DESC"
        ).format(
            st=q(config.COL_STORE_TYPE),
            sales=q(config.COL_MONTHLY_SALES),
            actual=q(config.COL_PRICE_ACTUAL),
        )
        results["by_store_type"] = [_row_to_dict(row) for row in spark.sql(sql_st).collect()]

    # 10) 平台
    if config.COL_PLATFORM in df.columns:
        sql_pf = (
            "SELECT {p} AS platform, SUM({sales}) AS total_sales, COUNT(*) AS project_count, "
            "AVG({rating}) AS avg_rating "
            "FROM sales GROUP BY {p} ORDER BY total_sales DESC"
        ).format(p=q(config.COL_PLATFORM), sales=q(config.COL_MONTHLY_SALES), rating=q(config.COL_RATING))
        results["by_platform"] = [_row_to_dict(row) for row in spark.sql(sql_pf).collect()]

    # 11) 项目销售 TOP（项目名称）
    if config.COL_PROJECT_NAME in df.columns:
        sql_proj = (
            "SELECT {pn} AS project_name, SUM({sales}) AS total_sales, COUNT(*) AS listing_count, "
            "AVG({actual}) AS avg_actual_price, AVG({rating}) AS avg_rating "
            "FROM sales GROUP BY {pn} ORDER BY total_sales DESC LIMIT 25"
        ).format(
            pn=q(config.COL_PROJECT_NAME),
            sales=q(config.COL_MONTHLY_SALES),
            actual=q(config.COL_PRICE_ACTUAL),
            rating=q(config.COL_RATING),
        )
        results["top_projects"] = [_row_to_dict(row) for row in spark.sql(sql_proj).collect()]

    # 12) 医生头衔与口碑、销量
    if config.COL_DOCTOR_TITLE in df.columns:
        sql_dt = (
            "SELECT {dt} AS doctor_title, SUM({sales}) AS total_sales, COUNT(*) AS project_count, "
            "AVG({rating}) AS avg_rating, AVG({actual}) AS avg_actual_price "
            "FROM sales GROUP BY {dt} ORDER BY total_sales DESC LIMIT 20"
        ).format(
            dt=q(config.COL_DOCTOR_TITLE),
            sales=q(config.COL_MONTHLY_SALES),
            rating=q(config.COL_RATING),
            actual=q(config.COL_PRICE_ACTUAL),
        )
        results["doctor_title_stats"] = [_row_to_dict(row) for row in spark.sql(sql_dt).collect()]

    # 13) 折扣率分桶 vs 平均销量（理解促销力度与动销）
    if config.COL_DISCOUNT in df.columns:
        dcol = q(config.COL_DISCOUNT)
        disc_bucket = (
            "CASE WHEN {dc} IS NULL THEN '未知' "
            "WHEN {dc} < 0.7 THEN '低折扣(<0.7)' "
            "WHEN {dc} < 0.85 THEN '中折扣(0.7-0.85)' "
            "WHEN {dc} <= 1.0 THEN '高折扣(>=0.85)' ELSE '其它' END"
        ).format(dc=dcol)
        sql_bucket = (
            "SELECT {bucket} AS discount_band, COUNT(*) AS project_count, "
            "AVG({sales}) AS avg_monthly_sales, AVG({rating}) AS avg_rating "
            "FROM sales GROUP BY {bucket} ORDER BY avg_monthly_sales DESC"
        ).format(
            bucket=disc_bucket,
            sales=q(config.COL_MONTHLY_SALES),
            rating=q(config.COL_RATING),
        )
        results["discount_band_stats"] = [_row_to_dict(row) for row in spark.sql(sql_bucket).collect()]

    # 14) 城市×品类 TOP — 地区消费偏好（联合维度）
    if config.COL_CITY in df.columns and config.COL_CATEGORY in df.columns:
        sql_cc = (
            "SELECT {city} AS city, {cat} AS category, SUM({sales}) AS total_sales, COUNT(*) AS project_count "
            "FROM sales GROUP BY {city}, {cat} "
            "ORDER BY total_sales DESC LIMIT 40"
        ).format(
            city=q(config.COL_CITY),
            cat=q(config.COL_CATEGORY),
            sales=q(config.COL_MONTHLY_SALES),
        )
        results["city_category_top"] = [_row_to_dict(row) for row in spark.sql(sql_cc).collect()]

    # 15) 聚类分析（如果使用聚类数据）
    if use_clustered_data and "cluster" in df.columns:
        # 聚类整体统计
        sql_cluster_summary = (
            "SELECT cluster, COUNT(*) AS count, "
            "AVG({sales}) AS avg_monthly_sales, AVG({rating}) AS avg_rating, "
            "AVG({actual}) AS avg_actual_price, AVG({orig}) AS avg_list_price, AVG({disc}) AS avg_discount "
            "FROM sales GROUP BY cluster ORDER BY cluster"
        ).format(
            sales=q(config.COL_MONTHLY_SALES),
            rating=q(config.COL_RATING),
            actual=q(config.COL_PRICE_ACTUAL),
            orig=q(config.COL_PRICE_ORIGINAL),
            disc=q(config.COL_DISCOUNT),
        )
        results["cluster_summary"] = [_row_to_dict(row) for row in spark.sql(sql_cluster_summary).collect()]

        # 聚类×城市分析
        if config.COL_CITY in df.columns:
            sql_cluster_city = (
                "SELECT cluster, {city} AS city, COUNT(*) AS count, "
                "AVG({sales}) AS avg_monthly_sales, AVG({actual}) AS avg_actual_price "
                "FROM sales GROUP BY cluster, {city} "
                "ORDER BY cluster, avg_monthly_sales DESC"
            ).format(
                city=q(config.COL_CITY),
                sales=q(config.COL_MONTHLY_SALES),
                actual=q(config.COL_PRICE_ACTUAL),
            )
            results["cluster_by_city"] = [_row_to_dict(row) for row in spark.sql(sql_cluster_city).collect()]

        # 聚类×品类分析
        if config.COL_CATEGORY in df.columns:
            sql_cluster_cat = (
                "SELECT cluster, {cat} AS category, COUNT(*) AS count, "
                "AVG({sales}) AS avg_monthly_sales, AVG({actual}) AS avg_actual_price "
                "FROM sales GROUP BY cluster, {cat} "
                "ORDER BY cluster, avg_monthly_sales DESC"
            ).format(
                cat=q(config.COL_CATEGORY),
                sales=q(config.COL_MONTHLY_SALES),
                actual=q(config.COL_PRICE_ACTUAL),
            )
            results["cluster_by_category"] = [_row_to_dict(row) for row in spark.sql(sql_cluster_cat).collect()]

        # 聚类×渠道分析
        if config.COL_CHANNEL in df.columns:
            sql_cluster_ch = (
                "SELECT cluster, {ch} AS channel, COUNT(*) AS count, "
                "AVG({sales}) AS avg_monthly_sales, AVG({rating}) AS avg_rating "
                "FROM sales GROUP BY cluster, {ch} "
                "ORDER BY cluster, avg_monthly_sales DESC"
            ).format(
                ch=q(config.COL_CHANNEL),
                sales=q(config.COL_MONTHLY_SALES),
                rating=q(config.COL_RATING),
            )
            results["cluster_by_channel"] = [_row_to_dict(row) for row in spark.sql(sql_cluster_ch).collect()]

    trend_path = os.path.join(output_dir, "trend_stats.json")
    with open(trend_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    spark.stop()
    return {"trend_stats_json": trend_path, "keys": list(results.keys())}


def run_clustered_analysis(output_dir=None):
    """
    专门对聚类后的数据进行分析。
    这个函数会读取聚类后的数据，并进行基于聚类的多维分析。

    Args:
        output_dir: 输出目录

    Returns:
        包含聚类分析结果的字典
    """
    output_dir = output_dir or config.OUTPUT_DIR
    clustered_path = config.clustered_parquet_uri(output_dir)

    if not os.path.exists(clustered_path.replace("hdfs://localhost:9000", "")) and not clustered_path.startswith("hdfs://"):
        raise FileNotFoundError(f"聚类数据文件不存在: {clustered_path}")

    return run_trend_analysis(parquet_path=clustered_path, output_dir=output_dir, use_clustered_data=True)


# ================================================================
# 数据仓库 DWS 层：基于 K-Means 聚类结果的多主题宽表派生
# ================================================================
#
# 设计目的：把聚类结果当成一级数据产物，再派生 8 张主题表，每张表在 Spark SQL 里
# 使用 CTE / 窗口函数 / 多表 JOIN / 多条件 CASE WHEN 等更丰富的构造方式，
# 物化为独立 Parquet 文件，并把聚合结果汇总到 cluster_insights.json 供看板读取。
#
# 主题覆盖：
#   1) cluster_profile              聚类画像 + 业务标签（RANK + CASE WHEN）
#   2) cluster_city_rank            聚类×城市 + 窗口排名（ROW_NUMBER / RANK）
#   3) cluster_category_preference  聚类品类偏好度 Lift（多表 JOIN + CTE）
#   4) cluster_channel_efficiency   聚类×渠道效率（加权销量 + RANK）
#   5) cluster_price_band           聚类×折扣分桶（CASE WHEN + PERCENT_RANK）
#   6) cluster_top_projects         聚类 TOP medical-beauty-spark-analysis（ROW_NUMBER WHERE rn<=5）
#   7) cluster_doctor_matching      聚类×医生头衔（COUNT DISTINCT + RANK）
#   8) cluster_month_delta          聚类月度环比（LAG + CTE + COALESCE）
# ================================================================


_WAREHOUSE_DIRNAME = "cluster_tables"


def cluster_warehouse_dir(output_dir=None):
    """聚类派生主题表的输出目录。"""
    if config.USE_HDFS:
        return config.hdfs_uri("output", _WAREHOUSE_DIRNAME)
    output_dir = output_dir or config.OUTPUT_DIR
    return os.path.join(output_dir, _WAREHOUSE_DIRNAME)


def _cast_numeric_cols(df, cols):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))
    return df


def _write_spark_parquet(df, path):
    """Spark 写 Parquet，Windows 无 winutils 时回退 pandas。"""
    try:
        df.write.mode("overwrite").parquet(path)
    except Exception:
        pdf = df.toPandas()
        _write_pandas_parquet(pdf, path)


def _join_storage_path(base, *parts):
    """同时兼容 Windows 本地路径与 hdfs:// URI 的路径拼接。"""
    if is_hdfs_uri(base):
        cleaned = [str(p).replace("\\", "/").strip("/") for p in parts if p]
        if not cleaned:
            return base
        return base.rstrip("/") + "/" + "/".join(cleaned)
    return os.path.join(base, *parts)


def _ensure_storage_dir(path):
    """HDFS 目录由 Spark/HDFS 客户端创建，本地目录才用 os.makedirs。"""
    if not is_hdfs_uri(path):
        os.makedirs(path, exist_ok=True)


def _write_pandas_parquet(pdf, path):
    """pandas 回退写 Parquet，并保持 Spark 可读取的时间戳精度。"""
    if is_hdfs_uri(path):
        write_parquet_from_pandas(pdf, path)
        return

    if os.path.isdir(path):
        path = os.path.join(path, "part-00000.parquet")
    else:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    pdf.to_parquet(
        path,
        index=False,
        engine="pyarrow",
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )


def _collect_as_dicts(df, limit=None):
    rows = df.limit(limit).collect() if limit else df.collect()
    return [_row_to_dict(r) for r in rows]


def run_cluster_warehouse(clustered_parquet=None, output_dir=None):
    """
    基于 K-Means 聚类结果派生 8 张主题宽表，物化为独立 Parquet，并汇总为
    cluster_insights.json 供看板读取。

    技术点：
      - CTE（WITH 多级子查询）、多表 JOIN、CROSS JOIN
      - 窗口函数：ROW_NUMBER / RANK / PERCENT_RANK / LAG
      - 多条件 CASE WHEN 打业务标签
      - 聚合函数：SUM、AVG、COUNT、COUNT DISTINCT
      - 加权指标、偏好度 Lift、月度环比增速

    Args:
        clustered_parquet: 聚类后 Parquet 路径，默认取 config.clustered_parquet_uri()
        output_dir:        输出根目录；主题表写入 output_dir/cluster_tables/
    Returns:
        dict: {warehouse_dir, insights_json, tables, total_rows, k}
    """
    clustered_parquet = clustered_parquet or config.clustered_parquet_uri()
    output_dir = output_dir or config.OUTPUT_DIR
    config.ensure_dirs()
    warehouse_dir = cluster_warehouse_dir(output_dir)
    _ensure_storage_dir(warehouse_dir)

    spark = _get_spark()
    df = spark.read.parquet(clustered_parquet)

    df = _cast_numeric_cols(
        df,
        [
            config.COL_PRICE_ORIGINAL,
            config.COL_PRICE_ACTUAL,
            config.COL_DISCOUNT,
            config.COL_MONTHLY_SALES,
            config.COL_REVIEW_COUNT,
            config.COL_RATING,
        ],
    )
    if "cluster" in df.columns:
        df = df.withColumn("cluster", F.col("cluster").cast("int"))
    else:
        spark.stop()
        raise ValueError("输入的 Parquet 缺少 cluster 列，请先运行 run_clustering()。")

    df.createOrReplaceTempView("sales_clustered")
    q = _quote_ident

    insights = {}
    tables_written = []

    def _emit(name, sql, row_limit=None):
        result = spark.sql(sql)
        path = _join_storage_path(warehouse_dir, name + ".parquet")
        _write_spark_parquet(result, path)
        # 注册临时视图，方便后续主题表 JOIN 之前已物化的表
        result.createOrReplaceTempView(name)
        insights[name] = _collect_as_dicts(result, limit=row_limit)
        tables_written.append(name)
        return result

    # ---------------- 1) 聚类画像 + 业务标签 ----------------
    sql_profile = (
        "WITH agg AS (\n"
        "  SELECT\n"
        "    cluster,\n"
        "    COUNT(*) AS project_count,\n"
        "    SUM({sales}) AS total_sales,\n"
        "    AVG({orig}) AS avg_list_price,\n"
        "    AVG({actual}) AS avg_actual_price,\n"
        "    AVG({disc}) AS avg_discount,\n"
        "    AVG({sales}) AS avg_monthly_sales,\n"
        "    AVG({rev}) AS avg_review_count,\n"
        "    AVG({rating}) AS avg_rating\n"
        "  FROM sales_clustered\n"
        "  WHERE cluster IS NOT NULL\n"
        "  GROUP BY cluster\n"
        "),\n"
        "ranked AS (\n"
        "  SELECT *,\n"
        "    RANK() OVER (ORDER BY avg_list_price DESC)      AS price_rank,\n"
        "    RANK() OVER (ORDER BY avg_monthly_sales DESC)   AS sales_rank,\n"
        "    RANK() OVER (ORDER BY avg_list_price ASC)       AS price_rank_asc,\n"
        "    RANK() OVER (ORDER BY avg_monthly_sales ASC)    AS sales_rank_asc\n"
        "  FROM agg\n"
        ")\n"
        "SELECT\n"
        "  cluster,\n"
        "  project_count, total_sales,\n"
        "  avg_list_price, avg_actual_price, avg_discount,\n"
        "  avg_monthly_sales, avg_review_count, avg_rating,\n"
        "  price_rank, sales_rank,\n"
        "  CASE\n"
        "    WHEN price_rank = 1 THEN '高端精品型'\n"
        "    WHEN sales_rank = 1 THEN '大众爆款型'\n"
        "    WHEN price_rank_asc = 1 AND sales_rank_asc = 1 THEN '长尾引流型'\n"
        "    ELSE '中端主力型'\n"
        "  END AS cluster_label,\n"
        "  CASE\n"
        "    WHEN avg_discount < 0.7  THEN '深折促销'\n"
        "    WHEN avg_discount < 0.85 THEN '中度折扣'\n"
        "    ELSE '高折扣/原价'\n"
        "  END AS discount_level\n"
        "FROM ranked\n"
        "ORDER BY cluster\n"
    ).format(
        sales=q(config.COL_MONTHLY_SALES),
        orig=q(config.COL_PRICE_ORIGINAL),
        actual=q(config.COL_PRICE_ACTUAL),
        disc=q(config.COL_DISCOUNT),
        rev=q(config.COL_REVIEW_COUNT),
        rating=q(config.COL_RATING),
    )
    _emit("cluster_profile", sql_profile)

    # ---------------- 2) 聚类×城市 + 窗口排名 ----------------
    sql_city = (
        "WITH city_cluster AS (\n"
        "  SELECT\n"
        "    cluster,\n"
        "    {city} AS city,\n"
        "    SUM({sales}) AS total_sales,\n"
        "    COUNT(*) AS project_count,\n"
        "    AVG({actual}) AS avg_actual_price,\n"
        "    AVG({rating}) AS avg_rating\n"
        "  FROM sales_clustered\n"
        "  WHERE cluster IS NOT NULL AND {city} IS NOT NULL\n"
        "  GROUP BY cluster, {city}\n"
        ")\n"
        "SELECT\n"
        "  cluster, city,\n"
        "  total_sales, project_count, avg_actual_price, avg_rating,\n"
        "  ROW_NUMBER() OVER (PARTITION BY cluster ORDER BY total_sales DESC) AS rank_in_cluster,\n"
        "  RANK()       OVER (PARTITION BY city    ORDER BY total_sales DESC) AS cluster_rank_in_city\n"
        "FROM city_cluster\n"
        "ORDER BY cluster, rank_in_cluster\n"
    ).format(
        city=q(config.COL_CITY),
        sales=q(config.COL_MONTHLY_SALES),
        actual=q(config.COL_PRICE_ACTUAL),
        rating=q(config.COL_RATING),
    )
    _emit("cluster_city_rank", sql_city, row_limit=150)

    # ---------------- 3) 聚类品类偏好 Lift（多表 JOIN） ----------------
    sql_pref = (
        "WITH cluster_cat AS (\n"
        "  SELECT cluster, {cat} AS category,\n"
        "         SUM({sales}) AS cat_sales, COUNT(*) AS cat_count\n"
        "  FROM sales_clustered\n"
        "  WHERE cluster IS NOT NULL AND {cat} IS NOT NULL\n"
        "  GROUP BY cluster, {cat}\n"
        "),\n"
        "cluster_total AS (\n"
        "  SELECT cluster, SUM({sales}) AS cluster_sales, COUNT(*) AS cluster_count\n"
        "  FROM sales_clustered\n"
        "  WHERE cluster IS NOT NULL\n"
        "  GROUP BY cluster\n"
        "),\n"
        "global_cat AS (\n"
        "  SELECT {cat} AS category,\n"
        "         SUM({sales}) AS global_cat_sales, COUNT(*) AS global_cat_count\n"
        "  FROM sales_clustered\n"
        "  WHERE {cat} IS NOT NULL\n"
        "  GROUP BY {cat}\n"
        "),\n"
        "global_total AS (\n"
        "  SELECT SUM({sales}) AS grand_sales, COUNT(*) AS grand_count FROM sales_clustered\n"
        ")\n"
        "SELECT\n"
        "  cc.cluster,\n"
        "  cc.category,\n"
        "  cc.cat_sales, cc.cat_count,\n"
        "  CASE WHEN ct.cluster_sales > 0 THEN cc.cat_sales / ct.cluster_sales END AS cluster_share,\n"
        "  CASE WHEN gt.grand_sales   > 0 THEN gc.global_cat_sales / gt.grand_sales END AS global_share,\n"
        "  CASE\n"
        "    WHEN ct.cluster_sales > 0 AND gc.global_cat_sales > 0 AND gt.grand_sales > 0\n"
        "    THEN (cc.cat_sales / ct.cluster_sales) / (gc.global_cat_sales / gt.grand_sales)\n"
        "  END AS preference_lift,\n"
        "  RANK() OVER (PARTITION BY cc.cluster ORDER BY cc.cat_sales DESC) AS cat_rank_in_cluster\n"
        "FROM cluster_cat cc\n"
        "JOIN cluster_total ct ON cc.cluster  = ct.cluster\n"
        "JOIN global_cat    gc ON cc.category = gc.category\n"
        "CROSS JOIN global_total gt\n"
        "ORDER BY cc.cluster, preference_lift DESC NULLS LAST\n"
    ).format(
        cat=q(config.COL_CATEGORY),
        sales=q(config.COL_MONTHLY_SALES),
    )
    _emit("cluster_category_preference", sql_pref, row_limit=200)

    # ---------------- 4) 聚类×渠道效率 ----------------
    sql_channel = (
        "WITH chc AS (\n"
        "  SELECT\n"
        "    cluster,\n"
        "    {ch} AS channel,\n"
        "    SUM({sales}) AS total_sales,\n"
        "    COUNT(*) AS project_count,\n"
        "    AVG({rating}) AS avg_rating,\n"
        "    SUM({sales} * {rating}) AS weighted_sales,\n"
        "    AVG({actual}) AS avg_actual_price\n"
        "  FROM sales_clustered\n"
        "  WHERE cluster IS NOT NULL AND {ch} IS NOT NULL\n"
        "  GROUP BY cluster, {ch}\n"
        ")\n"
        "SELECT\n"
        "  cluster, channel,\n"
        "  total_sales, project_count, avg_rating, weighted_sales, avg_actual_price,\n"
        "  CASE WHEN project_count > 0 THEN weighted_sales / project_count END AS efficiency_score,\n"
        "  RANK() OVER (PARTITION BY cluster ORDER BY weighted_sales DESC) AS channel_rank_in_cluster,\n"
        "  RANK() OVER (PARTITION BY channel ORDER BY weighted_sales DESC) AS cluster_rank_in_channel\n"
        "FROM chc\n"
        "ORDER BY cluster, channel_rank_in_cluster\n"
    ).format(
        ch=q(config.COL_CHANNEL),
        sales=q(config.COL_MONTHLY_SALES),
        rating=q(config.COL_RATING),
        actual=q(config.COL_PRICE_ACTUAL),
    )
    _emit("cluster_channel_efficiency", sql_channel)

    # ---------------- 5) 聚类×折扣分桶 + PERCENT_RANK ----------------
    sql_band = (
        "WITH banded AS (\n"
        "  SELECT\n"
        "    cluster,\n"
        "    CASE\n"
        "      WHEN {disc} IS NULL THEN '未知'\n"
        "      WHEN {disc} < 0.6  THEN '深折(<0.6)'\n"
        "      WHEN {disc} < 0.75 THEN '中折(0.6-0.75)'\n"
        "      WHEN {disc} < 0.9  THEN '浅折(0.75-0.9)'\n"
        "      ELSE '原价(>=0.9)'\n"
        "    END AS discount_band,\n"
        "    {sales}  AS sales,\n"
        "    {rating} AS rating,\n"
        "    {actual} AS actual_price\n"
        "  FROM sales_clustered\n"
        "  WHERE cluster IS NOT NULL\n"
        "),\n"
        "agg AS (\n"
        "  SELECT\n"
        "    cluster, discount_band,\n"
        "    COUNT(*) AS project_count,\n"
        "    SUM(sales)  AS total_sales,\n"
        "    AVG(sales)  AS avg_sales,\n"
        "    AVG(rating) AS avg_rating,\n"
        "    AVG(actual_price) AS avg_price\n"
        "  FROM banded\n"
        "  GROUP BY cluster, discount_band\n"
        ")\n"
        "SELECT *,\n"
        "  PERCENT_RANK() OVER (PARTITION BY cluster ORDER BY total_sales) AS sales_pct_in_cluster,\n"
        "  RANK()         OVER (PARTITION BY cluster ORDER BY total_sales DESC) AS band_rank_in_cluster\n"
        "FROM agg\n"
        "ORDER BY cluster, band_rank_in_cluster\n"
    ).format(
        disc=q(config.COL_DISCOUNT),
        sales=q(config.COL_MONTHLY_SALES),
        rating=q(config.COL_RATING),
        actual=q(config.COL_PRICE_ACTUAL),
    )
    _emit("cluster_price_band", sql_band)

    # ---------------- 6) 聚类 TOP medical-beauty-spark-analysis（ROW_NUMBER） ----------------
    sql_top = (
        "WITH proj AS (\n"
        "  SELECT\n"
        "    cluster,\n"
        "    {pn} AS project_name,\n"
        "    SUM({sales}) AS total_sales,\n"
        "    AVG({actual}) AS avg_actual_price,\n"
        "    AVG({rating}) AS avg_rating,\n"
        "    COUNT(*) AS listing_count\n"
        "  FROM sales_clustered\n"
        "  WHERE cluster IS NOT NULL AND {pn} IS NOT NULL\n"
        "  GROUP BY cluster, {pn}\n"
        "),\n"
        "ranked AS (\n"
        "  SELECT *,\n"
        "    ROW_NUMBER() OVER (PARTITION BY cluster ORDER BY total_sales DESC) AS rn\n"
        "  FROM proj\n"
        ")\n"
        "SELECT cluster, project_name, total_sales, avg_actual_price, avg_rating, listing_count, rn\n"
        "FROM ranked\n"
        "WHERE rn <= 5\n"
        "ORDER BY cluster, rn\n"
    ).format(
        pn=q(config.COL_PROJECT_NAME),
        sales=q(config.COL_MONTHLY_SALES),
        actual=q(config.COL_PRICE_ACTUAL),
        rating=q(config.COL_RATING),
    )
    _emit("cluster_top_projects", sql_top)

    # ---------------- 7) 聚类×医生头衔（COUNT DISTINCT + RANK） ----------------
    sql_doc = (
        "WITH dt AS (\n"
        "  SELECT\n"
        "    cluster,\n"
        "    {dt} AS doctor_title,\n"
        "    COUNT(*) AS project_count,\n"
        "    COUNT(DISTINCT {dn}) AS doctor_count,\n"
        "    SUM({sales}) AS total_sales,\n"
        "    AVG({rating}) AS avg_rating,\n"
        "    AVG({actual}) AS avg_actual_price\n"
        "  FROM sales_clustered\n"
        "  WHERE cluster IS NOT NULL AND {dt} IS NOT NULL\n"
        "  GROUP BY cluster, {dt}\n"
        ")\n"
        "SELECT *,\n"
        "  RANK() OVER (PARTITION BY cluster ORDER BY total_sales DESC) AS title_rank_in_cluster\n"
        "FROM dt\n"
        "ORDER BY cluster, title_rank_in_cluster\n"
    ).format(
        dt=q(config.COL_DOCTOR_TITLE),
        dn=q(config.COL_DOCTOR_NAME),
        sales=q(config.COL_MONTHLY_SALES),
        rating=q(config.COL_RATING),
        actual=q(config.COL_PRICE_ACTUAL),
    )
    _emit("cluster_doctor_matching", sql_doc, row_limit=100)

    # ---------------- 8) 聚类月度环比（LAG） ----------------
    sql_month = (
        "WITH monthly AS (\n"
        "  SELECT\n"
        "    cluster,\n"
        "    {m} AS stats_month,\n"
        "    SUM({sales}) AS total_sales,\n"
        "    COUNT(*) AS project_count,\n"
        "    AVG({actual}) AS avg_price\n"
        "  FROM sales_clustered\n"
        "  WHERE cluster IS NOT NULL AND {m} IS NOT NULL AND {m} <> '未知'\n"
        "  GROUP BY cluster, {m}\n"
        "),\n"
        "with_lag AS (\n"
        "  SELECT *,\n"
        "    LAG(total_sales)  OVER (PARTITION BY cluster ORDER BY stats_month) AS prev_sales,\n"
        "    LAG(project_count) OVER (PARTITION BY cluster ORDER BY stats_month) AS prev_count\n"
        "  FROM monthly\n"
        ")\n"
        "SELECT\n"
        "  cluster, stats_month,\n"
        "  total_sales, project_count, avg_price,\n"
        "  prev_sales, prev_count,\n"
        "  (total_sales - COALESCE(prev_sales, 0)) AS sales_delta,\n"
        "  CASE\n"
        "    WHEN prev_sales IS NULL OR prev_sales = 0 THEN NULL\n"
        "    ELSE (total_sales - prev_sales) / prev_sales\n"
        "  END AS sales_growth_rate\n"
        "FROM with_lag\n"
        "ORDER BY cluster, stats_month\n"
    ).format(
        m=q(config.COL_STATS_MONTH),
        sales=q(config.COL_MONTHLY_SALES),
        actual=q(config.COL_PRICE_ACTUAL),
    )
    _emit("cluster_month_delta", sql_month)

    # ---------------- 元信息 + 聚类中心（便于前端一次拉取） ----------------
    total_rows = df.count()
    k_from_profile = len(insights.get("cluster_profile", []))

    centers_path = os.path.join(output_dir, "cluster_centers.json")
    if os.path.isfile(centers_path):
        try:
            with open(centers_path, "r", encoding="utf-8") as f:
                insights["cluster_centers"] = json.load(f)
        except (OSError, json.JSONDecodeError):
            pass

    insights["_metadata"] = {
        "k": k_from_profile,
        "total_rows": int(total_rows),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "tables": tables_written,
        "warehouse_dir": warehouse_dir,
    }

    insights_path = os.path.join(output_dir, "cluster_insights.json")
    with open(insights_path, "w", encoding="utf-8") as f:
        json.dump(insights, f, ensure_ascii=False, indent=2)

    spark.stop()
    return {
        "warehouse_dir": warehouse_dir,
        "insights_json": insights_path,
        "tables": tables_written,
        "total_rows": int(total_rows),
        "k": k_from_profile,
    }


# ================================================================
# 高级分析：统计检验 & 相关性 / 关联 & 结构 / 合规风险
# ================================================================
#
# 与聚类仓库同级的 ADS 层扩展：补齐 GROUP BY 之外的"量化关系"指标。
# 所有函数读取 clean_data.parquet（不依赖聚类结果），独立产出 JSON/Parquet，
# 便于 Flask 看板直接读。技术点：
#   - Spark SQL 的 corr() / regr_slope / regr_r2（相关与回归）
#   - 窗口函数 SUM() OVER / ROW_NUMBER + CTE（HHI / CR10 / 帕累托）
#   - Spark + pandas 混合（城市×品类销量矩阵 → numpy 余弦相似度）
#   - 多条件 CASE WHEN 组合合规指标，按权重合成机构风险分
# ================================================================


_ADVANCED_DIRNAME = "advanced_tables"


def advanced_warehouse_dir(output_dir=None):
    """高级分析派生表的输出目录。"""
    if config.USE_HDFS:
        return config.hdfs_uri("output", _ADVANCED_DIRNAME)
    output_dir = output_dir or config.OUTPUT_DIR
    return os.path.join(output_dir, _ADVANCED_DIRNAME)


# 6 个数值特征对，供相关矩阵使用
_ADVANCED_NUMERIC_COLS = [
    config.COL_PRICE_ORIGINAL,
    config.COL_PRICE_ACTUAL,
    config.COL_DISCOUNT,
    config.COL_MONTHLY_SALES,
    config.COL_REVIEW_COUNT,
    config.COL_RATING,
]


def _prepare_advanced_df(spark, parquet_path):
    """读取清洗后的 Parquet，转数值类型并注册临时视图 sales_advanced。"""
    df = spark.read.parquet(parquet_path)
    df = _cast_numeric_cols(df, _ADVANCED_NUMERIC_COLS)
    if "udi_match" in df.columns:
        df = df.withColumn("udi_match", F.col("udi_match").cast("double"))
    df.createOrReplaceTempView("sales_advanced")
    return df


# -------------------- 一、统计检验 & 相关性 --------------------
def run_statistical_analysis(parquet_path=None, output_dir=None):
    """
    统计检验 & 相关性分析：
      1) Pearson 相关矩阵（6 个数值特征两两）
      2) Spearman 相关矩阵（等价于排名后 Pearson，用窗口 RANK 实现）
      3) 价格弹性：按品类分组 ln(sales) ~ ln(price) 的一元线性回归
      4) 评分-销量相关性：按城市、按品类分层

    产物：
      - advanced_tables/stat_price_elasticity.parquet
      - advanced_tables/stat_rating_sales_by_city.parquet
      - advanced_tables/stat_rating_sales_by_category.parquet
      - statistical_analysis.json（含矩阵、分层相关、弹性排名）
    """
    parquet_path = parquet_path or config.clean_parquet_uri()
    output_dir = output_dir or config.OUTPUT_DIR
    config.ensure_dirs()
    warehouse_dir = advanced_warehouse_dir(output_dir)
    _ensure_storage_dir(warehouse_dir)

    spark = _get_spark()
    df = _prepare_advanced_df(spark, parquet_path)
    q = _quote_ident

    results = {}
    available = [c for c in _ADVANCED_NUMERIC_COLS if c in df.columns]

    def _corr_matrix(view_name, col_ref):
        """一次 SELECT 取 N*(N-1)/2 个 corr，再展开成对称矩阵。"""
        if not available:
            return []
        pairs = []
        parts = []
        for i, a in enumerate(available):
            for b in available[i + 1:]:
                alias = "c__{}__{}".format(i, available.index(b))
                parts.append("corr({a}, {b}) AS {al}".format(a=col_ref(a), b=col_ref(b), al=alias))
                pairs.append((alias, a, b))
        if not parts:
            return []
        sql = "SELECT " + ", ".join(parts) + " FROM " + view_name
        row = spark.sql(sql).collect()[0]
        out = []
        for a in available:
            out.append({"feature_a": a, "feature_b": a, "corr": 1.0})
        for alias, a, b in pairs:
            v = row[alias]
            val = float(v) if v is not None else None
            out.append({"feature_a": a, "feature_b": b, "corr": val})
            out.append({"feature_a": b, "feature_b": a, "corr": val})
        return out

    # 1) Pearson 相关矩阵
    results["pearson_matrix"] = _corr_matrix("sales_advanced", lambda c: q(c))
    results["features"] = available

    # 2) Spearman：排名后 Pearson
    rank_sql_parts = []
    for c in available:
        rank_sql_parts.append(
            "RANK() OVER (ORDER BY {col}) AS r_{alias}".format(col=q(c), alias=c)
        )
    if rank_sql_parts:
        ranked_sql = (
            "SELECT " + ", ".join(rank_sql_parts) + " FROM sales_advanced WHERE "
            + " AND ".join("{} IS NOT NULL".format(q(c)) for c in available)
        )
        spark.sql(ranked_sql).createOrReplaceTempView("sales_ranked")
        results["spearman_matrix"] = _corr_matrix("sales_ranked", lambda c: "r_" + c)

    # 3) 价格弹性：按品类分组 ln(sales) ~ ln(price)
    if config.COL_CATEGORY in df.columns:
        sql_elast = (
            "WITH base AS (\n"
            "  SELECT {cat} AS category,\n"
            "         LN({actual}) AS ln_price,\n"
            "         LN({sales})  AS ln_sales\n"
            "  FROM sales_advanced\n"
            "  WHERE {cat} IS NOT NULL AND {actual} > 0 AND {sales} > 0\n"
            ")\n"
            "SELECT category,\n"
            "       regr_slope(ln_sales, ln_price)     AS elasticity,\n"
            "       regr_intercept(ln_sales, ln_price) AS intercept,\n"
            "       regr_r2(ln_sales, ln_price)        AS r2,\n"
            "       COUNT(*) AS n\n"
            "FROM base\n"
            "GROUP BY category\n"
            "ORDER BY elasticity ASC NULLS LAST\n"
        ).format(
            cat=q(config.COL_CATEGORY),
            actual=q(config.COL_PRICE_ACTUAL),
            sales=q(config.COL_MONTHLY_SALES),
        )
        elast_df = spark.sql(sql_elast)
        _write_spark_parquet(elast_df, _join_storage_path(warehouse_dir, "stat_price_elasticity.parquet"))
        results["price_elasticity_by_category"] = _collect_as_dicts(elast_df)

    # 4) 评分-销量相关性：按城市分层
    if config.COL_CITY in df.columns:
        sql_rc = (
            "SELECT {city} AS city,\n"
            "       corr({rating}, {sales}) AS corr_rating_sales,\n"
            "       COUNT(*) AS n,\n"
            "       AVG({rating}) AS avg_rating,\n"
            "       AVG({sales})  AS avg_sales\n"
            "FROM sales_advanced\n"
            "WHERE {city} IS NOT NULL\n"
            "GROUP BY {city}\n"
            "HAVING COUNT(*) >= 30\n"
            "ORDER BY corr_rating_sales DESC NULLS LAST\n"
        ).format(
            city=q(config.COL_CITY),
            rating=q(config.COL_RATING),
            sales=q(config.COL_MONTHLY_SALES),
        )
        rc_city = spark.sql(sql_rc)
        _write_spark_parquet(rc_city, _join_storage_path(warehouse_dir, "stat_rating_sales_by_city.parquet"))
        results["rating_sales_corr_by_city"] = _collect_as_dicts(rc_city)

    # 5) 评分-销量相关性：按品类分层
    if config.COL_CATEGORY in df.columns:
        sql_rcat = (
            "SELECT {cat} AS category,\n"
            "       corr({rating}, {sales}) AS corr_rating_sales,\n"
            "       COUNT(*) AS n,\n"
            "       AVG({rating}) AS avg_rating,\n"
            "       AVG({sales})  AS avg_sales\n"
            "FROM sales_advanced\n"
            "WHERE {cat} IS NOT NULL\n"
            "GROUP BY {cat}\n"
            "ORDER BY corr_rating_sales DESC NULLS LAST\n"
        ).format(
            cat=q(config.COL_CATEGORY),
            rating=q(config.COL_RATING),
            sales=q(config.COL_MONTHLY_SALES),
        )
        rc_cat = spark.sql(sql_rcat)
        _write_spark_parquet(rc_cat, _join_storage_path(warehouse_dir, "stat_rating_sales_by_category.parquet"))
        results["rating_sales_corr_by_category"] = _collect_as_dicts(rc_cat)

    out_path = os.path.join(output_dir, "statistical_analysis.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    spark.stop()
    return {"statistical_json": out_path, "warehouse_dir": warehouse_dir}


# -------------------- 二、关联 & 结构分析 --------------------
def run_structure_analysis(parquet_path=None, output_dir=None):
    """
    关联 & 结构分析：
      1) 机构集中度 HHI + CR10（整体 / 按城市 / 按品类）
      2) 项目帕累托 20/80（累计销量分布）
      3) 城市相似度矩阵：基于城市×品类销量的余弦相似度

    产物：
      - advanced_tables/struct_hhi_overall.parquet
      - advanced_tables/struct_hhi_by_city.parquet
      - advanced_tables/struct_hhi_by_category.parquet
      - advanced_tables/struct_pareto_projects.parquet
      - advanced_tables/struct_city_similarity.parquet
      - structure_analysis.json
    """
    parquet_path = parquet_path or config.clean_parquet_uri()
    output_dir = output_dir or config.OUTPUT_DIR
    config.ensure_dirs()
    warehouse_dir = advanced_warehouse_dir(output_dir)
    _ensure_storage_dir(warehouse_dir)

    spark = _get_spark()
    df = _prepare_advanced_df(spark, parquet_path)
    q = _quote_ident

    results = {}

    # 1) 全局 HHI + CR10：一次聚合
    sql_hhi_overall = (
        "WITH inst_sales AS (\n"
        "  SELECT {inst} AS institution, SUM({sales}) AS ss\n"
        "  FROM sales_advanced\n"
        "  WHERE {inst} IS NOT NULL\n"
        "  GROUP BY {inst}\n"
        "),\n"
        "with_total AS (\n"
        "  SELECT institution, ss,\n"
        "         SUM(ss) OVER ()                         AS total_ss,\n"
        "         ROW_NUMBER() OVER (ORDER BY ss DESC)    AS rn\n"
        "  FROM inst_sales\n"
        ")\n"
        "SELECT\n"
        "  SUM(POWER(ss / total_ss, 2)) * 10000 AS hhi,\n"
        "  SUM(CASE WHEN rn <= 10 THEN ss / total_ss END) AS cr10,\n"
        "  SUM(CASE WHEN rn <= 5  THEN ss / total_ss END) AS cr5,\n"
        "  COUNT(*) AS institution_count,\n"
        "  MAX(total_ss) AS total_sales\n"
        "FROM with_total\n"
    ).format(inst=q(config.COL_INSTITUTION), sales=q(config.COL_MONTHLY_SALES))
    overall_df = spark.sql(sql_hhi_overall)
    _write_spark_parquet(overall_df, _join_storage_path(warehouse_dir, "struct_hhi_overall.parquet"))
    overall_rows = _collect_as_dicts(overall_df)
    results["hhi_overall"] = overall_rows[0] if overall_rows else {}

    # 2) 按城市 HHI
    if config.COL_CITY in df.columns:
        sql_hhi_city = (
            "WITH inst_city AS (\n"
            "  SELECT {city} AS city, {inst} AS institution, SUM({sales}) AS ss\n"
            "  FROM sales_advanced\n"
            "  WHERE {city} IS NOT NULL AND {inst} IS NOT NULL\n"
            "  GROUP BY {city}, {inst}\n"
            "),\n"
            "with_total AS (\n"
            "  SELECT city, institution, ss,\n"
            "         SUM(ss) OVER (PARTITION BY city)                         AS total_ss,\n"
            "         ROW_NUMBER() OVER (PARTITION BY city ORDER BY ss DESC)   AS rn\n"
            "  FROM inst_city\n"
            ")\n"
            "SELECT city,\n"
            "       SUM(POWER(ss / total_ss, 2)) * 10000 AS hhi,\n"
            "       SUM(CASE WHEN rn <= 10 THEN ss / total_ss END) AS cr10,\n"
            "       COUNT(*) AS institution_count,\n"
            "       MAX(total_ss) AS total_sales,\n"
            "       CASE\n"
            "         WHEN SUM(POWER(ss / total_ss, 2)) * 10000 < 1500 THEN '竞争充分'\n"
            "         WHEN SUM(POWER(ss / total_ss, 2)) * 10000 < 2500 THEN '中度集中'\n"
            "         ELSE '高度集中'\n"
            "       END AS concentration_level\n"
            "FROM with_total\n"
            "GROUP BY city\n"
            "ORDER BY hhi DESC\n"
        ).format(
            city=q(config.COL_CITY),
            inst=q(config.COL_INSTITUTION),
            sales=q(config.COL_MONTHLY_SALES),
        )
        city_df = spark.sql(sql_hhi_city)
        _write_spark_parquet(city_df, _join_storage_path(warehouse_dir, "struct_hhi_by_city.parquet"))
        results["hhi_by_city"] = _collect_as_dicts(city_df)

    # 3) 按品类 HHI
    if config.COL_CATEGORY in df.columns:
        sql_hhi_cat = (
            "WITH inst_cat AS (\n"
            "  SELECT {cat} AS category, {inst} AS institution, SUM({sales}) AS ss\n"
            "  FROM sales_advanced\n"
            "  WHERE {cat} IS NOT NULL AND {inst} IS NOT NULL\n"
            "  GROUP BY {cat}, {inst}\n"
            "),\n"
            "with_total AS (\n"
            "  SELECT category, institution, ss,\n"
            "         SUM(ss) OVER (PARTITION BY category)                         AS total_ss,\n"
            "         ROW_NUMBER() OVER (PARTITION BY category ORDER BY ss DESC)   AS rn\n"
            "  FROM inst_cat\n"
            ")\n"
            "SELECT category,\n"
            "       SUM(POWER(ss / total_ss, 2)) * 10000 AS hhi,\n"
            "       SUM(CASE WHEN rn <= 10 THEN ss / total_ss END) AS cr10,\n"
            "       COUNT(*) AS institution_count\n"
            "FROM with_total\n"
            "GROUP BY category\n"
            "ORDER BY hhi DESC\n"
        ).format(
            cat=q(config.COL_CATEGORY),
            inst=q(config.COL_INSTITUTION),
            sales=q(config.COL_MONTHLY_SALES),
        )
        cat_df = spark.sql(sql_hhi_cat)
        _write_spark_parquet(cat_df, _join_storage_path(warehouse_dir, "struct_hhi_by_category.parquet"))
        results["hhi_by_category"] = _collect_as_dicts(cat_df)

    # 4) 帕累托 20/80：按项目销量累计
    if config.COL_PROJECT_NAME in df.columns:
        sql_pareto = (
            "WITH proj AS (\n"
            "  SELECT {pn} AS project_name, SUM({sales}) AS total_sales\n"
            "  FROM sales_advanced\n"
            "  WHERE {pn} IS NOT NULL\n"
            "  GROUP BY {pn}\n"
            "),\n"
            "ranked AS (\n"
            "  SELECT project_name, total_sales,\n"
            "         ROW_NUMBER() OVER (ORDER BY total_sales DESC) AS rn,\n"
            "         SUM(total_sales) OVER (ORDER BY total_sales DESC\n"
            "                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_sales,\n"
            "         SUM(total_sales) OVER () AS grand_sales,\n"
            "         COUNT(*) OVER ()         AS grand_count\n"
            "  FROM proj\n"
            ")\n"
            "SELECT project_name, total_sales, rn, cum_sales,\n"
            "       cum_sales / grand_sales AS cum_share,\n"
            "       CAST(rn AS DOUBLE) / grand_count AS project_share\n"
            "FROM ranked\n"
            "ORDER BY rn\n"
        ).format(pn=q(config.COL_PROJECT_NAME), sales=q(config.COL_MONTHLY_SALES))
        pareto_df = spark.sql(sql_pareto)
        _write_spark_parquet(pareto_df, _join_storage_path(warehouse_dir, "struct_pareto_projects.parquet"))
        pareto_rows = _collect_as_dicts(pareto_df)

        # 关键切点：找到 cum_share 首次 >= 0.5 / 0.8 / 0.95 的项目份额
        def _first_reach(rows, threshold):
            for row in rows:
                if row.get("cum_share") is not None and row["cum_share"] >= threshold:
                    return {
                        "cum_share_threshold": threshold,
                        "project_share": row.get("project_share"),
                        "rn": row.get("rn"),
                    }
            return None

        breakpoints = [
            _first_reach(pareto_rows, 0.5),
            _first_reach(pareto_rows, 0.8),
            _first_reach(pareto_rows, 0.95),
        ]
        # Parquet 行数较多时只保留 1000 行给前端（每 N 行采样一次）
        step = max(1, len(pareto_rows) // 500) if pareto_rows else 1
        sampled = pareto_rows[::step] if step > 1 else pareto_rows
        results["pareto_projects"] = sampled
        results["pareto_breakpoints"] = [b for b in breakpoints if b is not None]

    # 5) 城市相似度矩阵：基于城市×品类销量的余弦相似度
    if config.COL_CITY in df.columns and config.COL_CATEGORY in df.columns:
        sql_cc = (
            "SELECT {city} AS city, {cat} AS category, SUM({sales}) AS total_sales\n"
            "FROM sales_advanced\n"
            "WHERE {city} IS NOT NULL AND {cat} IS NOT NULL\n"
            "GROUP BY {city}, {cat}\n"
        ).format(
            city=q(config.COL_CITY),
            cat=q(config.COL_CATEGORY),
            sales=q(config.COL_MONTHLY_SALES),
        )
        cc_df = spark.sql(sql_cc)
        import pandas as pd
        import numpy as np

        pdf = cc_df.toPandas()
        if len(pdf) > 0:
            mat = pdf.pivot_table(
                index="city", columns="category", values="total_sales", fill_value=0
            )
            arr = mat.values.astype(float)
            norms = np.linalg.norm(arr, axis=1, keepdims=True)
            norms[norms == 0] = 1.0
            normalized = arr / norms
            sim = normalized @ normalized.T
            cities = list(mat.index)

            sim_rows = []
            for i, ci in enumerate(cities):
                for j, cj in enumerate(cities):
                    sim_rows.append({"city_a": ci, "city_b": cj, "similarity": float(sim[i, j])})
            sim_pdf = pd.DataFrame(sim_rows)
            _write_pandas_parquet(
                sim_pdf, _join_storage_path(warehouse_dir, "struct_city_similarity.parquet")
            )

            # 取每个城市 Top-5 相似城市（排除自身）
            topk = []
            for i, ci in enumerate(cities):
                order = np.argsort(-sim[i])
                picks = []
                for idx in order:
                    if cities[idx] == ci:
                        continue
                    picks.append({"city": cities[idx], "similarity": float(sim[i, idx])})
                    if len(picks) >= 5:
                        break
                topk.append({"city": ci, "top_similar": picks})
            results["city_similarity_matrix"] = sim_rows
            results["city_similarity_top"] = topk
            results["city_similarity_cities"] = cities

    out_path = os.path.join(output_dir, "structure_analysis.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    spark.stop()
    return {"structure_json": out_path, "warehouse_dir": warehouse_dir}


# -------------------- 三、合规风险打分 --------------------
def run_compliance_analysis(parquet_path=None, output_dir=None):
    """
    合规风险打分：
      1) 按机构/品类统计 UDI 不匹配率、疑似无效批准文号率、低评分率、价格异常率
      2) 合成机构合规风险分（加权）并排名

    输出：
      - advanced_tables/comp_institution_risk.parquet
      - advanced_tables/comp_category_risk.parquet
      - advanced_tables/comp_city_risk.parquet
      - compliance_analysis.json
    """
    parquet_path = parquet_path or config.clean_parquet_uri()
    output_dir = output_dir or config.OUTPUT_DIR
    config.ensure_dirs()
    warehouse_dir = advanced_warehouse_dir(output_dir)
    _ensure_storage_dir(warehouse_dir)

    spark = _get_spark()
    df = _prepare_advanced_df(spark, parquet_path)
    q = _quote_ident

    results = {}

    # 全局价格均值 / 标准差（用于 z-score 异常判定）
    price_row = spark.sql(
        "SELECT AVG({actual}) AS mu, STDDEV_POP({actual}) AS sd FROM sales_advanced WHERE {actual} IS NOT NULL".format(
            actual=q(config.COL_PRICE_ACTUAL)
        )
    ).collect()
    mu = float(price_row[0]["mu"]) if price_row and price_row[0]["mu"] is not None else 0.0
    sd = float(price_row[0]["sd"]) if price_row and price_row[0]["sd"] is not None else 0.0
    results["price_baseline"] = {"mu": mu, "sd": sd}

    # approval_no: 合法医疗器械批准文号一般以 "国械" 开头；其它视为疑似无效
    # udi_match: 1=匹配, 0=不匹配（整型在 _prepare_advanced_df 里已转 double）
    common_metrics = (
        "AVG(CASE WHEN {udi} IS NULL OR {udi} = 0 THEN 1.0 ELSE 0.0 END)         AS udi_mismatch_rate,\n"
        "AVG(CASE WHEN {ap} IS NULL OR {ap} IN ('未知','','nan','None')\n"
        "         OR {ap} NOT LIKE '国械%' THEN 1.0 ELSE 0.0 END)               AS invalid_approval_rate,\n"
        "AVG(CASE WHEN {rating} < 4.0 THEN 1.0 ELSE 0.0 END)                    AS low_rating_rate,\n"
        "AVG(CASE WHEN {sd} > 0 AND ABS(({actual} - {mu_v}) / {sd_v}) > 3\n"
        "         THEN 1.0 ELSE 0.0 END)                                       AS price_anomaly_rate,\n"
        "AVG({rating})      AS avg_rating,\n"
        "AVG({actual})      AS avg_actual_price,\n"
        "SUM({sales})       AS total_sales,\n"
        "COUNT(*)           AS project_count"
    ).format(
        udi=q("udi_match"),
        ap=q(config.COL_APPROVAL_NO),
        rating=q(config.COL_RATING),
        actual=q(config.COL_PRICE_ACTUAL),
        sales=q(config.COL_MONTHLY_SALES),
        sd=str(sd),
        sd_v=str(sd if sd and sd > 0 else 1.0),
        mu_v=str(mu),
    )

    # 1) 机构合规画像 + 风险分（加权合成，归一化到 0-100）
    sql_inst = (
        "WITH base AS (\n"
        "  SELECT {inst} AS institution,\n"
        "         " + common_metrics + "\n"
        "  FROM sales_advanced\n"
        "  WHERE {inst} IS NOT NULL\n"
        "  GROUP BY {inst}\n"
        "  HAVING COUNT(*) >= 5\n"
        ")\n"
        "SELECT institution, project_count, total_sales,\n"
        "       udi_mismatch_rate, invalid_approval_rate,\n"
        "       low_rating_rate, price_anomaly_rate,\n"
        "       avg_rating, avg_actual_price,\n"
        "       100.0 * (0.40 * udi_mismatch_rate\n"
        "              + 0.30 * invalid_approval_rate\n"
        "              + 0.20 * low_rating_rate\n"
        "              + 0.10 * price_anomaly_rate) AS risk_score,\n"
        "       CASE\n"
        "         WHEN 100.0 * (0.40 * udi_mismatch_rate + 0.30 * invalid_approval_rate\n"
        "                     + 0.20 * low_rating_rate  + 0.10 * price_anomaly_rate) >= 30 THEN '高风险'\n"
        "         WHEN 100.0 * (0.40 * udi_mismatch_rate + 0.30 * invalid_approval_rate\n"
        "                     + 0.20 * low_rating_rate  + 0.10 * price_anomaly_rate) >= 15 THEN '中风险'\n"
        "         ELSE '低风险'\n"
        "       END AS risk_level,\n"
        "       RANK() OVER (ORDER BY 100.0 * (0.40 * udi_mismatch_rate\n"
        "                                   + 0.30 * invalid_approval_rate\n"
        "                                   + 0.20 * low_rating_rate\n"
        "                                   + 0.10 * price_anomaly_rate) DESC) AS risk_rank\n"
        "FROM base\n"
        "ORDER BY risk_score DESC\n"
    ).format(inst=q(config.COL_INSTITUTION))
    inst_df = spark.sql(sql_inst)
    _write_spark_parquet(inst_df, _join_storage_path(warehouse_dir, "comp_institution_risk.parquet"))
    # 全量落 Parquet，前端只取 Top-50 + Bottom-10
    inst_rows = _collect_as_dicts(inst_df)
    results["institution_risk_top"] = inst_rows[:50]
    results["institution_risk_bottom"] = inst_rows[-10:] if len(inst_rows) >= 10 else []
    results["institution_count_total"] = len(inst_rows)
    if inst_rows:
        # 风险等级分布
        level_counter = {}
        for r in inst_rows:
            level_counter[r.get("risk_level")] = level_counter.get(r.get("risk_level"), 0) + 1
        results["institution_risk_distribution"] = [
            {"risk_level": k, "count": v} for k, v in level_counter.items()
        ]

    # 2) 品类合规画像
    if config.COL_CATEGORY in df.columns:
        sql_cat = (
            "SELECT {cat} AS category,\n"
            "       " + common_metrics + "\n"
            "FROM sales_advanced\n"
            "WHERE {cat} IS NOT NULL\n"
            "GROUP BY {cat}\n"
            "ORDER BY udi_mismatch_rate DESC\n"
        ).format(cat=q(config.COL_CATEGORY))
        cat_df = spark.sql(sql_cat)
        _write_spark_parquet(cat_df, _join_storage_path(warehouse_dir, "comp_category_risk.parquet"))
        results["category_risk"] = _collect_as_dicts(cat_df)

    # 3) 城市合规画像
    if config.COL_CITY in df.columns:
        sql_city = (
            "SELECT {city} AS city,\n"
            "       " + common_metrics + "\n"
            "FROM sales_advanced\n"
            "WHERE {city} IS NOT NULL\n"
            "GROUP BY {city}\n"
            "ORDER BY udi_mismatch_rate DESC\n"
        ).format(city=q(config.COL_CITY))
        city_df = spark.sql(sql_city)
        _write_spark_parquet(city_df, _join_storage_path(warehouse_dir, "comp_city_risk.parquet"))
        results["city_risk"] = _collect_as_dicts(city_df)

    out_path = os.path.join(output_dir, "compliance_analysis.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    spark.stop()
    return {"compliance_json": out_path, "warehouse_dir": warehouse_dir}


def run_advanced_analysis(parquet_path=None, output_dir=None):
    """
    一次性跑三块高级分析，并汇总到 advanced_insights.json。
    """
    output_dir = output_dir or config.OUTPUT_DIR
    config.ensure_dirs()

    r1 = run_statistical_analysis(parquet_path, output_dir)
    r2 = run_structure_analysis(parquet_path, output_dir)
    r3 = run_compliance_analysis(parquet_path, output_dir)

    insights = {}
    for path_key, section in [
        (r1["statistical_json"], "statistical"),
        (r2["structure_json"], "structure"),
        (r3["compliance_json"], "compliance"),
    ]:
        try:
            with open(path_key, "r", encoding="utf-8") as f:
                insights[section] = json.load(f)
        except (OSError, json.JSONDecodeError):
            insights[section] = {}

    insights["_metadata"] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "warehouse_dir": advanced_warehouse_dir(output_dir),
    }

    summary_path = os.path.join(output_dir, "advanced_insights.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(insights, f, ensure_ascii=False, indent=2)

    return {
        "statistical_json": r1["statistical_json"],
        "structure_json": r2["structure_json"],
        "compliance_json": r3["compliance_json"],
        "advanced_insights_json": summary_path,
    }


def run(parquet_path=None, output_dir=None, k=None, use_clustered_data=True,
        build_warehouse=True, build_advanced=True):
    """
    执行聚类与趋势分析。

    Args:
        parquet_path:       数据文件路径（默认 clean_data.parquet）
        output_dir:         输出目录
        k:                  聚类数量（默认 config.KMEANS_K）
        use_clustered_data: 趋势分析时是否使用聚类后的数据（默认 True）
        build_warehouse:    是否额外派生聚类主题宽表（DWS 层，默认 True）
        build_advanced:     是否跑高级分析（统计检验/结构/合规，默认 True）
    """
    run_clustering(parquet_path, k, output_dir)
    run_trend_analysis(parquet_path, output_dir, use_clustered_data)
    if build_warehouse:
        run_cluster_warehouse(output_dir=output_dir)
    if build_advanced:
        run_advanced_analysis(parquet_path, output_dir)
    return {"status": "ok"}


if __name__ == "__main__":
    run()
