# -*- coding: utf-8 -*-
"""
数据采集与预处理模块
功能：读取医美销售 CSV，去重、缺失值处理、异常值处理、日期修复（如 ########）
说明：全程使用 pandas 处理，避免 Windows 下 PySpark Python worker 连接超时；输出 Parquet 供 Spark 分析使用。
"""

import os
import sys
import re

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from src import config
from src.hdfs_io import is_hdfs_uri, read_csv_pandas, write_parquet_from_pandas


def _read_csv_with_pandas(path, encoding=None):
    """用 pandas 读取 CSV，自动尝试 utf-8 / gbk / gb18030，返回 (pandas.DataFrame, encoding)。"""
    if encoding:
        try:
            pdf = pd.read_csv(path, encoding=encoding, dtype=str, on_bad_lines="skip")
            return pdf, encoding
        except Exception:
            pass
    for enc in ["utf-8", "gbk", "gb18030"]:
        try:
            pdf = pd.read_csv(path, encoding=enc, dtype=str, on_bad_lines="skip")
            return pdf, enc
        except Exception:
            continue
    raise RuntimeError("无法用 utf-8/gbk/gb18030 解码 CSV，请检查文件编码。")


def _standardize_columns(pdf):
    """只保留前 28 列并统一列名为 CLEAN_HEADERS。"""
    n = min(28, len(pdf.columns), len(config.CLEAN_HEADERS))
    cols = list(pdf.columns[:n])
    pdf = pdf[cols].copy()
    pdf.columns = config.CLEAN_HEADERS[:n]
    for i, h in enumerate(config.CLEAN_HEADERS):
        if i >= n:
            pdf[h] = None
    return pdf[[c for c in config.CLEAN_HEADERS if c in pdf.columns]]


def _cast_columns(pdf):
    """数值列、日期列转为正确类型；无效或 ######## 置为 NaN。"""
    date_pattern = re.compile(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}$")

    def parse_date(v):
        if pd.isna(v) or v is None or str(v).strip() in ("", "########"):
            return pd.NaT
        s = str(v).strip()
        if not date_pattern.match(s):
            return pd.NaT
        try:
            return pd.to_datetime(s)
        except Exception:
            return pd.NaT

    for c in config.NUMERIC_COLS:
        if c in pdf.columns:
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce")
    for c in config.DATE_COLS:
        if c in pdf.columns:
            pdf[c] = pdf[c].apply(parse_date)
    # 「月销量」与采集批次对应：用采集日期的年月作为统计月份，便于按月份筛选与趋势分析
    if config.COL_COLLECTION_DATE in pdf.columns:
        pdf[config.COL_STATS_MONTH] = pdf[config.COL_COLLECTION_DATE].apply(
            lambda x: x.strftime("%Y-%m") if pd.notna(x) else "未知"
        )
    return pdf


def _fill_missing(pdf):
    """缺失值：数值列用中位数，字符串列用 '未知'。"""
    numeric = [c for c in config.NUMERIC_COLS if c in pdf.columns]
    for c in numeric:
        if pdf[c].isna().any():
            pdf[c] = pdf[c].fillna(pdf[c].median())
    str_cols = [c for c in pdf.columns if c not in config.NUMERIC_COLS and c not in config.DATE_COLS]
    for c in str_cols:
        pdf[c] = pdf[c].fillna("未知").astype(str)
    return pdf


def _remove_outliers_iqr(pdf):
    """基于 IQR 剔除数值列异常值。"""
    out = pdf.copy()
    for c in config.NUMERIC_COLS:
        if c not in out.columns:
            continue
        q1, q3 = out[c].quantile(0.25), out[c].quantile(0.75)
        iqr = q3 - q1
        if iqr <= 0:
            continue
        low, high = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        out = out[(out[c] >= low) & (out[c] <= high)]
    return out


def _split_single_month_into_two_month_labels(pdf):
    """
    若清洗后「统计月份」仅有一个非「未知」取值，将全表按机构/项目/城市稳定排序后二等分：
    前半标为「上一自然月」、后半保持原年月，以便看板出现两个统计月份作对比。
    说明：仅用于同源单批数据的演示拆分，并非真实多期采集。
    """
    col = config.COL_STATS_MONTH
    if col not in pdf.columns or len(pdf) < 2:
        return pdf
    ser = pdf[col].astype(str)
    mask = ser.notna() & (ser != "未知") & (ser != "nan")
    if not mask.any():
        return pdf
    distinct = pd.Series(ser[mask].unique())
    if len(distinct) != 1:
        return pdf
    base = str(distinct.iloc[0])
    try:
        period = pd.Period(base + "-01", freq="M")
        prev_m = (period - 1).strftime("%Y-%m")
    except (ValueError, TypeError):
        return pdf
    sort_cols = [
        c
        for c in (
            config.COL_INSTITUTION,
            config.COL_PROJECT_NAME,
            config.COL_CITY,
        )
        if c in pdf.columns
    ]
    ordered = pdf.sort_values(sort_cols, kind="mergesort") if sort_cols else pdf.sort_index(kind="mergesort")
    idx = ordered.index.tolist()
    mid = len(idx) // 2
    first, second = idx[:mid], idx[mid:]
    pdf = pdf.copy()
    pdf.loc[first, col] = prev_m
    pdf.loc[second, col] = base
    return pdf


def run(input_path=None, output_path=None, encoding=None, remove_outliers=True):
    """
    执行完整预处理流程（纯 pandas，不启动 Spark）。
    """
    input_path = input_path or config.raw_csv_input()
    output_path = output_path or config.clean_parquet_uri()
    encoding = encoding or config.CSV_ENCODING
    config.ensure_dirs()

    if is_hdfs_uri(input_path):
        pdf, used_encoding = read_csv_pandas(input_path, encoding)
    else:
        pdf, used_encoding = _read_csv_with_pandas(input_path, encoding)
    total_before = len(pdf)

    pdf = _standardize_columns(pdf)
    pdf = _cast_columns(pdf)
    pdf = pdf.drop_duplicates()
    pdf = _fill_missing(pdf)
    if remove_outliers:
        pdf = _remove_outliers_iqr(pdf)
    pdf = _split_single_month_into_two_month_labels(pdf)
    total_after = len(pdf)

    if is_hdfs_uri(output_path):
        write_parquet_from_pandas(pdf, output_path)
    else:
        pdf.to_parquet(
            output_path,
            index=False,
            engine="pyarrow",
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )
    return {
        "total_before": total_before,
        "total_after": total_after,
        "output_path": output_path,
        "encoding_used": used_encoding,
    }


if __name__ == "__main__":
    result = run(
        encoding=os.environ.get("CSV_ENCODING", config.CSV_ENCODING),
        remove_outliers=True,
    )
    print("预处理完成:", result)
