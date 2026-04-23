# -*- coding: utf-8 -*-
"""项目配置：路径、编码、存储等"""

import os

# 项目根目录（project/）
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
DB_DIR = os.path.join(PROJECT_ROOT, "database")

# 原始数据文件（本地模式）：UTF-8 编码 + 英文列名，避免 HDFS/Docker 等环境乱码
RAW_CSV = os.path.join(DATA_DIR, "medical_beauty_data.csv")
# 清洗后 Parquet（本地模式，供 Spark 使用）
CLEAN_PARQUET = os.path.join(OUTPUT_DIR, "clean_data.parquet")

# ---------- HDFS 模式（环境变量 USE_HDFS=1 启用）----------
# 大数据走 HDFS；trend_stats.json / cluster_centers.json 仍写本地 output，供看板读取。
USE_HDFS = os.environ.get("USE_HDFS", "0").lower() in ("1", "true", "yes")
HDFS_NAMENODE_HOST = os.environ.get("HDFS_NAMENODE_HOST", "localhost")
HDFS_NAMENODE_PORT = int(os.environ.get("HDFS_NAMENODE_PORT", "9000"))
HDFS_USER = os.environ.get("HDFS_USER") or os.environ.get("HADOOP_USER_NAME") or "hdfs"
# HDFS 上工作目录（不含 hdfs://host:port 前缀），如 /user/medical_beauty
HDFS_WORKDIR = os.environ.get("HDFS_WORKDIR", "/user/medical_beauty").strip().strip("/")


def hdfs_uri(*relative_parts):
    """拼 hdfs://host:port + HDFS_WORKDIR + 相对路径。"""
    rel = "/".join(p.strip("/") for p in relative_parts if p)
    base = "hdfs://{}:{}".format(HDFS_NAMENODE_HOST, HDFS_NAMENODE_PORT)
    if HDFS_WORKDIR:
        return "{}/{}/{}".format(base, HDFS_WORKDIR, rel)
    return "{}/{}".format(base, rel)


def raw_csv_input():
    """预处理读取的 CSV 路径（本地或 HDFS）。"""
    if USE_HDFS:
        rel = os.environ.get("HDFS_RAW_REL", "raw/medical_beauty_data.csv")
        return hdfs_uri(rel)
    return RAW_CSV


def clean_parquet_uri():
    """清洗结果 Parquet（本地或 HDFS）。"""
    if USE_HDFS:
        rel = os.environ.get("HDFS_CLEAN_REL", "clean/clean_data.parquet")
        return hdfs_uri(rel)
    return CLEAN_PARQUET


def clustered_parquet_uri(output_dir=None):
    """聚类结果 Parquet（本地或 HDFS）。"""
    if USE_HDFS:
        rel = os.environ.get("HDFS_CLUSTERED_REL", "output/clustered_data.parquet")
        return hdfs_uri(rel)
    od = output_dir or OUTPUT_DIR
    return os.path.join(od, "clustered_data.parquet")


def spark_default_fs():
    """Spark 默认文件系统 URI。"""
    return "hdfs://{}:{}".format(HDFS_NAMENODE_HOST, HDFS_NAMENODE_PORT)
# 看板用户 JSON（注册用户、个人资料）
DASHBOARD_USERS_JSON = os.path.join(DB_DIR, "dashboard_users.json")

# CSV 编码：若报错可改为 'gbk' 或 'gb18030'
CSV_ENCODING = "utf-8"

# HADOOP_HOME（Windows 下 Spark 写本地文件需 winutils）
# 候选路径：D:\hadoop、D:\ProgramFiles\hadoop 等，以存在 bin\winutils.exe 为准
def _valid_hadoop_home(candidate):
    if not candidate:
        return False
    return os.path.isfile(os.path.join(candidate, "bin", "winutils.exe"))


def _detect_hadoop_home():
    # 若已设置 HADOOP_HOME 且该路径有效（存在 bin\winutils.exe），则使用并规范为绝对路径
    env_home = os.environ.get("HADOOP_HOME")
    if env_home and _valid_hadoop_home(env_home):
        return os.path.abspath(env_home)
    # 否则依次尝试常见路径：优先 D:\hadoop，再 D:\ProgramFiles\hadoop 等
    for candidate in [
        r"D:\hadoop",
        r"D:\ProgramFiles\hadoop",
        os.path.join(os.environ.get("PROGRAMFILES", "C:\\Program Files"), "hadoop"),
    ]:
        if _valid_hadoop_home(candidate):
            return os.path.abspath(candidate)
    return None


HADOOP_HOME = _detect_hadoop_home()


def ensure_hadoop_home():
    """
    在启动 Spark 前调用：设置 HADOOP_HOME，并将 HADOOP_HOME\\bin 加入 PATH，
    以便 JVM 能加载 hadoop.dll，避免 winutils / NativeIO 相关报错。
    """
    home = HADOOP_HOME or os.environ.get("HADOOP_HOME")
    if not home or not _valid_hadoop_home(home):
        return
    home = os.path.abspath(home)
    os.environ["HADOOP_HOME"] = home
    bin_dir = os.path.join(home, "bin")
    if os.path.isdir(bin_dir):
        path = os.environ.get("PATH", "")
        if bin_dir not in path:
            os.environ["PATH"] = bin_dir + os.pathsep + path


# 聚类数量
KMEANS_K = 3

# 可视化看板登录（可用环境变量覆盖，生产环境务必修改）
# 设置环境变量：DASHBOARD_USER、DASHBOARD_PASS、FLASK_SECRET_KEY
SECRET_KEY = os.environ.get("FLASK_SECRET_KEY", "dev-change-me-medical-beauty-dashboard")
DASHBOARD_USER = os.environ.get("DASHBOARD_USER", "admin")
DASHBOARD_PASS = os.environ.get("DASHBOARD_PASS", "admin123")

# 列名（英文，与 medical_beauty_data.csv 一致；避免 HDFS/Docker/Spark 对中文列名的编码问题）
COL_PLATFORM = "platform"
COL_CITY = "city"
COL_DISTRICT = "district"
COL_STORE_TYPE = "store_type"
COL_INSTITUTION = "institution"
COL_CATEGORY = "category"
COL_SUBCATEGORY = "subcategory"
COL_SPEC = "spec"
COL_PROJECT_NAME = "project_name"
COL_PRICE_ORIGINAL = "list_price"
COL_PRICE_ACTUAL = "actual_price"
COL_DISCOUNT = "discount_rate"
COL_MONTHLY_SALES = "monthly_sales"
COL_REVIEW_COUNT = "review_count"
COL_RATING = "rating"
COL_DOCTOR_NAME = "doctor_name"
COL_DOCTOR_TITLE = "doctor_title"
COL_PAYMENT = "payment_method"
COL_INSTALLMENT = "installment"
COL_CHANNEL = "channel"
COL_ACTIVITY_TAG = "activity_tag"
COL_APPROVAL_NO = "approval_no"
COL_DEVICE_BRAND = "device_brand"
COL_DEVICE_MODEL = "device_model"
COL_UDI_MATCH = "udi_match"
COL_LISTING_DATE = "listing_date"
COL_COLLECTION_DATE = "collection_date"
COL_NEW_CUSTOMER = "new_customer"
# 由采集日期派生：表示该行 monthly_sales 字段对应的统计月份（YYYY-MM）
COL_STATS_MONTH = "stats_month"

# 原始 CSV 表头（与 medical_beauty_data.csv 的第一行一致）
RAW_HEADERS = [
    "platform", "city", "district", "store_type", "institution",
    "category", "subcategory", "spec", "project_name",
    "list_price", "actual_price", "discount_rate", "monthly_sales",
    "review_count", "rating", "doctor_name", "doctor_title",
    "payment_method", "installment", "channel", "activity_tag", "approval_no",
    "device_brand", "device_model", "udi_match",
    "listing_date", "collection_date", "new_customer",
]

# 标准化后的列名（用于 DataFrame）
CLEAN_HEADERS = [
    COL_PLATFORM, COL_CITY, COL_DISTRICT, COL_STORE_TYPE, COL_INSTITUTION,
    COL_CATEGORY, COL_SUBCATEGORY, COL_SPEC, COL_PROJECT_NAME,
    COL_PRICE_ORIGINAL, COL_PRICE_ACTUAL, COL_DISCOUNT, COL_MONTHLY_SALES,
    COL_REVIEW_COUNT, COL_RATING, COL_DOCTOR_NAME, COL_DOCTOR_TITLE,
    COL_PAYMENT, COL_INSTALLMENT, COL_CHANNEL, COL_ACTIVITY_TAG, COL_APPROVAL_NO,
    COL_DEVICE_BRAND, COL_DEVICE_MODEL, COL_UDI_MATCH,
    COL_LISTING_DATE, COL_COLLECTION_DATE, COL_NEW_CUSTOMER,
]

NUMERIC_COLS = [
    COL_PRICE_ORIGINAL, COL_PRICE_ACTUAL, COL_DISCOUNT,
    COL_MONTHLY_SALES, COL_REVIEW_COUNT, COL_RATING,
    COL_INSTALLMENT, COL_UDI_MATCH, COL_NEW_CUSTOMER,
]

DATE_COLS = [COL_LISTING_DATE, COL_COLLECTION_DATE]


def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(DB_DIR, exist_ok=True)
