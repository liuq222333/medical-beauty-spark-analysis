import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src import config

import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler


parquet_path = config.clean_parquet_uri()

# 读取数据
df = pd.read_parquet(parquet_path)

# 聚类用的特征
FEATURE_COLS = [
    config.COL_PRICE_ORIGINAL,
    config.COL_PRICE_ACTUAL,
    config.COL_DISCOUNT,
    config.COL_MONTHLY_SALES,
    config.COL_REVIEW_COUNT,
    config.COL_RATING,
]

# 清洗数据
available = [c for c in FEATURE_COLS if c in df.columns]
df = df[available].dropna()

# 标准化
scaler = StandardScaler()
X_scaled = scaler.fit_transform(df)

# 肘部法则
wcss = []
k_range = range(2, 9)

print("正在计算 K=2~8 的肘部法则…")
for k in k_range:
    kmeans = KMeans(n_clusters=k, random_state=42)
    kmeans.fit(X_scaled)
    wcss.append(kmeans.inertia_)
    print(f"K = {k} → WCSS = {kmeans.inertia_:.2f}")

# 解决乱码问题：设置中文字体
plt.rcParams["font.sans-serif"] = ["SimHei", "DejaVu Sans"]  # 黑体，Windows 自带
plt.rcParams["axes.unicode_minus"] = False  # 解决负号显示问题

# 画图
plt.figure(figsize=(9, 5))
plt.plot(k_range, wcss, marker='o', linewidth=2)
plt.title("肘部法则曲线（最优K值）")
plt.xlabel("聚类数量 K")
plt.ylabel("簇内平方和 WCSS")
plt.grid(True)
plt.show()