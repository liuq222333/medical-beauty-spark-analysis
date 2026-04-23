# 更新日志

本次迭代围绕三个目标：**项目结构整理**、**数据分析层增厚（基于 K-Means 的数据仓库 DWS 层）**、**数据集国际化（UTF-8 英文列名）**。

---

## 一、项目结构整理

**背景**：原项目分散在两个子目录 `1/` 和 `2/`（各自一半代码和一半配置），路径上不完整。

**改动**：把两个子包的内容合并到项目根目录 `项目/`，形成标准的 Python 工程布局：

```
项目/
├── data/                    # 原始数据 CSV
├── output/                  # Parquet / JSON 产物
├── database/                # 看板用户 JSON
├── doc/                     # 项目说明文档
├── src/                     # 所有 Python 源码
│   ├── config.py
│   ├── preprocess.py
│   ├── storage.py
│   ├── analysis.py
│   ├── visualize.py
│   ├── dashboard_auth.py
│   ├── hdfs_io.py
│   ├── elbow_plot.py
│   ├── templates/           # Flask HTML 模板
│   └── static/              # CSS / JS / 图片 / 用户上传
├── run_all.py               # 一键跑完整流程
├── requirements.txt
├── docker-compose.hdfs.yml
└── README.md
```

---

## 二、数据分析层重构：加入基于 K-Means 的数据仓库 DWS 层

**背景**：原版分析模块只是对清洗后的 Parquet 跑了十几段 `GROUP BY` 语句，K-Means 聚类结果没有被真正利用——只是在同一份数据里多加了三段 `GROUP BY cluster` 的简单聚合。

**改动**：把 K-Means 聚类结果当作一级数据产物，**再派生 8 张基于聚类结果的主题宽表**，每张表使用 Spark SQL 的高级构造（CTE、窗口函数、多表 JOIN、多条件 CASE WHEN），物化为独立 Parquet 文件，最终聚合到 JSON 供看板读取。

### 数据仓库分层

```
ODS 层  data/medical_beauty_data.csv
  ↓ preprocess.py   pandas 去重 / 缺失值 / IQR 异常值 / 日期修复 / 派生 stats_month
DWD 层  output/clean_data.parquet
  ↓ run_clustering  Spark MLlib: VectorAssembler + StandardScaler + KMeans(K=3)
DWD+   output/clustered_data.parquet  +  output/cluster_centers.json
  ↓ run_cluster_warehouse  Spark SQL 派生 8 张主题宽表
DWS 层  output/cluster_tables/*.parquet
  ↓ 聚合物化
ADS 层  output/trend_stats.json  +  output/cluster_insights.json
  ↓ Flask
可视化  /api/analytics · /api/cluster · /api/cluster_insights
```

### 8 张聚类派生主题表

| 表名 | 主题 | 使用的 Spark SQL 技术点 |
|------|------|------------------------|
| `cluster_profile` | 聚类画像 + 业务标签 | 两级 CTE；4 次 `RANK() OVER` 算价格/销量排名；多条件 `CASE WHEN` 自动打 **高端精品型 / 大众爆款型 / 长尾引流型 / 中端主力型** 业务标签 + 按均折扣分 **深折促销 / 中度折扣 / 高折扣/原价** 三档 |
| `cluster_city_rank` | 聚类×城市销量分布 | `ROW_NUMBER() OVER (PARTITION BY cluster ORDER BY total_sales DESC)` 簇内城市排名；`RANK() OVER (PARTITION BY city)` 同城市各簇排名 |
| `cluster_category_preference` | 聚类品类偏好度 Lift | **4 级 CTE（cluster_cat / cluster_total / global_cat / global_total）+ 3 个 INNER JOIN + CROSS JOIN**；计算 Lift = 聚类内品类占比 ÷ 全局品类占比，>1 表示偏好该品类 |
| `cluster_channel_efficiency` | 聚类×渠道效率 | 加权销量 `SUM(monthly_sales × rating)`；双向 `RANK()`（簇内渠道排名 + 渠道内簇排名） |
| `cluster_price_band` | 聚类×折扣分桶 | 多条件 `CASE WHEN` 分 5 档折扣；`PERCENT_RANK() OVER (PARTITION BY cluster)` 计算簇内销量百分位 |
| `cluster_top_projects` | 聚类内 TOP 5 项目 | `ROW_NUMBER() OVER` + `WHERE rn <= 5` 取每聚类销量前 5 的热销项目 |
| `cluster_doctor_matching` | 聚类×医生头衔匹配 | `COUNT(DISTINCT doctor_name)` 统计不重复医生数；`RANK()` 簇内头衔销量排名 |
| `cluster_month_delta` | 聚类月度销量环比 | 两级 CTE；`LAG(total_sales) OVER (PARTITION BY cluster ORDER BY stats_month)` 取上月销量；`COALESCE` 处理首月空值；算销量增量与增长率 |

### 后端改动

- **`src/analysis.py`**：
  - 新增函数 `run_cluster_warehouse(clustered_parquet, output_dir)`
  - 新增辅助：`cluster_warehouse_dir()`、`_cast_numeric_cols()`、`_write_spark_parquet()`（Windows 无 winutils 时 pandas 回退）、`_collect_as_dicts()`
  - `run()` 默认追加 `build_warehouse=True` 自动调用
- **`run_all.py`**：流程从 3 步改为 4 步，新增"4. 聚类数据仓库派生"显式输出

### Web 接口

- **`src/visualize.py`**：新增 `get_cluster_insights_json()` 读 `cluster_insights.json`；新增路由 `/api/cluster_insights` 返回 8 张表 + 聚类中心 + 元信息（需登录）

### 前端看板

- **`src/templates/dashboard.html`**：侧边栏新增"**聚类深度分析**"导航项；新 `<section id="panel-cluster_deep">` 包含：
  - 聚类画像卡片区（每聚类 1 张卡，显示业务标签 + 核心指标）
  - 6 张 ECharts 图表：聚类×城市（堆叠柱）、聚类品类偏好度（热力图，Lift 着色）、聚类×折扣分桶（堆叠柱）、聚类月度销量（折线+tooltip 显环比）、聚类×渠道加权销量、聚类×医生头衔（堆叠柱）
  - 4 张数据表：聚类画像明细、聚类 TOP 5 项目、聚类×医生、聚类×折扣分桶
  - 5 个导出 CSV 按钮
- **`src/static/js/dashboard_app.js`**：
  - 新增模块变量 `clusterInsights` 和 `CLUSTER_LABEL_CLASS`、`CLUSTER_COLORS`、`clusterColor(cid)`
  - 新增 `renderClusterDeep()` 总入口，调用 11 个子渲染函数
  - `loadData()` 的 `Promise.all` 新增 `/api/cluster_insights` 请求
  - `bindToolbars()` 绑定新增的 6 个按钮
- **`src/static/css/dashboard_app.css`**：新增 `.cluster-cards` 网格布局、`.cluster-card` 渐变卡片、`.cc-tag.tag-premium/popular/longtail/mid` 业务标签配色、`.sub-title`、`.deep-meta`

### 文档

- **`doc/项目说明.md`**：在"3. 数据分析模块"下新增 3.1 数据仓库分层设计、3.2 聚类派生主题表（含 8 张表对照表）、3.3 分析技术栈总结

---

## 三、数据集国际化：UTF-8 + 英文列名

**背景**：原 CSV 文件是 **GBK 编码**的 `data/中国医美消费数据.csv`，列名也是中文（`平台`、`城市`、`项目品类` 等）。在 HDFS、Docker、Spark 等 UTF-8 环境下，文件名和列名都容易乱码；Spark SQL 里也必须用 ` ` ` 反引号包裹每个中文列名，写起来又丑又易错。

**改动**：统一把所有**标识符**（文件名、列名、SQL 别名、JSON key）英文化；**数据值**（`北京`、`新氧`、`面部护理`）保持中文并用 UTF-8 存储；**界面显示文字**（表头、标签、菜单项）也保持中文用户体验。

### 数据文件

- **新增** `data/medical_beauty_data.csv`（UTF-8，英文列头，37.9MB，150000 行）
- **保留** `data/中国医美消费数据.csv`（GBK 原文件，不再引用，后续可删除）
- 转换脚本逻辑（已执行完）：用 Python stdlib `csv` 模块读取 GBK 源文件 → 替换表头为英文 → 以 UTF-8 写入新文件；`preprocess.py` 按列位置取前 28 列，不依赖列头精确匹配，所以转换后流程完全兼容

### 列名对照表（28 列 + 1 派生）

| # | 中文（旧） | 英文（新） |
|---|---|---|
| 1 | 平台 | `platform` |
| 2 | 城市 | `city` |
| 3 | 商圈/区县 | `district` |
| 4 | 门店类型 | `store_type` |
| 5 | 机构名称 | `institution` |
| 6 | 项目品类 | `category` |
| 7 | 项目子类 | `subcategory` |
| 8 | 规格 | `spec` |
| 9 | 项目名称 | `project_name` |
| 10 | 标价(元) | `list_price` |
| 11 | 到手价(元) | `actual_price` |
| 12 | 折扣率 | `discount_rate` |
| 13 | 月销量 | `monthly_sales` |
| 14 | 评价数 | `review_count` |
| 15 | 评分 | `rating` |
| 16 | 医生姓名 | `doctor_name` |
| 17 | 医生头衔 | `doctor_title` |
| 18 | 支付方式 | `payment_method` |
| 19 | 是否分期 | `installment` |
| 20 | 渠道 | `channel` |
| 21 | 活动标签 | `activity_tag` |
| 22 | 批准文号 | `approval_no` |
| 23 | 器械品牌 | `device_brand` |
| 24 | 型号/机型 | `device_model` |
| 25 | UDI匹配 | `udi_match` |
| 26 | 上架日期 | `listing_date` |
| 27 | 采集日期 | `collection_date` |
| 28 | 新客标识 | `new_customer` |
| (派生) | 统计月份 | `stats_month` |

### 代码改动

- **`src/config.py`**：
  - `RAW_CSV` 指向 `data/medical_beauty_data.csv`
  - `raw_csv_input()` 的 HDFS 默认路径也改为英文
  - 所有 `COL_*` 常量改英文字符串
  - `RAW_HEADERS`、`CLEAN_HEADERS`、`NUMERIC_COLS`、`DATE_COLS` 同步更新
- **`src/analysis.py`**：14 处 Spark SQL 别名从 ``AS `城市` `` / ``AS `项目品类` `` 等改为 `AS city` / `AS category` 等（SQL 更简洁，去掉了 backtick 包裹中文的写法）
- **`src/static/js/dashboard_app.js`**：12 处数据访问从 `d["城市"]` / `d["项目品类"]` 等改为 `d["city"]` / `d["category"]` 等；**对象字面量里的中文 key 保留**（用来拼显示行，跟 `fillTable` 的中文表头对应）
- **`src/templates/dashboard.html`**：表头文字保持中文（用户看的界面不变）

### 产物

- **已重新生成** `output/clean_data.parquet`（英文列名，124726 行；dedup + IQR 异常值剔除 + 单月数据切半派生）
- **已删除** `output/clustered_data.parquet`、`output/cluster_centers.json`、`output/trend_stats.json`、`output/cluster_insights.json`、`output/cluster_tables/`（这些需要在 Spark 机器上重跑 `python run_all.py` 重新生成）

### 文档

- **`README.md`**：
  - CSV 路径更新为 `medical_beauty_data.csv`
  - "放置数据"章节补一份中英对照表
  - HDFS Docker 命令中的文件名同步
- **`doc/项目说明.md`**：ODS 层标注为"UTF-8，英文列名"

---

## 四、验证清单

本次改动后在 Mac 上做的本地验证：

| 检查 | 结果 |
|------|------|
| 所有 Python 文件 `ast.parse` | ✓ 通过（config / preprocess / storage / analysis / visualize / dashboard_auth / hdfs_io / run_all） |
| `node -c dashboard_app.js` | ✓ 通过 |
| grep 残留中文列名标识符 | ✓ 无残留 |
| 跑 `python -m src.preprocess` | ✓ 成功：150000 → 124726 行，UTF-8 → Parquet，列全为英文 |
| 确认 Parquet 列 | ✓ 29 列（28 + stats_month） |
| 确认 `stats_month` 分布 | ✓ `2025-08` 62363 / `2025-09` 62363 |

---

## 五、接下来的 TODO（需要在有 Spark 的机器上完成）

1. 同步仓库到装了 **Java 8/11 + Spark 3.3+** 的机器
2. `pip install -r requirements.txt`
3. `python run_all.py` —— 完整跑一遍 4 步流程，生成 `clustered_data.parquet`、`cluster_centers.json`、`trend_stats.json`、`cluster_insights.json`、`cluster_tables/*.parquet`
4. `python -m src.visualize` 启动看板，访问 `http://127.0.0.1:5000/`，确认"聚类深度分析"页的画像卡片、6 张图和 4 张表都正常显示
5. （可选）删除遗留文件：`data/中国医美消费数据.csv`（旧 GBK）、`database/medical_beauty.db`（过期 SQLite）、`.venv/` 和 `venv/`（Windows 专用虚拟环境）
