/**
 * 医美数据分析看板：JSON 快照聚合 + 月份筛选、多维度图表与导出
 */
 // ==========================================
// 终极修复：选择8月/9月/全部，图表立刻刷新
// ==========================================
document.addEventListener('DOMContentLoaded', function() {
    const sel = document.getElementById('selectStatsMonth');
    sel.addEventListener('change', function() {
        console.log("✅ 月份切换为：", this.value);

        if (window.refreshAllPanels) {
            window.refreshAllPanels();
        } else if (window.renderOverview) {
            window.renderOverview();
            window.renderCity();
            window.renderTrend();
            window.renderCategory();
            window.renderChannel();
            window.renderCluster();
        }
    });
});

(function () {
  var trendData = null;
  var clusterInsights = null;
  var charts = {};
  var cityTopN = 15;
  var selectedMonth = "all";

  // 聚类业务标签 → CSS tag 类名
  var CLUSTER_LABEL_CLASS = {
    "高端精品型": "tag-premium",
    "大众爆款型": "tag-popular",
    "长尾引流型": "tag-longtail",
    "中端主力型": "tag-mid"
  };
  // 聚类渲染配色（按簇 id 取模）
  var CLUSTER_COLORS = ["#38bdf8", "#f97316", "#a78bfa", "#34d399", "#f472b6", "#fb923c", "#22d3ee", "#facc15"];
  function clusterColor(cid) {
    var n = (cid == null ? 0 : Number(cid)) | 0;
    return CLUSTER_COLORS[((n % CLUSTER_COLORS.length) + CLUSTER_COLORS.length) % CLUSTER_COLORS.length];
  }

  function toast(msg) {
    var el = document.getElementById("msgToast");
    if (!el) return;
    el.textContent = msg;
    el.classList.add("show");
    setTimeout(function () { el.classList.remove("show"); }, 2200);
  }

  function downloadCsv(filename, rows) {
    if (!rows || !rows.length) {
      toast("无数据可导出");
      return;
    }
    var headers = Object.keys(rows[0]);
    var lines = [headers.join(",")];
    rows.forEach(function (row) {
      lines.push(headers.map(function (h) {
        var v = row[h];
        if (v == null) v = "";
        v = String(v).replace(/"/g, '""');
        if (/[",\n]/.test(v)) v = '"' + v + '"';
        return v;
      }).join(","));
    });
    var blob = new Blob(["\ufeff" + lines.join("\n")], { type: "text/csv;charset=utf-8" });
    var a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    a.click();
    URL.revokeObjectURL(a.href);
    toast("已导出 " + filename);
  }

  function fmtNum(n) {
    if (n == null || isNaN(n)) return "—";
    return Number(n).toLocaleString("zh-CN", { maximumFractionDigits: 2 });
  }

  function fillTable(tbodyId, headers, rows) {
    var tb = document.getElementById(tbodyId);
    if (!tb) return;
    tb.innerHTML = rows.map(function (r) {
      return "<tr>" + headers.map(function (h) {
        return "<td>" + (r[h] != null ? r[h] : "") + "</td>";
      }).join("") + "</tr>";
    }).join("");
  }

  function syncMonthSelect() {
    var sel = document.getElementById("selectStatsMonth");
    if (!sel || !trendData) return;
    var months = trendData.available_months || [];
    var keep = selectedMonth;
    sel.innerHTML = '<option value="all">全部（所有采集批次）</option>';
    months.forEach(function (m) {
      var v = m.stats_month != null ? String(m.stats_month) : "";
      if (!v || v === "未知") return;
      var opt = document.createElement("option");
      opt.value = v;
      opt.textContent = v + "（" + (m.row_cnt != null ? m.row_cnt : "") + " 条）";
      sel.appendChild(opt);
    });
    if (keep && (keep === "all" || Array.prototype.some.call(sel.options, function (o) { return o.value === keep; }))) {
      sel.value = keep;
    } else {
      sel.value = "all";
      selectedMonth = "all";
    }
  }

  function getCityRows() {
    var list = (trendData && trendData.by_city) || [];
    return list.map(function (d) {
      return {
        城市: d["city"] || "",
        月销量合计: d.total_sales,
        项目数: d.project_count,
        平均评分: d.avg_rating != null ? Number(d.avg_rating).toFixed(3) : "—",
        平均到手价: d.avg_actual_price != null ? Number(d.avg_actual_price).toFixed(2) : "—",
        平均折扣率: d.avg_discount != null ? Number(d.avg_discount).toFixed(4) : "—"
      };
    });
  }

  function getCategoryRows() {
    return ((trendData && trendData.by_category) || []).map(function (d) {
      return {
        项目品类: d["category"] || "",
        月销量合计: d.total_sales,
        项目数: d.project_count,
        平均到手价: d.avg_actual_price != null ? Number(d.avg_actual_price).toFixed(2) : "—",
        平均折扣: d.avg_discount != null ? Number(d.avg_discount).toFixed(4) : "—",
        平均评分: d.avg_rating != null ? Number(d.avg_rating).toFixed(3) : "—"
      };
    });
  }

  function getChannelRows() {
    return ((trendData && trendData.by_channel) || []).map(function (d) {
      return {
        渠道: d["channel"] || "",
        销量合计: d.total_sales,
        记录数: d.count,
        平均评分: d.avg_rating != null ? Number(d.avg_rating).toFixed(3) : "—"
      };
    });
  }

  function renderOverview() {
    var s = (trendData && trendData.summary) || {};
    var byCity = (trendData && trendData.by_city) || [];
    var byCat = (trendData && trendData.by_category) || [];
    var byCh = (trendData && trendData.by_channel) || [];

    var elRows = document.getElementById("statRows");
    if (elRows) elRows.textContent = s.row_cnt != null ? fmtNum(s.row_cnt) : "—";
    document.getElementById("statSales").textContent = fmtNum(s.total_sales);
    var elA = document.getElementById("statAvgActual");
    if (elA) elA.textContent = s.avg_actual_price != null ? fmtNum(s.avg_actual_price) : "—";
    var elD = document.getElementById("statAvgDisc");
    if (elD) elD.textContent = s.avg_discount != null ? fmtNum(s.avg_discount) : "—";
    var elR = document.getElementById("statAvgRating");
    if (elR) elR.textContent = s.avg_rating != null ? Number(s.avg_rating).toFixed(3) : "—";
    document.getElementById("statCities").textContent = byCity.length;
    document.getElementById("statCategories").textContent = byCat.length;
    document.getElementById("statChannels").textContent = byCh.length;

    if (!charts.overviewCity) {
      charts.overviewCity = echarts.init(document.getElementById("chartOverviewCity"));
    }
    var top = byCity.slice(0, 8);
    charts.overviewCity.setOption({
      tooltip: { trigger: "axis" },
      grid: { left: 12, right: 12, top: 28, bottom: 4, containLabel: true },
      xAxis: { type: "category", data: top.map(function (d) { return d["city"] || ""; }), axisLabel: { rotate: 30 } },
      yAxis: {
  type: "value",
  name: "月销量合计",
  min: 700000,
  max: 780000
},
      series: [{ type: "bar", data: top.map(function (d) { return d.total_sales || 0; }), itemStyle: { color: "#38bdf8" } }]
    });

    if (!charts.overviewCat) {
      charts.overviewCat = echarts.init(document.getElementById("chartOverviewCat"));
    }
    charts.overviewCat.setOption({
      tooltip: { trigger: "item" },
      series: [{
        type: "pie",
        radius: ["40%", "65%"],
        data: byCat.slice(0, 10).map(function (d) {
          return { name: d["category"] || "", value: d.total_sales || 0 };
        })
      }]
    });
  }

  function renderCity() {
    var full = getCityRows();
    var slice = full.slice(0, cityTopN);
    if (!charts.city) charts.city = echarts.init(document.getElementById("chartCity"));
    charts.city.setOption({
      tooltip: { trigger: "axis" },
      grid: { left: 12, right: 24, top: 40, bottom: 48, containLabel: true },
      xAxis: { type: "category", data: slice.map(function (r) { return r.城市; }) },
      yAxis: {
  type: "value",
  name: "月销量合计",
  min: 700000,
  max: 780000
},
      series: [{ type: "bar", data: slice.map(function (r) { return r.月销量合计; }), itemStyle: { color: "#38bdf8" } }]
    });
    fillTable("tbodyCity", ["城市", "月销量合计", "项目数", "平均评分", "平均到手价", "平均折扣率"], slice);
  }

  function renderCategory() {
    var rows = getCategoryRows();
    if (!charts.category) charts.category = echarts.init(document.getElementById("chartCategory"));
    charts.category.setOption({
      tooltip: { trigger: "item" },
      series: [{
        type: "pie",
        radius: "62%",
        data: rows.map(function (r) { return { name: r.项目品类, value: r.月销量合计 }; })
      }]
    });
    fillTable("tbodyCategory", ["项目品类", "月销量合计", "项目数", "平均到手价", "平均折扣", "平均评分"], rows);
  }

  function renderChannel() {
    var rows = getChannelRows();
    if (!charts.channel) charts.channel = echarts.init(document.getElementById("chartChannel"));
    charts.channel.setOption({
      tooltip: { trigger: "axis" },
      grid: { left: 12, right: 24, top: 40, bottom: 48, containLabel: true },
      xAxis: { type: "category", data: rows.map(function (r) { return r.渠道; }) },
      yAxis: { type: "value", name: "销量合计" },
      series: [{ type: "bar", data: rows.map(function (r) { return r.销量合计; }), itemStyle: { color: "#22d3ee" } }]
    });
    fillTable("tbodyChannel", ["渠道", "销量合计", "记录数", "平均评分"], rows);
  }

  function renderTrend() {
    var months = (trendData && trendData.sales_trend_by_month) || [];
    var projects = (trendData && trendData.top_projects) || [];

    if (!charts.trendMonth) charts.trendMonth = echarts.init(document.getElementById("chartTrendMonth"));
    charts.trendMonth.setOption({
      tooltip: { trigger: "axis" },
      legend: { data: ["月销量合计", "项目条数"], bottom: 0 },
      grid: { left: 12, right: 48, top: 40, bottom: 56, containLabel: true },
      xAxis: { type: "category", data: months.map(function (m) { return m.stats_month || ""; }) },
yAxis: {
    type: "value",
    name: "销量",
    min: 7460000,   // 你可以自己调
    max: 7490000,   // 你可以自己调
},

series: [
    {
        name: "月销量合计",
        type: "line",
        smooth: true,
        data: months.map(function (m) { return m.total_sales || 0; }),
        itemStyle: { color: "#38bdf8" }
    }
]
    });

    var pt = projects.slice(0, 12);
    if (!charts.topProj) charts.topProj = echarts.init(document.getElementById("chartTopProjects"));
    charts.topProj.setOption({
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
      grid: { left: 12, right: 24, top: 28, bottom: 8, containLabel: true },
      xAxis: { type: "value", name: "销量" },
      yAxis: { type: "category", data: pt.map(function (p) { return (p["project_name"] || "").slice(0, 18); }).reverse(), axisLabel: { fontSize: 11 } },
      series: [{ type: "bar", data: pt.map(function (p) { return p.total_sales || 0; }).reverse(), itemStyle: { color: "#a78bfa" } }]
    });

    fillTable("tbodyTrendMonth", ["统计月份", "月销量合计", "项目条数", "平均评分", "平均到手价"], months.map(function (m) {
      return {
        统计月份: m.stats_month,
        月销量合计: fmtNum(m.total_sales),
        项目条数: m.project_count,
        平均评分: m.avg_rating != null ? Number(m.avg_rating).toFixed(3) : "—",
        平均到手价: m.avg_actual_price != null ? Number(m.avg_actual_price).toFixed(2) : "—"
      };
    }));
  }

  function renderRegionPref() {
    var dist = (trendData && trendData.by_district) || [];
    var cc = (trendData && trendData.city_category_top) || [];
    var dslice = dist.slice(0, 15);

    if (!charts.district) charts.district = echarts.init(document.getElementById("chartDistrict"));
    charts.district.setOption({
      tooltip: { trigger: "axis" },
      grid: { left: 12, right: 12, top: 28, bottom:48, containLabel: true },
      xAxis: { type: "category", data: dslice.map(function (d) { return d["district"] || ""; }), axisLabel: { rotate: 35, fontSize: 10 } },
      yAxis: { type: "value", name: "销量" },
      series: [{ type: "bar", data: dslice.map(function (d) { return d.total_sales || 0; }), itemStyle: { color: "#34d399" } }]
    });

    var ctop = cc.slice(0, 12);
    if (!charts.cityCat) charts.cityCat = echarts.init(document.getElementById("chartCityCategory"));
    charts.cityCat.setOption({
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
      grid: { left: 12, right: 24, top: 28, bottom: 8, containLabel: true },
      xAxis: {type: "value",
  min: 160000,
  max: 190000},
      yAxis: {
        type: "category",
        data: ctop.map(function (r) { return (r["city"] || "") + " / " + (r["category"] || "").slice(0, 8); }).reverse(),
        axisLabel: { fontSize: 10 }
      },
      series: [{ type: "bar", data: ctop.map(function (r) { return r.total_sales || 0; }).reverse(), itemStyle: { color: "#fb923c" } }]
    });

    fillTable("tbodyDistrict", ["商圈区县", "月销量合计", "项目数", "平均到手价", "平均评分"], dist.slice(0, 30).map(function (d) {
      return {
        商圈区县: d["district"] || "",
        月销量合计: fmtNum(d.total_sales),
        项目数: d.project_count,
        平均到手价: d.avg_actual_price != null ? Number(d.avg_actual_price).toFixed(2) : "—",
        平均评分: d.avg_rating != null ? Number(d.avg_rating).toFixed(3) : "—"
      };
    }));
  }

  function renderPriceDiscount() {
    var bands = (trendData && trendData.discount_band_stats) || [];
    var doc = (trendData && trendData.doctor_title_stats) || [];
    var stores = (trendData && trendData.by_store_type) || [];

    if (!charts.discount) charts.discount = echarts.init(document.getElementById("chartDiscountBand"));
    charts.discount.setOption({
      tooltip: { trigger: "axis" },
      legend: { bottom: 0 },
      grid: { left: 12, right: 44, top: 32, bottom: 48, containLabel: true },
      xAxis: { type: "category", data: bands.map(function (b) { return b.discount_band || ""; }), axisLabel: { rotate: 20, fontSize: 10 } },
      yAxis: [
        { type: "value", name: "平均月销量" },
        { type: "value", name: "评分", max: 5, splitLine: { show: false } }
      ],
      series: [
        { name: "平均月销量", type: "bar", yAxisIndex: 0, data: bands.map(function (b) { return b.avg_monthly_sales || 0; }), itemStyle: { color: "#f472b6" } },
        { name: "平均评分", type: "line", yAxisIndex: 1, smooth: true, data: bands.map(function (b) { return b.avg_rating != null ? Number(b.avg_rating) : 0; }), itemStyle: { color: "#6366f1" } }
      ]
    });

    var dt = doc.slice(0, 12);
    if (!charts.docTitle) charts.docTitle = echarts.init(document.getElementById("chartDoctorTitle"));
    charts.docTitle.setOption({
      tooltip: { trigger: "axis" },
      grid: { left: 12, right: 12, top: 28, bottom: 48, containLabel: true },
      xAxis: { type: "category", data: dt.map(function (d) { return (d["doctor_title"] || "").slice(0, 12); }), axisLabel: { rotate: 25, fontSize: 10 } },
      yAxis: { type: "value", name: "销量" },
      series: [{ type: "bar", data: dt.map(function (d) { return d.total_sales || 0; }), itemStyle: { color: "#2dd4bf" } }]
    });

    fillTable("tbodyStoreType", ["门店类型", "月销量合计", "项目数", "平均到手价"], stores.map(function (d) {
      return {
        门店类型: d["store_type"] || "",
        月销量合计: fmtNum(d.total_sales),
        项目数: d.project_count,
        平均到手价: d.avg_actual_price != null ? Number(d.avg_actual_price).toFixed(2) : "—"
      };
    }));
  }

  function renderCluster() {
    var cc = trendData && trendData.cluster_centers;
    if (!cc || !cc.k) {
      if (charts.cluster) charts.cluster.clear();
      var th = document.getElementById("clusterThead");
      if (th) th.innerHTML = "<tr><th>—</th></tr>";
      fillTable("tbodyCluster", ["提示"], [{ 提示: "暂无聚类数据，请运行 Spark 分析生成 cluster_centers.json" }]);
      return;
    }
    var cols = cc.feature_cols || [];
    var centers = cc.centers || [];
    var tableRows = centers.map(function (vec, i) {
      var o = { 聚类编号: "簇 " + i };
      cols.forEach(function (c, j) {
        o[c] = vec[j] != null ? Number(vec[j]).toFixed(4) : "—";
      });
      return o;
    });
    var headers = ["聚类编号"].concat(cols);
    var thead = document.getElementById("clusterThead");
    var tb = document.getElementById("tbodyCluster");
    if (thead) {
      thead.innerHTML = "<tr>" + headers.map(function (h) { return "<th>" + h + "</th>"; }).join("") + "</tr>";
    }
    if (tb) {
      tb.innerHTML = tableRows.map(function (r) {
        return "<tr>" + headers.map(function (h) {
          return "<td>" + (r[h] != null ? r[h] : "") + "</td>";
        }).join("") + "</tr>";
      }).join("");
    }
    if (!charts.cluster) charts.cluster = echarts.init(document.getElementById("chartCluster"));
    var series = cols.map(function (c, j) {
      return {
        name: c,
        type: "bar",
        data: centers.map(function (vec) { return vec[j] != null ? vec[j] : 0; })
      };
    });
    charts.cluster.setOption({
      tooltip: { trigger: "axis" },
      legend: { type: "scroll", bottom: 0 },
      grid: { left: 12, right: 12, top: 48, bottom: 80, containLabel: true },
      xAxis: {
        type: "category",
        data: centers.map(function (_, i) { return "簇 " + i; })
      },
      yAxis: { type: "value", name: "中心值(标准化)" },
      series: series
    });
  }

  // ===========================================================
  // 聚类深度分析页（数据仓库 DWS 层，读 /api/cluster_insights）
  // ===========================================================
  function renderClusterDeep() {
    var ins = clusterInsights || {};
    var profile = ins.cluster_profile || [];
    var cityRank = ins.cluster_city_rank || [];
    var catPref = ins.cluster_category_preference || [];
    var band = ins.cluster_price_band || [];
    var monthDelta = ins.cluster_month_delta || [];
    var channelEff = ins.cluster_channel_efficiency || [];
    var docMatch = ins.cluster_doctor_matching || [];
    var topProj = ins.cluster_top_projects || [];
    var meta = ins._metadata || {};

    renderClusterCards(profile);
    renderClusterProfileTable(profile);
    renderClusterTopProjTable(topProj);
    renderClusterDoctorTable(docMatch);
    renderClusterBandTable(band);

    renderDeepCityChart(cityRank);
    renderDeepCatPrefChart(catPref);
    renderDeepBandChart(band);
    renderDeepMonthChart(monthDelta);
    renderDeepChannelChart(channelEff);
    renderDeepDoctorChart(docMatch);

    var metaEl = document.getElementById("deepMeta");
    if (metaEl) {
      if (meta.generated_at) {
        metaEl.innerHTML =
          "数据更新时间：" + meta.generated_at + "　|　" +
          "参与计算行数：" + fmtNum(meta.total_rows || 0) + "　|　" +
          "聚类数 K = " + (meta.k || "—") + "　|　" +
          "主题表：" + (meta.tables || []).join("、");
      } else {
        metaEl.textContent = "尚未生成聚类主题表，请先执行 run_cluster_warehouse()（或 python run_all.py）。";
      }
    }
  }

  function renderClusterCards(profile) {
    var el = document.getElementById("clusterCards");
    if (!el) return;
    if (!profile.length) {
      el.innerHTML = '<div style="color:#64748b;font-size:13px;">暂无聚类画像数据</div>';
      return;
    }
    el.innerHTML = profile.map(function (r) {
      var label = r.cluster_label || "—";
      var cls = CLUSTER_LABEL_CLASS[label] || "tag-mid";
      var lines = [
        ["项目数", fmtNum(r.project_count)],
        ["销量合计", fmtNum(r.total_sales)],
        ["均标价", r.avg_list_price != null ? "¥" + Number(r.avg_list_price).toFixed(0) : "—"],
        ["均到手价", r.avg_actual_price != null ? "¥" + Number(r.avg_actual_price).toFixed(0) : "—"],
        ["均折扣率", r.avg_discount != null ? Number(r.avg_discount).toFixed(3) : "—"],
        ["均月销量", r.avg_monthly_sales != null ? Number(r.avg_monthly_sales).toFixed(1) : "—"],
        ["均评价数", r.avg_review_count != null ? Number(r.avg_review_count).toFixed(1) : "—"],
        ["均评分", r.avg_rating != null ? Number(r.avg_rating).toFixed(3) : "—"]
      ];
      return (
        '<div class="cluster-card">' +
          '<div class="cc-head">' +
            '<span class="cc-id">簇 ' + r.cluster + '</span>' +
            '<span class="cc-tag ' + cls + '">' + label + '</span>' +
          '</div>' +
          '<div class="cc-label">' + label + '</div>' +
          '<div class="cc-sub">折扣等级：' + (r.discount_level || "—") +
            '　|　价格排名 ' + (r.price_rank || "—") + '　销量排名 ' + (r.sales_rank || "—") + '</div>' +
          '<div class="cc-metrics">' +
            lines.map(function (p) {
              return '<span class="m-label">' + p[0] + '</span><span class="m-value">' + p[1] + '</span>';
            }).join("") +
          '</div>' +
        '</div>'
      );
    }).join("");
  }

  function renderClusterProfileTable(profile) {
    var rows = profile.map(function (r) {
      return {
        簇: "簇 " + r.cluster,
        业务标签: r.cluster_label || "—",
        折扣等级: r.discount_level || "—",
        项目数: fmtNum(r.project_count),
        销量合计: fmtNum(r.total_sales),
        均标价: r.avg_list_price != null ? Number(r.avg_list_price).toFixed(2) : "—",
        均到手价: r.avg_actual_price != null ? Number(r.avg_actual_price).toFixed(2) : "—",
        均折扣: r.avg_discount != null ? Number(r.avg_discount).toFixed(4) : "—",
        均月销量: r.avg_monthly_sales != null ? Number(r.avg_monthly_sales).toFixed(2) : "—",
        均评价数: r.avg_review_count != null ? Number(r.avg_review_count).toFixed(1) : "—",
        均评分: r.avg_rating != null ? Number(r.avg_rating).toFixed(3) : "—",
        价格排名: r.price_rank != null ? r.price_rank : "—",
        销量排名: r.sales_rank != null ? r.sales_rank : "—"
      };
    });
    fillTable("tbodyClusterProfile",
      ["簇", "业务标签", "折扣等级", "项目数", "销量合计",
       "均标价", "均到手价", "均折扣", "均月销量", "均评价数", "均评分",
       "价格排名", "销量排名"], rows);
  }

  function renderClusterTopProjTable(topProj) {
    var rows = topProj.map(function (r) {
      return {
        簇: "簇 " + r.cluster,
        排名: r.rn,
        项目名称: r.project_name || "—",
        销量合计: fmtNum(r.total_sales),
        均到手价: r.avg_actual_price != null ? Number(r.avg_actual_price).toFixed(2) : "—",
        均评分: r.avg_rating != null ? Number(r.avg_rating).toFixed(3) : "—",
        上架条数: r.listing_count
      };
    });
    fillTable("tbodyClusterTopProj",
      ["簇", "排名", "项目名称", "销量合计", "均到手价", "均评分", "上架条数"], rows);
  }

  function renderClusterDoctorTable(docMatch) {
    var rows = docMatch.map(function (r) {
      return {
        簇: "簇 " + r.cluster,
        医生头衔: r.doctor_title || "—",
        簇内排名: r.title_rank_in_cluster != null ? r.title_rank_in_cluster : "—",
        项目数: fmtNum(r.project_count),
        医生数: fmtNum(r.doctor_count),
        销量合计: fmtNum(r.total_sales),
        均评分: r.avg_rating != null ? Number(r.avg_rating).toFixed(3) : "—",
        均到手价: r.avg_actual_price != null ? Number(r.avg_actual_price).toFixed(2) : "—"
      };
    });
    fillTable("tbodyClusterDoctor",
      ["簇", "医生头衔", "簇内排名", "项目数", "医生数", "销量合计", "均评分", "均到手价"], rows);
  }

  function renderClusterBandTable(band) {
    var rows = band.map(function (r) {
      return {
        簇: "簇 " + r.cluster,
        折扣分桶: r.discount_band || "—",
        项目数: fmtNum(r.project_count),
        销量合计: fmtNum(r.total_sales),
        均销量: r.avg_sales != null ? Number(r.avg_sales).toFixed(2) : "—",
        均评分: r.avg_rating != null ? Number(r.avg_rating).toFixed(3) : "—",
        均到手价: r.avg_price != null ? Number(r.avg_price).toFixed(2) : "—",
        簇内销量百分位: r.sales_pct_in_cluster != null ? (Number(r.sales_pct_in_cluster) * 100).toFixed(1) + "%" : "—"
      };
    });
    fillTable("tbodyClusterBand",
      ["簇", "折扣分桶", "项目数", "销量合计", "均销量", "均评分", "均到手价", "簇内销量百分位"], rows);
  }

  function distinctClusters(rows) {
    var s = {};
    rows.forEach(function (r) { if (r.cluster != null) s[r.cluster] = true; });
    return Object.keys(s).map(Number).sort(function (a, b) { return a - b; });
  }

  function renderDeepCityChart(cityRank) {
    var el = document.getElementById("chartDeepCity");
    if (!el) return;
    if (!charts.deepCity) charts.deepCity = echarts.init(el);
    if (!cityRank.length) { charts.deepCity.clear(); return; }

    var clusters = distinctClusters(cityRank);
    var citySum = {};
    cityRank.forEach(function (r) {
      if (r.city) citySum[r.city] = (citySum[r.city] || 0) + (Number(r.total_sales) || 0);
    });
    var topCities = Object.keys(citySum).sort(function (a, b) { return citySum[b] - citySum[a]; }).slice(0, 10);

    var series = clusters.map(function (cl) {
      return {
        name: "簇 " + cl,
        type: "bar",
        stack: "cluster",
        itemStyle: { color: clusterColor(cl) },
        data: topCities.map(function (c) {
          var row = cityRank.find(function (r) { return Number(r.cluster) === cl && r.city === c; });
          return row ? (Number(row.total_sales) || 0) : 0;
        })
      };
    });
    charts.deepCity.setOption({
      title: { text: "聚类×城市销量分布（TOP 10 城市）", left: "center", textStyle: { fontSize: 13, color: "#0f172a" } },
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
      legend: { bottom: 0 },
      grid: { left: 12, right: 12, top: 40, bottom: 48, containLabel: true },
      xAxis: { type: "category", data: topCities, axisLabel: { rotate: 25, fontSize: 11 } },
      yAxis: { type: "value", name: "销量" },
      series: series
    });
  }

  function renderDeepCatPrefChart(catPref) {
    var el = document.getElementById("chartDeepCatPref");
    if (!el) return;
    if (!charts.deepCatPref) charts.deepCatPref = echarts.init(el);
    if (!catPref.length) { charts.deepCatPref.clear(); return; }

    var clusters = distinctClusters(catPref);
    var topCats = {};
    clusters.forEach(function (cl) {
      catPref.filter(function (r) { return Number(r.cluster) === cl; })
        .sort(function (a, b) { return (Number(b.preference_lift) || 0) - (Number(a.preference_lift) || 0); })
        .slice(0, 4)
        .forEach(function (r) { if (r.category) topCats[r.category] = true; });
    });
    var cats = Object.keys(topCats);
    var data = [];
    var maxLift = 0;
    cats.forEach(function (cat, yi) {
      clusters.forEach(function (cl, xi) {
        var row = catPref.find(function (r) { return Number(r.cluster) === cl && r.category === cat; });
        var lift = row && row.preference_lift != null ? Number(row.preference_lift) : null;
        if (lift != null && lift > maxLift) maxLift = lift;
        data.push([xi, yi, lift != null ? Number(lift.toFixed(2)) : null]);
      });
    });

    charts.deepCatPref.setOption({
      title: { text: "聚类×品类偏好度 Lift（>1=偏好，<1=回避）", left: "center", textStyle: { fontSize: 13, color: "#0f172a" } },
      tooltip: {
        position: "top",
        formatter: function (p) {
          var v = p.data[2];
          return "簇 " + clusters[p.data[0]] + " × " + cats[p.data[1]] +
            "<br/>Lift = " + (v != null ? v : "—");
        }
      },
      grid: { left: 100, right: 12, top: 40, bottom: 60, containLabel: true },
      xAxis: { type: "category", data: clusters.map(function (c) { return "簇 " + c; }) },
      yAxis: { type: "category", data: cats, axisLabel: { fontSize: 11 } },
      visualMap: {
        min: 0, max: Math.max(2, Math.ceil(maxLift)), calculable: true,
        orient: "horizontal", left: "center", bottom: 0,
        inRange: { color: ["#f0f9ff", "#7dd3fc", "#0ea5e9", "#0c4a6e"] }
      },
      series: [{
        type: "heatmap",
        data: data,
        label: { show: true, formatter: function (p) { return p.data[2] != null ? p.data[2] : "—"; }, fontSize: 11 }
      }]
    });
  }

  function renderDeepBandChart(band) {
    var el = document.getElementById("chartDeepBand");
    if (!el) return;
    if (!charts.deepBand) charts.deepBand = echarts.init(el);
    if (!band.length) { charts.deepBand.clear(); return; }

    var clusters = distinctClusters(band);
    var bandOrder = ["深折(<0.6)", "中折(0.6-0.75)", "浅折(0.75-0.9)", "原价(>=0.9)", "未知"];
    var presentBands = {};
    band.forEach(function (r) { if (r.discount_band) presentBands[r.discount_band] = true; });
    var bands = bandOrder.filter(function (b) { return presentBands[b]; })
      .concat(Object.keys(presentBands).filter(function (b) { return bandOrder.indexOf(b) < 0; }));

    var series = clusters.map(function (cl) {
      return {
        name: "簇 " + cl,
        type: "bar",
        stack: "band",
        itemStyle: { color: clusterColor(cl) },
        data: bands.map(function (b) {
          var row = band.find(function (r) { return Number(r.cluster) === cl && r.discount_band === b; });
          return row ? (Number(row.total_sales) || 0) : 0;
        })
      };
    });
    charts.deepBand.setOption({
      title: { text: "聚类×折扣分桶销量（堆叠）", left: "center", textStyle: { fontSize: 13, color: "#0f172a" } },
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
      legend: { bottom: 0 },
      grid: { left: 12, right: 12, top: 40, bottom: 48, containLabel: true },
      xAxis: { type: "category", data: bands, axisLabel: { fontSize: 11 } },
      yAxis: { type: "value", name: "销量" },
      series: series
    });
  }

  function renderDeepMonthChart(monthDelta) {
    var el = document.getElementById("chartDeepMonth");
    if (!el) return;
    if (!charts.deepMonth) charts.deepMonth = echarts.init(el);
    if (!monthDelta.length) { charts.deepMonth.clear(); return; }

    var clusters = distinctClusters(monthDelta);
    var monthsSet = {};
    monthDelta.forEach(function (r) { if (r.stats_month) monthsSet[r.stats_month] = true; });
    var months = Object.keys(monthsSet).sort();

    var series = clusters.map(function (cl) {
      return {
        name: "簇 " + cl,
        type: "line",
        smooth: true,
        itemStyle: { color: clusterColor(cl) },
        lineStyle: { color: clusterColor(cl), width: 2 },
        data: months.map(function (m) {
          var row = monthDelta.find(function (r) { return Number(r.cluster) === cl && r.stats_month === m; });
          return row ? (Number(row.total_sales) || 0) : 0;
        })
      };
    });
    charts.deepMonth.setOption({
      title: { text: "聚类月度销量（LAG 窗口函数）", left: "center", textStyle: { fontSize: 13, color: "#0f172a" } },
      tooltip: {
        trigger: "axis",
        formatter: function (params) {
          var lines = [params[0].axisValue];
          params.forEach(function (p) {
            var row = monthDelta.find(function (r) {
              return r.stats_month === p.axisValue && ("簇 " + r.cluster) === p.seriesName;
            });
            var gr = row && row.sales_growth_rate != null
              ? "（环比 " + (Number(row.sales_growth_rate) * 100).toFixed(1) + "%）" : "";
            lines.push(p.marker + p.seriesName + " : " + fmtNum(p.value) + gr);
          });
          return lines.join("<br/>");
        }
      },
      legend: { bottom: 0 },
      grid: { left: 12, right: 24, top: 40, bottom: 48, containLabel: true },
      xAxis: { type: "category", data: months },
      yAxis: { type: "value", name: "销量" },
      series: series
    });
  }

  function renderDeepChannelChart(channelEff) {
    var el = document.getElementById("chartDeepChannel");
    if (!el) return;
    if (!charts.deepChannel) charts.deepChannel = echarts.init(el);
    if (!channelEff.length) { charts.deepChannel.clear(); return; }

    var clusters = distinctClusters(channelEff);
    var channelsSet = {};
    channelEff.forEach(function (r) { if (r.channel) channelsSet[r.channel] = true; });
    var channels = Object.keys(channelsSet);

    var series = clusters.map(function (cl) {
      return {
        name: "簇 " + cl,
        type: "bar",
        itemStyle: { color: clusterColor(cl) },
        data: channels.map(function (ch) {
          var row = channelEff.find(function (r) { return Number(r.cluster) === cl && r.channel === ch; });
          return row ? (Number(row.weighted_sales) || 0) : 0;
        })
      };
    });
    charts.deepChannel.setOption({
      title: { text: "聚类×渠道加权销量（Σ 销量×评分）", left: "center", textStyle: { fontSize: 13, color: "#0f172a" } },
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
      legend: { bottom: 0 },
      grid: { left: 12, right: 12, top: 40, bottom: 48, containLabel: true },
      xAxis: { type: "category", data: channels, axisLabel: { fontSize: 11 } },
      yAxis: { type: "value", name: "加权销量" },
      series: series
    });
  }

  function renderDeepDoctorChart(docMatch) {
    var el = document.getElementById("chartDeepDoctor");
    if (!el) return;
    if (!charts.deepDoctor) charts.deepDoctor = echarts.init(el);
    if (!docMatch.length) { charts.deepDoctor.clear(); return; }

    var clusters = distinctClusters(docMatch);
    var titleSum = {};
    docMatch.forEach(function (r) {
      if (r.doctor_title) titleSum[r.doctor_title] = (titleSum[r.doctor_title] || 0) + (Number(r.total_sales) || 0);
    });
    var titles = Object.keys(titleSum).sort(function (a, b) { return titleSum[b] - titleSum[a]; }).slice(0, 8);

    var series = clusters.map(function (cl) {
      return {
        name: "簇 " + cl,
        type: "bar",
        stack: "doc",
        itemStyle: { color: clusterColor(cl) },
        data: titles.map(function (t) {
          var row = docMatch.find(function (r) { return Number(r.cluster) === cl && r.doctor_title === t; });
          return row ? (Number(row.total_sales) || 0) : 0;
        })
      };
    });
    charts.deepDoctor.setOption({
      title: { text: "聚类×医生头衔销量分布（TOP 8 头衔）", left: "center", textStyle: { fontSize: 13, color: "#0f172a" } },
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
      legend: { bottom: 0 },
      grid: { left: 12, right: 12, top: 40, bottom: 60, containLabel: true },
      xAxis: { type: "category", data: titles.map(function (t) { return (t || "").slice(0, 10); }), axisLabel: { rotate: 25, fontSize: 10 } },
      yAxis: { type: "value", name: "销量" },
      series: series
    });
  }

  function renderAll() {
    syncMonthSelect();
    renderOverview();
    renderCity();
    renderCategory();
    renderChannel();
    renderTrend();
    renderRegionPref();
    renderPriceDiscount();
    renderCluster();
    renderClusterDeep();
  }

  function loadData() {
    var q = selectedMonth === "all" ? "" : ("?month=" + encodeURIComponent(selectedMonth));
    var h401 = function (r) {
      if (r.status === 401) {
        window.location.href = "/login";
        return Promise.reject(new Error("401"));
      }
      return r.json();
    };
    return Promise.all([
      fetch("/api/analytics" + q).then(h401),
      fetch("/api/cluster").then(h401),
      fetch("/api/cluster_insights").then(h401).catch(function () { return {}; })
    ]).then(function (triple) {
      trendData = triple[0];
      trendData.cluster_centers = triple[1].cluster_centers;
      clusterInsights = triple[2] || {};
      renderAll();
      toast("数据已更新");
    }).catch(function (e) {
      if (e && e.message !== "401") toast("加载失败：" + (e.message || ""));
    });
  }

  function switchPanel(id) {
    document.querySelectorAll(".nav-item").forEach(function (btn) {
      btn.classList.toggle("active", btn.getAttribute("data-panel") === id);
    });
    document.querySelectorAll(".panel").forEach(function (p) {
      p.classList.toggle("active", p.id === "panel-" + id);
    });
    document.getElementById("panelBreadcrumb").textContent =
      document.querySelector('.nav-item[data-panel="' + id + '"]').textContent.trim();
    setTimeout(function () {
      Object.keys(charts).forEach(function (k) {
        var c = charts[k];
        if (c && c.resize) c.resize();
      });
    }, 80);
  }

  function bindNav() {
    document.querySelectorAll(".nav-item").forEach(function (btn) {
      btn.addEventListener("click", function () {
        switchPanel(btn.getAttribute("data-panel"));
      });
    });
  }

  function bindToolbars() {
    var sel = document.getElementById("selectStatsMonth");
    if (sel) {
      sel.addEventListener("change", function () {
        selectedMonth = sel.value || "all";
        loadData();
      });
    }

    document.getElementById("btnRefreshAll").addEventListener("click", function () { loadData(); });
    var ov = document.getElementById("btnOverviewRefresh");
    if (ov) ov.addEventListener("click", function () { loadData(); });
    document.getElementById("btnCityRefresh").addEventListener("click", function () { renderCity(); toast("图表已刷新"); });
    document.getElementById("btnCityExport").addEventListener("click", function () {
      downloadCsv("地区销量.csv", getCityRows());
    });
    document.getElementById("btnCityToggleTop").addEventListener("click", function () {
      cityTopN = cityTopN >= 999 ? 15 : 999;
      document.getElementById("btnCityToggleTop").textContent = cityTopN >= 999 ? "仅显示 TOP15" : "显示全部城市";
      renderCity();
      toast(cityTopN >= 999 ? "已显示全部城市" : "已切换为 TOP15");
    });

    document.getElementById("btnCatRefresh").addEventListener("click", function () { renderCategory(); toast("图表已刷新"); });
    document.getElementById("btnCatExport").addEventListener("click", function () {
      downloadCsv("品类销量.csv", getCategoryRows());
    });

    document.getElementById("btnChRefresh").addEventListener("click", function () { renderChannel(); toast("图表已刷新"); });
    document.getElementById("btnChExport").addEventListener("click", function () {
      downloadCsv("渠道销量.csv", getChannelRows());
    });

    var tr = document.getElementById("btnTrendRefresh");
    if (tr) tr.addEventListener("click", function () { renderTrend(); toast("已刷新"); });
    var tm = document.getElementById("btnTrendExportMonth");
    if (tm) tm.addEventListener("click", function () {
      downloadCsv("分月销售趋势.csv", (trendData && trendData.sales_trend_by_month) || []);
    });
    var tp = document.getElementById("btnTrendExportProj");
    if (tp) tp.addEventListener("click", function () {
      downloadCsv("项目销量TOP.csv", (trendData && trendData.top_projects) || []);
    });

    var rr = document.getElementById("btnRegionRefresh");
    if (rr) rr.addEventListener("click", function () { renderRegionPref(); toast("已刷新"); });
    var ed = document.getElementById("btnExportDistrict");
    if (ed) ed.addEventListener("click", function () {
      downloadCsv("商圈区县销量.csv", (trendData && trendData.by_district) || []);
    });
    var ec = document.getElementById("btnExportCityCat");
    if (ec) ec.addEventListener("click", function () {
      downloadCsv("城市品类联合TOP.csv", (trendData && trendData.city_category_top) || []);
    });

    var pr = document.getElementById("btnPriceRefresh");
    if (pr) pr.addEventListener("click", function () { renderPriceDiscount(); toast("已刷新"); });
    var exd = document.getElementById("btnExportDiscount");
    if (exd) exd.addEventListener("click", function () {
      downloadCsv("折扣分桶统计.csv", (trendData && trendData.discount_band_stats) || []);
    });
    var exdoc = document.getElementById("btnExportDoctorTitle");
    if (exdoc) exdoc.addEventListener("click", function () {
      downloadCsv("医生头衔统计.csv", (trendData && trendData.doctor_title_stats) || []);
    });

    document.getElementById("btnClusterRefresh").addEventListener("click", function () { renderCluster(); toast("已刷新"); });
    document.getElementById("btnClusterExport").addEventListener("click", function () {
      var cc = trendData && trendData.cluster_centers;
      if (!cc || !cc.centers) {
        toast("无聚类数据");
        return;
      }
      var cols = ["聚类编号"].concat(cc.feature_cols || []);
      var rows = (cc.centers || []).map(function (vec, i) {
        var o = { 聚类编号: "簇_" + i };
        (cc.feature_cols || []).forEach(function (c, j) {
          o[c] = vec[j];
        });
        return o;
      });
      downloadCsv("聚类中心.csv", rows);
    });

    // 聚类深度分析页按钮
    var btnDR = document.getElementById("btnDeepRefresh");
    if (btnDR) btnDR.addEventListener("click", function () {
      fetch("/api/cluster_insights")
        .then(function (r) { return r.status === 401 ? (window.location.href = "/login", null) : r.json(); })
        .then(function (j) { if (j) { clusterInsights = j; renderClusterDeep(); toast("已刷新"); } });
    });
    var btnEP = document.getElementById("btnExportProfile");
    if (btnEP) btnEP.addEventListener("click", function () {
      downloadCsv("聚类画像.csv", (clusterInsights && clusterInsights.cluster_profile) || []);
    });
    var btnECR = document.getElementById("btnExportCityRank");
    if (btnECR) btnECR.addEventListener("click", function () {
      downloadCsv("聚类_城市排名.csv", (clusterInsights && clusterInsights.cluster_city_rank) || []);
    });
    var btnECP = document.getElementById("btnExportCatPref");
    if (btnECP) btnECP.addEventListener("click", function () {
      downloadCsv("聚类_品类偏好.csv", (clusterInsights && clusterInsights.cluster_category_preference) || []);
    });
    var btnETP = document.getElementById("btnExportTopProj");
    if (btnETP) btnETP.addEventListener("click", function () {
      downloadCsv("聚类_TOP项目.csv", (clusterInsights && clusterInsights.cluster_top_projects) || []);
    });
    var btnEDM = document.getElementById("btnExportDocMatch");
    if (btnEDM) btnEDM.addEventListener("click", function () {
      downloadCsv("聚类_医生头衔.csv", (clusterInsights && clusterInsights.cluster_doctor_matching) || []);
    });
  }

  window.addEventListener("resize", function () {
    Object.keys(charts).forEach(function (k) {
      if (charts[k] && charts[k].resize) charts[k].resize();
    });
  });

  document.addEventListener("DOMContentLoaded", function () {
    bindNav();
    bindToolbars();
    loadData();
  });
})();

