# 美股/A股板块追踪 + 量化交易平台 v3.0

> 在线地址：`https://worthy-grace-production-2af9.up.railway.app`

## 项目概述

全栈股票板块追踪与量化交易信号平台，支持**美股**和**A股**双市场。基于 Flask + Vanilla JS，部署在 Railway。

### 技术栈

| 层级 | 技术 |
|------|------|
| 后端 | Python 3.12 + Flask + APScheduler + Gunicorn |
| 前端 | Vanilla JS + Chart.js + ECharts |
| 数据源 | Yahoo Finance API v8 |
| 部署 | Railway (GitHub Auto Deploy) |
| 存储 | 内存缓存 + 本地 SQLite (`data/sectors.db`) |

---

## 项目结构

```
us-stock-tracker/
├── app.py                  # 主后端 (4036行) - Flask API + 数据抓取 + 缓存
├── templates/
│   └── index.html          # 前端单页应用 (3134行) - 所有页面和逻辑
├── static/                 # 静态资源
├── data/
│   └── sectors.db          # 板块数据本地缓存 (SQLite)
├── check_sectors.py        # 板块数据诊断工具
├── init_sample.py          # 初始化采样
├── requirements.txt        # Python 依赖
├── runtime.txt             # Python 版本声明
├── Procfile                # Railway 启动命令
├── gunicorn.conf.py        # Gunicorn 配置
├── render.yaml             # Render.com 备用部署配置
└── start.sh                # 本地开发启动脚本
```

---

## 前端功能 (10个Tab)

### 1. 📊 板块追踪 (Dashboard)
- **市场切换**：美股 / A股 双市场
- **日期导航**：滑块选择历史日期、回到最新
- **概览卡片**：板块总数、上涨/下跌数、涨跌比、平均涨幅、新高/新低
- **板块列表**：所有板块涨跌幅，可点击查看龙头股 Top 10
- **分类分组**：按行业分类（半导体、能源、金融等）
- **主图表**：Top/Bottom 15 子板块涨跌幅柱状图
- **宏观指标**：VIX、美元指数、美债收益率、原油、黄金、比特币
- **新闻面板**：Yahoo Finance + KOL 推文聚合

### 2. 🫧 涨跌气泡图
- ECharts 气泡图：X轴=涨跌幅，Y轴=成交量，气泡大小=市值
- 红色=上涨，绿色=下跌
- 支持日期切换

### 3. 🔥 板块轮动 (Rotation)
- **多周期排名**：1日 / 5日 / 20日 / 60日涨跌幅排名
- **Top 10 最强 / 最弱**：双列排名卡片
- **昨日收盘视角**：切换查看反弹前的真实排名（排除今日涨幅干扰）
- **超跌反弹信号**：基于昨日收盘60日跌幅，找出最超跌的板块
  - 命中反弹的标为金色 🎯
  - 点击板块名弹出龙头股详情

### 4. 🔗 相关性矩阵
- ECharts 热力图：板块间相关性矩阵
- 支持日期切换

### 5. 📈 趋势分析
- 所有板块60日趋势线图
- 标注当前趋势标签（强势上涨、反弹中、弱势下跌等）
- 可切换板块

### 6. ⭐ 自选监控
- 自定义股票列表（LocalStorage 持久化）
- 实时行情：价格、涨跌幅、RSI(14)、成交量
- K线迷你图（Sparkline）
- 点击展开详细技术分析面板

### 7. 📉 做T助手 (T0)
- **多周期共振分析**：5分钟K线 + 日线
- **日内指标**：RSI(6)、MACD(12,26,9)、布林带(20,2)、VWAP
- **日线背景**：RSI(14)、布林带(20,2)、日涨跌幅
- **综合评分**：做多/做空信号加权打分
- **信号等级**：强烈做多(≥40) / 偏多做T(≥20) / 观望 / 偏空做T(≤-20) / 强烈做空(≤-40)
- 快捷按钮：NVDA、SPY、贵州茅台、宁德时代等

### 8. 🎯 智能选股
- **综合评分选股**：低RSI(超卖) + 高夏普(高效) + MACD上升 + 放量 + 布林下轨
- **回测策略选股**：基于历史回测结果的推荐标的
- 美股/A股双市场

### 9. 📰 市场情报
- 市场事件日历（财报、经济数据等）
- IPO 日历
- 双列布局（桌面端）

### 10. 🔬 策略回测
- **均值回归策略**：500次蒙特卡洛模拟
- 策略逻辑：超跌板块 → 低RSI个股 → 缩量 → 未涨
- 回测参数自定义
- 结果：收益率、胜率、夏普比率、最大回撤
- 回测历史记录

---

## 后端 API 文档

### 核心数据接口

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/latest` | GET | 美股最新板块数据 |
| `/api/dates` | GET | 美股可用日期列表 |
| `/api/sector/<name>` | GET | 板块龙头股详情 (Top 10) |
| `/api/history` | GET | 历史板块数据 (用于K线) |
| `/api/refresh` | POST | 手动触发数据刷新 |
| `/api/health` | GET | 系统健康检查 |
| `/api/macro` | GET | 宏观指标 (VIX, DXY等) |
| `/api/news` | GET | 美股新闻聚合 |
| `/api/correlation` | GET | 美股板块相关性矩阵 |

### A股接口

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/cn/latest` | GET | A股最新板块数据 |
| `/api/cn/dates` | GET | A股可用日期列表 |
| `/api/cn/sector/<name>` | GET | A股板块龙头股 |
| `/api/cn/rotation` | GET | A股板块轮动 |
| `/api/cn/correlation` | GET | A股板块相关性 |
| `/api/cn/news` | GET | A股新闻聚合 |
| `/api/cn/screener/us` | GET | A股智能选股 |
| `/api/cn/backtest` | POST | A股策略回测 |

### 股票分析接口

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/stock/<symbol>/t0` | GET | T0做T信号 (日内+日线多周期) |
| `/api/stock/<symbol>/technicals` | GET | 技术指标 (均线/RSI/MACD/布林/KDJ等) |
| `/api/stock/<symbol>/history` | GET | 历史K线数据 |
| `/api/stock/<symbol>/intraday` | GET | 日内分时数据 |
| `/api/watchlist/quotes` | POST | 批量自选股实时行情 |

### 轮动与信号接口

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/rotation` | GET | 美股板块轮动数据 (支持 `?ref=prev` 昨日视角) |
| `/api/rotation/oversold` | GET | 超跌反弹信号 (基于昨日60日跌幅) |

### 回测接口

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/backtest` | POST | 美股策略回测 |
| `/api/backtest/history` | GET | 回测历史记录 |
| `/api/backtest/mean-reversion` | GET/POST | 均值回归回测 (500次蒙特卡洛) |
| `/api/cn/backtest` | POST | A股策略回测 |

### 市场情报接口

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/market/intel` | GET | 市场事件 + IPO日历 |
| `/api/screener/us` | GET | 美股智能选股结果 |
| `/api/news` | GET | 美股新闻 |
| `/api/cn/news` | GET | A股新闻 |

### 管理接口

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/backfill` | POST | 历史数据回填 |
| `/` | GET | 主页面 |

---

## 数据流架构

```
Yahoo Finance API v8
        │
        ▼
┌─────────────────┐
│  APScheduler     │  定时任务 (美股: 每天 03:30 UTC, A股: 每天 06:30 UTC)
│  fetch_sector    │  抓取所有板块ETF的日线数据
│  fetch_cn_sector │  计算涨跌幅、缓存到内存
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Flask API       │  /api/latest, /api/sector/<name>, /api/rotation, etc.
│  内存缓存         │  板块数据、轮动数据、技术指标缓存
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  前端 SPA        │  Vanilla JS + Chart.js + ECharts
│  10个Tab         │  数据可视化、交互分析
└─────────────────┘
```

### 缓存策略

| 缓存 | TTL | 说明 |
|------|-----|------|
| 板块数据 | 内存常驻 | APScheduler定时刷新 |
| 个股技术指标 | 每月刷新 | `/api/stock/<symbol>/technicals` |
| KOL推文 | 30分钟 | market_intel |
| 轮动数据 | 实时计算 | 访问时基于最新板块数据生成 |

---

## 关键算法

### 1. 做T多周期共振评分
```python
# 日内信号 (5分钟K线)
RSI(6) < 25 → 做多 +25    RSI(6) > 75 → 做空 +25
触布林下轨 → 做多 +20      触布林上轨 → 做空 +20
MACD金叉  → 做多 +18       MACD死叉 → 做空 +18
低于VWAP  → 做多 +8        高于VWAP → 做空 +8

# 日线背景 (更高权重)
RSI(14) < 30 → 做多 +30    RSI(14) > 70 → 做空 +30
日线触下轨 → 做多 +35      日线触上轨 → 做空 +35
日线收跌   → 做多 +10

# 综合判断
net_score = 做多总分 - 做空总分
≥40: 强烈做多 | ≥20: 偏多 | <20: 观望 | ≤-20: 偏空 | ≤-40: 强烈做空
```

### 2. 板块轮动累计收益计算
```python
# 所有多周期收益统一从收盘价计算
def cum_return(prev_close):
    return (d1_close - prev_close) / prev_close * 100

# d5/d20/d60 返回使用对应日期的close，而非单日change_pct
```

### 3. 超跌反弹信号
```python
# 基准日 = 昨天收盘 (排除今日涨幅干扰)
yesterday = dates[1]
d60_yesterday = dates[60]

# 计算60日跌幅 (昨天vs60天前)，升序排列取最弱
sorted_by_d60 = sorted(sectors, key=lambda x: x['d60_yesterday'])

# 今天涨了的 = 命中反弹 → 金色标记 🎯
```

---

## 部署运维

### Railway 部署
```bash
# 自动部署: push到main分支即可触发
git push origin main

# 手动部署:
railway up

# 查看状态:
railway status

# 环境变量 (Railway Dashboard 配置):
# FLASK_ENV=production
```

### 关键配置

**gunicorn.conf.py:**
```python
preload_app = True   # APScheduler在worker fork前启动
workers = 2           # Railway免费层限制
timeout = 120         # 回测等长请求的超时
```

**requirements.txt:**
```
flask>=3.0
requests>=2.31
apscheduler>=3.10
```

### 定时任务
- **美股数据抓取**：每天 03:30 UTC (美东 23:30)
- **A股数据抓取**：每天 06:30 UTC (北京时间 14:30)
- **前端自动刷新**：Dashboard 10分钟，Watchlist 30秒，News 15分钟

### 健康检查
`GET /api/health` 返回所有子系统的状态和最新数据时间。

---

## 本地开发

```bash
# 安装依赖
pip install -r requirements.txt

# 启动服务
python app.py
# 访问 http://localhost:8080

# 使用 start.sh (包含外网隧道)
./start.sh
```

---

## 设计特点

1. **单页应用 (SPA)**：所有10个Tab共用一个HTML文件，Tab切换通过 `display:none/block` 控制
2. **事件委托**：板块点击使用 `onclick` + `data-sector` 属性，弹窗在所有Tab外
3. **移动端适配**：`@media(max-width:768px)` 全面响应式，关键布局JS动态切换
4. **内存优先**：所有数据缓存在Python字典中，APScheduler定时更新
5. **渐进增强**：日线数据作为可选项，获取失败时降级为仅日内分析

---

*最后更新: 2026-08-10 | v3.0*
