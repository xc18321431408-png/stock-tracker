# CLAUDE.md

## 项目概述
美股/A股板块追踪 + 量化交易平台。Flask + Vanilla JS 单页应用，部署在 Railway。
- 线上: https://worthy-grace-production-2af9.up.railway.app
- 仓库: https://github.com/xc18321431408-png/stock-tracker (main分支)

## 常用命令
```bash
# 本地运行
python app.py                          # 启动Flask (端口8080)

# 部署 (自动: git push main → Railway, 手动:)
railway up                            # 部署到Railway生产环境
railway status                        # 查看部署状态

# 测试API
curl -s https://worthy-grace-production-2af9.up.railway.app/api/health
curl -s https://worthy-grace-production-2af9.up.railway.app/api/stock/NEE/t0
```

## 架构
```
app.py (4036行)          → Flask后端，全部API + APScheduler定时抓取 + 内存缓存
templates/index.html (3134行) → 前端单页应用，10个Tab，Vanilla JS + Chart.js + ECharts
data/sectors.db          → SQLite本地缓存
gunicorn.conf.py         → preload_app=True, workers=2, timeout=120
```

## 前端10个Tab
板块追踪 | 涨跌气泡图 | 板块轮动 | 相关性矩阵 | 趋势分析 | 自选监控 | 做T助手 | 智能选股 | 市场情报 | 策略回测

Tab切换通过 `switchTab('name')` 控制 `display:none/block`。不要改这个机制。

## 重要约定（必须遵守）

### 弹窗位置
`modalOverlay` 必须放在所有tab的 **外面**（footer和toast之间）。切勿放进任何tab div内部，否则在非仪表盘tab弹窗会被 `display:none` 隐藏。

### 板块点击
统一使用 inline `onclick="openSectorDetail('板块名')"`。不要用事件委托（data-sector + closest），历史证明在某些浏览器不可靠。

### 股票涨跌幅
- `fetch_stock_quotes()`: 用 `meta.previousClose`（1m接口）
- `api_watchlist_quotes()`: 用 `closes_raw[-2]`（不能用chartPreviousClose，1mo range会返回一个月前的价格）

### 板块轮动多周期收益
d5/d20/d60必须用累计收益 `(d1_close - prev_close) / prev_close * 100`，不能直接用单日change_pct。

### 做T助手
必须同时拉取日内(5m)和日线(1d)数据做多周期共振。日线RSI(14)/BB(20,2)权重(30-35分)高于日内(5m)信号权重(8-25分)。历史bug：只看5分钟线导致日线RSI=20时给出"超买"信号。

### 移动端
使用 `isMobile()` 函数（`window.innerWidth <= 768`）判断，关键布局用JS条件切换grid列数。CSS有 `@media(max-width:768px)` 全局媒体查询。

## API路由分类
```
/api/latest|dates|sector/<name>         → 核心板块数据
/api/cn/latest|dates|sector/<name>      → A股数据
/api/stock/<symbol>/t0|technicals|history → 个股分析
/api/rotation?ref=prev                  → 轮动数据(ref=prev=昨日视角)
/api/rotation/oversold?limit=10         → 超跌反弹信号
/api/watchlist/quotes                   → 自选股行情
/api/backtest|screener|macro|correlation → 回测/选股/宏观/相关性
/api/market/intel|news|cn/news          → 情报/新闻
/api/health                             → 健康检查
```

## 数据刷新
- APScheduler定时: 美股03:30 UTC, A股06:30 UTC
- 前端自动: Dashboard 10min, Watchlist 30s, News 15min
- 所有数据存在内存字典，重启丢失需重新抓取

## 用户偏好
- 说中文，回复简洁直接
- 先测试再确认，不猜测
- 不要改动已稳定的电脑端布局
- 用 `railway up` 部署（需用户确认生产环境）
