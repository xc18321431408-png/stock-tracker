"""
US + China A-Share Stock Market Sector Tracker v3.0
- 40+ US sub-industry ETFs
- 30+ China A-Share sector ETFs
- Top 10 constituent stocks per sector
- Quantitative trading backtest platform
- Security protections
"""
import os, json, sqlite3, secrets, re, time, requests, math, threading, concurrent.futures
from datetime import datetime, timedelta
from functools import wraps
from collections import defaultdict
from email.utils import parsedate_to_datetime
import xml.etree.ElementTree as ET
from flask import Flask, render_template, request, jsonify, abort
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)

# ── Config ─────────────────────────────────────────────────
MAX_REQUESTS = 60
ADMIN_USER = "admin"
ADMIN_PASS = secrets.token_hex(8)
rate_store = {}
DB_PATH = os.path.join(os.path.dirname(__file__), 'data', 'sectors.db')
CN_DB_PATH = os.path.join(os.path.dirname(__file__), 'data', 'cn_sectors.db')

SA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

YF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}

# ── News Cache ─────────────────────────────────────────────
_news_cache = {}  # {market: (timestamp, articles)}
_kol_cache = {}   # {market: (timestamp, articles)}
_technicals_cache = {}  # {symbol: (timestamp, data)} — 30 day TTL
_news_cache_lock = threading.Lock()

# ── KOL Twitter Accounts ──────────────────────────────────
# (handle, display_name, focus_tags, market)
TWITTER_KOLS = [
    ("aleabitoreddit", "Serenity·白毛股神", ["AI人工智能", "半导体", "芯片"], "us"),
    ("dylan522p", "Dylan Patel·半导体研究", ["半导体", "芯片", "AI人工智能"], "us"),
    ("firstadopter", "Tae Kim·芯片分析", ["半导体", "芯片", "科技"], "us"),
    ("xingpt", "XinGPT·AI供应链", ["AI人工智能", "半导体", "云计算"], "us"),
    ("tengyanai", "滕岩·AI半导体", ["AI人工智能", "半导体", "科技"], "us"),
    ("kobeissiletter", "Kobeissi·宏观策略", ["金融", "宏观经济"], "us"),
    ("amy6tina", "Sober·期权策略", ["金融", "期权"], "us"),
    ("incomesharks", "IncomeSharks·财报分析", ["金融", "财报"], "us"),
    ("convertbond", "L.McDonald·流动性与利率", ["金融", "宏观经济", "银行"], "us"),
    # CN KOLs (less active on Twitter, but occasionally post)
    ("diaomao2023", "交易员小帅·宏观", ["宏观经济", "金融"], "cn"),
    ("jackli727", "零下二度·期权宏观", ["宏观经济", "期权"], "cn"),
]

# Build ticker→sector lookup cache
_ticker_sector_map = {}  # {ticker_lower: [sector_names]}
_ticker_sector_built = False

def _build_ticker_sector_map():
    """Build a lookup from stock ticker to sector names."""
    global _ticker_sector_built
    if _ticker_sector_built:
        return
    for sector_stocks in [SECTOR_STOCKS, CN_SECTOR_STOCKS]:
        for sector_name, stocks in sector_stocks.items():
            for info in stocks:
                ticker = info[0].lower().replace('.ss','').replace('.sz','').replace('.bj','')
                if ticker not in _ticker_sector_map:
                    _ticker_sector_map[ticker] = []
                if sector_name not in _ticker_sector_map[ticker]:
                    _ticker_sector_map[ticker].append(sector_name)
    _ticker_sector_built = True

def _extract_tickers_from_text(text):
    """Extract stock tickers from tweet text (e.g. $AAPL, $NVDA)."""
    tickers = set()
    for match in re.finditer(r'\$([A-Za-z]{1,6})', text):
        ticker = match.group(1).upper()
        tickers.add(ticker)
    # Also match common ticker mentions without $ (e.g. 'NVDA is up')
    # But be more careful to avoid false positives - match uppercase 2-5 letter words
    for match in re.finditer(r'\b([A-Z]{2,5})\b', text):
        ticker = match.group(1)
        if ticker.lower() in _ticker_sector_map:
            tickers.add(ticker)
    return tickers

def _fetch_kol_tweets_via_nitter(handle, name):
    """Fetch recent tweets for a KOL via Nitter RSS. Returns list of articles."""
    articles = []
    # Try multiple Nitter instances for reliability (with delay between attempts)
    instances = [
        'https://nitter.net',
        'https://nitter.poast.org',
        'https://nitter.1d4.us',
        'https://nitter.privacydev.net',
    ]
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'application/rss+xml, application/xml, text/xml, */*',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    for idx, instance in enumerate(instances):
        try:
            if idx > 0:
                time.sleep(2)  # Delay between instance attempts
            url = f"{instance}/{handle}/rss"
            resp = requests.get(url, headers=headers, timeout=12)
            if resp.status_code != 200 or len(resp.text) < 200:
                print(f"[KOL] Nitter {instance}/{handle} returned {resp.status_code}, len={len(resp.text)}")
                continue
            root = ET.fromstring(resp.text)
            item_count = 0
            for item in root.findall('.//item'):
                title_el = item.find('title')
                link_el = item.find('link')
                pub_el = item.find('pubDate')
                desc_el = item.find('description')
                title = title_el.text.strip() if title_el is not None and title_el.text else ''
                link = link_el.text.strip() if link_el is not None and link_el.text else '#'
                published = _parse_rss_date(pub_el.text.strip()) if pub_el is not None and pub_el.text else ''
                desc = desc_el.text.strip() if desc_el is not None and desc_el.text else ''
                if not title:
                    continue
                item_count += 1
                # Extract stock tickers and match to sectors
                tickers = _extract_tickers_from_text(title + ' ' + desc)
                matched_sectors = set()
                for t in tickers:
                    secs = _ticker_sector_map.get(t.lower(), [])
                    matched_sectors.update(secs)
                articles.append({
                    'title': f'🐦 {name}: {title}',
                    'link': link,
                    'published': published,
                    'source': f'X·{name.split("·")[0]}',
                    'sectors': list(matched_sectors),
                    'kol': name,
                    'tickers': list(tickers),
                })
            print(f"[KOL] Got {item_count} tweets from {instance}/{handle}")
            break  # success
        except Exception as e:
            print(f"[KOL] Nitter error for {handle} via {instance}: {e}")
            continue
    if not articles:
        print(f"[KOL] WARNING: No tweets fetched for {handle} from any instance")
    return articles

def _fetch_all_kol_tweets(market='us'):
    """Fetch tweets from all KOLs for given market (cached 20 min)."""
    now = time.time()
    with _news_cache_lock:
        cached = _kol_cache.get(market)
        if cached and (now - cached[0]) < 1200:  # 20 min cache
            return cached[1]

    _build_ticker_sector_map()

    kols = [k for k in TWITTER_KOLS if k[3] == market]

    all_articles = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_fetch_kol_tweets_via_nitter, k[0], k[1]): k for k in kols}
        for future in concurrent.futures.as_completed(futures):
            k = futures[future]
            try:
                articles = future.result()
                # Add KOL focus tags to articles without sector matches
                focus_tags = k[2]
                for a in articles:
                    if not a['sectors']:
                        a['sectors'] = focus_tags[:2]
                all_articles.extend(articles)
            except Exception as e:
                print(f"[KOL] Error fetching {k[1]}: {e}")

    # Deduplicate by title
    seen = set()
    unique = []
    for a in all_articles:
        key = a['title'].strip().lower()[:80]
        if key not in seen:
            seen.add(key)
            unique.append(a)

    # Sort by time
    def _sort_key(a):
        try:
            return datetime.fromisoformat(a.get('published','').replace('Z','+00:00'))
        except:
            return datetime(2000,1,1)
    unique.sort(key=_sort_key, reverse=True)

    # Format for frontend
    result = []
    for a in unique:
        result.append({
            'title': a['title'],
            'link': a.get('link', '#'),
            'source': a.get('source', ''),
            'published': a.get('published', ''),
            'sector_tags': json.dumps(a.get('sectors', []), ensure_ascii=False),
        })

    with _news_cache_lock:
        _kol_cache[market] = (now, result)

    return result

def _parse_rss_date(date_str):
    """Parse various RSS date formats to ISO."""
    try:
        return parsedate_to_datetime(date_str).isoformat()
    except:
        return date_str

def _fetch_yahoo_rss(symbol, company_name=''):
    """Fetch Yahoo Finance RSS headlines for a single stock symbol.
    Only returns articles that mention the symbol or company name."""
    articles = []
    try:
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
        resp = requests.get(url, headers=YF_HEADERS, timeout=8)
        if resp.status_code != 200:
            return articles
        root = ET.fromstring(resp.content)
        for item in root.findall('.//item'):
            title_el = item.find('title')
            link_el = item.find('link')
            pub_el = item.find('pubDate')
            desc_el = item.find('description')
            title = title_el.text.strip() if title_el is not None and title_el.text else ''
            link = link_el.text.strip() if link_el is not None and link_el.text else '#'
            published = _parse_rss_date(pub_el.text.strip()) if pub_el is not None and pub_el.text else ''
            desc = desc_el.text.strip() if desc_el is not None and desc_el.text else ''
            # Only keep articles that mention the stock symbol or company name
            text_to_check = (title + ' ' + desc).lower()
            sym_lower = symbol.lower()
            # Check if symbol appears as a whole word (not part of another word)
            sym_mentioned = re.search(r'\b' + re.escape(sym_lower) + r'\b', text_to_check)
            name_mentioned = company_name and company_name.lower() in text_to_check
            if title and (sym_mentioned or name_mentioned or len(articles) < 3):
                articles.append({
                    'title': title,
                    'link': link,
                    'published': published,
                    'source': 'Yahoo',
                    'symbol': symbol,
                    'desc': desc,
                })
        # If we got enough filtered articles, only return those; otherwise keep top 5
        filtered = [a for a in articles if sym_lower in (a['title'] + a['desc']).lower()]
        if len(filtered) >= 2:
            articles = filtered
    except Exception as e:
        print(f"[News] RSS error for {symbol}: {e}")
    return articles

def _fetch_cn_stock_news(symbol, name):
    """Fetch news for CN A-share stock using East Money API."""
    articles = []
    try:
        code = symbol.replace('.SS','').replace('.SZ','').replace('.BJ','')
        # Determine market code: 1=SSE, 0=SZSE
        if '.SZ' in symbol:
            mkt = '0'
        elif '.BJ' in symbol:
            # use a try-fetch with SSE format first then fallback
            mkt = '0'
        else:
            mkt = '1'
        secid = f"{mkt}.{code}"
        url = f"https://push2.eastmoney.com/api/qt/stock/news/get?secid={secid}&page=1&size=8"
        resp = requests.get(url, headers=YF_HEADERS, timeout=8)
        if resp.status_code != 200:
            return articles
        data = resp.json()
        news_list = (data.get('data') or {}).get('list') or []
        for item in news_list:
            articles.append({
                'title': item.get('title', ''),
                'link': item.get('url', '#'),
                'published': item.get('showTime', ''),
                'source': item.get('source', '东方财富'),
                'symbol': symbol,
                'desc': item.get('digest', ''),
            })
    except Exception as e:
        print(f"[News] CN news error for {symbol}: {e}")
    return articles

def _get_symbol_sector_map(market='us'):
    """Build symbol → [sector_names] mapping, also returns {sector: [stock_infos]}."""
    sectors = SECTOR_STOCKS if market != 'cn' else CN_SECTOR_STOCKS
    symbol_sectors = defaultdict(list)
    sector_stocks = {}
    for sector_name, stocks in sectors.items():
        sector_stocks[sector_name] = []
        for info in stocks:
            sym = info[0]
            name = info[1]
            desc = info[2] if len(info) > 2 else ''
            symbol_sectors[sym].append(sector_name)
            sector_stocks[sector_name].append({'symbol': sym, 'name': name, 'desc': desc})
    return dict(symbol_sectors), sector_stocks

def _fetch_all_news(market='us'):
    """Fetch news for all sector stocks (cached 10min)."""
    now = time.time()
    with _news_cache_lock:
        cached = _news_cache.get(market)
        if cached and (now - cached[0]) < 600:
            return cached[1]

    symbol_sectors, sector_stocks = _get_symbol_sector_map(market)

    # Collect unique symbols (top 2 per sector for speed)
    seen = set()
    fetch_symbols = []
    for sname, stocks in sector_stocks.items():
        for s in stocks[:2]:
            if s['symbol'] not in seen:
                seen.add(s['symbol'])
                fetch_symbols.append((s['symbol'], s['name']))

    # Limit to avoid overwhelming requests
    fetch_symbols = fetch_symbols[:40]

    all_articles = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        if market == 'cn':
            futures = {executor.submit(_fetch_cn_stock_news, sym, name): sym for sym, name in fetch_symbols}
        else:
            futures = {executor.submit(_fetch_yahoo_rss, sym, name): sym for sym, name in fetch_symbols}

        # Also fetch KOL tweets in parallel
        kol_future = executor.submit(_fetch_all_kol_tweets, market)

        for future in concurrent.futures.as_completed(futures):
            sym = futures[future]
            try:
                articles = future.result()
                sectors = symbol_sectors.get(sym, [])
                for a in articles:
                    a['sectors'] = sectors
                all_articles.extend(articles)
            except Exception as e:
                print(f"[News] Future error for {sym}: {e}")

        # Get KOL tweets result
        try:
            kol_articles = kol_future.result()
            all_articles.extend(kol_articles)
            print(f"[News] Merged {len(kol_articles)} KOL tweets")
        except Exception as e:
            print(f"[News] KOL fetch error: {e}")

    # Deduplicate by exact title match
    seen_titles = set()
    unique = []
    for a in all_articles:
        title_key = a['title'].strip().lower()
        if title_key and title_key not in seen_titles:
            seen_titles.add(title_key)
            unique.append(a)

    # Sort by published time (newest first), articles without time go last
    def _sort_key(a):
        try:
            return datetime.fromisoformat(a.get('published', '').replace('Z','+00:00'))
        except:
            try:
                return datetime.strptime(a.get('published', ''), '%Y-%m-%d %H:%M:%S')
            except:
                return datetime(2000, 1, 1)
    unique.sort(key=_sort_key, reverse=True)

    # Format for frontend
    result = []
    for a in unique:
        result.append({
            'title': a['title'],
            'link': a.get('link', '#'),
            'source': a.get('source', ''),
            'published': a.get('published', ''),
            'sector_tags': json.dumps(a.get('sectors', []), ensure_ascii=False),
        })

    with _news_cache_lock:
        _news_cache[market] = (now, result)

    return result

# ── 40+ Sub-Industry ETFs ───────────────────────────────────
# (parent_category, symbol, chinese_name)
SUB_SECTORS = [
    # 科技 (XLK parent)
    ("科技", "SMH", "半导体"), ("科技", "SOXX", "芯片"), ("科技", "IGV", "软件"),
    ("科技", "CLOU", "云计算"), ("科技", "CIBR", "网络安全"), ("科技", "ARKK", "颠覆创新"),
    ("科技", "ROBT", "机器人AI"), ("科技", "FINX", "金融科技"),
    ("科技", "BOTZ", "AI人工智能"), ("科技", "CIEN", "光模块光通信"),
    ("科技", "SMH", "存储芯片"),
    ("科技", "TSM", "全球半导体"), ("科技", "ASML", "半导体设备"),
    # 全球/新兴市场
    ("全球", "EWJ", "日本市场"), ("全球", "EWG", "德国市场"), ("全球", "EWU", "英国市场"),
    ("全球", "FXI", "中国大盘"), ("全球", "KWEB", "中国互联网"), ("全球", "EWZ", "巴西市场"),
    ("全球", "INDA", "印度市场"), ("全球", "EWY", "韩国市场"),
    # 金融 (XLF parent)
    ("金融", "KBE", "银行"), ("金融", "KRE", "区域银行"), ("金融", "IAI", "券商投行"),
    ("金融", "KIE", "保险"), ("金融", "REM", "抵押REITs"),
    # 医疗 (XLV parent)
    ("医疗健康", "IBB", "生物科技"), ("医疗健康", "XBI", "生技"), ("医疗健康", "IHI", "医疗器械"),
    ("医疗健康", "XHE", "医疗设备"), ("医疗健康", "PJP", "制药"),
    # 能源 (XLE parent)
    ("能源", "XOP", "油气勘探"), ("能源", "OIH", "油服"), ("能源", "TAN", "太阳能"),
    ("能源", "FAN", "风能"), ("能源", "ICLN", "清洁能源"),
    # 工业 (XLI parent)
    ("工业", "ITA", "航空航天"), ("工业", "XAR", "国防军工"), ("工业", "PAVE", "基建工程"),
    # 消费 (XLY/XLP parents)
    ("消费品", "XRT", "零售"), ("消费品", "IBUY", "在线零售"), ("消费品", "XHB", "房屋建筑"),
    ("消费品", "PEJ", "娱乐餐饮"), ("消费品", "PBJ", "食品饮料"),
    # 房地产 (XLRE parent)
    ("房地产", "VNQ", "综合REITs"), ("房地产", "REZ", "住宅REITs"), ("房地产", "INDS", "工业REITs"),
    # 通信 (XLC parent)
    ("通信服务", "FIVG", "5G通信"), ("通信服务", "SOCL", "社交媒体"),
    # 材料 (XLB parent)
    ("基础材料", "GDX", "金矿"), ("基础材料", "SLV", "白银"), ("基础材料", "PICK", "矿业"),
    ("基础材料", "XME", "金属矿业"),
    # 公用事业 (XLU parent)
    ("公用事业", "URA", "铀与核能"), ("公用事业", "PHO", "水务"),
    # 指数
    ("指数", "SPY", "标普500"), ("指数", "QQQ", "纳斯达克100"), ("指数", "DIA", "道琼斯"),
    ("指数", "IWM", "罗素2000"), ("指数", "EEM", "新兴市场"), ("指数", "VXX", "波动率"),
]

# ── Sector → Top 10 Constituent Stocks (with name + description) ──
SECTOR_STOCKS = {
    "半导体": [
        ("NVDA","英伟达","AI芯片龙头，全球GPU市占率超80%"),
        ("TSM","台积电","全球最大晶圆代工厂(市占>55%)，苹果/NVidia/AMD核心代工"),
        ("AVGO","博通","网络芯片与基础设施软件巨头"),
        ("ASML","阿斯麦","全球独家EUV光刻机，芯片制造必需设备"),
        ("AMD","超威半导体","CPU/GPU双线作战，数据中心快速崛起"),
        ("QCOM","高通","移动通信芯片霸主，5G专利大户"),
        ("TXN","德州仪器","模拟芯片之王，工业/汽车电子广泛"),
        ("INTC","英特尔","老牌CPU厂商，正转型晶圆代工"),
        ("MU","美光科技","全球DRAM/NAND存储三巨头之一"),
        ("AMAT","应用材料","全球最大半导体设备制造商"),
    ],
    "芯片": [
        ("NVDA","英伟达","AI芯片龙头"), ("TSM","台积电","全球晶圆代工霸主"),
        ("AVGO","博通","网络芯片"), ("ASML","阿斯麦","独家EUV光刻机"),
        ("AMD","超威半导体","CPU+GPU"), ("QCOM","高通","移动芯片"),
        ("INTC","英特尔","CPU+代工"), ("MU","美光","存储芯片"),
        ("AMAT","应用材料","半导体设备"), ("MRVL","美满电子","数据中心芯片"),
    ],
    "软件": [
        ("MSFT","微软","全球最大软件公司，Azure云+Office+Copilot AI"),
        ("ORCL","甲骨文","企业级数据库与云基础设施巨头"),
        ("ADBE","Adobe","创意软件+数字营销SaaS之王"),
        ("CRM","赛富时","全球CRM SaaS龙头，Agentforce AI平台"),
        ("NOW","ServiceNow","IT工作流自动化SaaS领导者"),
        ("SAP","SAP","欧洲最大软件公司，ERP系统全球第一"),
        ("INTU","Intuit","财税软件TurboTax+QuickBooks霸主"),
        ("PLTR","Palantir","大数据AI分析，政府+商业双轮驱动"),
        ("SNOW","Snowflake","云数据仓库SaaS先驱"),
        ("WDAY","Workday","HR/财务云SaaS领军者"),
    ],
    "云计算": [
        ("AMZN","亚马逊","AWS全球第一大公有云，电商+云计算双引擎"),
        ("MSFT","微软","Azure全球第二大云平台"),
        ("GOOGL","Alphabet","Google Cloud+搜索+AI(Gemini)"),
        ("CRM","赛富时","CRM SaaS云端化先驱"),
        ("NOW","ServiceNow","ITSM工作流云平台"),
        ("SNOW","Snowflake","云原生数据仓库"),
        ("NET","Cloudflare","全球CDN/网络安全云服务商"),
        ("ZS","Zscaler","零信任云安全领导者"),
        ("DDOG","Datadog","云监控/可观测性SaaS"),
        ("MDB","MongoDB","NoSQL文档数据库云服务"),
    ],
    "网络安全": [
        ("CRWD","CrowdStrike","端点安全+威胁情报AI平台王者"),
        ("PANW","派拓网络","下一代防火墙+云安全SASE先驱"),
        ("ZS","Zscaler","零信任安全架构领导者"),
        ("FTNT","飞塔","全球防火墙出货量第一"),
        ("OKTA","Okta","企业身份管理IAM SaaS龙头"),
        ("NET","Cloudflare","CDN+DDoS防护+Zero Trust"),
        ("S","SentinelOne","AI驱动端点安全新锐"),
        ("CYBR","CyberArk","特权访问管理PAM龙头"),
        ("VRNS","Varonis","数据安全与分析平台"),
        ("TENB","Tenable","漏洞管理/暴露管理领导者"),
    ],
    "金融科技": [
        ("V","Visa","全球最大支付网络，信用卡/借记卡清算"),
        ("MA","万事达","全球第二大支付网络"),
        ("PYPL","PayPal","在线支付鼻祖，Venmo母公司"),
        ("SQ","Block","Square支付+Cash App+比特币生态"),
        ("COIN","Coinbase","美国最大合规加密货币交易所"),
        ("AFRM","Affirm","先买后付BNPL先驱"),
        ("SOFI","SoFi","一站式数字银行+贷款平台"),
        ("TOST","Toast","餐饮业POS/支付SaaS垂直龙头"),
        ("FIS","FIS","银行核心系统+支付处理巨头"),
        ("GPN","Global Payments","全球支付技术服务商"),
    ],
    "银行": [
        ("JPM","摩根大通","美国最大银行，资产超3.8万亿美元"),
        ("BAC","美国银行","全美第二大银行，巴菲特重仓"),
        ("WFC","富国银行","以零售/社区银行见长"),
        ("C","花旗集团","全球化程度最高的美国银行"),
        ("GS","高盛","华尔街顶级投行，交易/并购王者"),
        ("MS","摩根士丹利","财富管理+机构证券双支柱"),
        ("SCHW","嘉信理财","最大折扣券商，零售投资平台"),
        ("PNC","PNC金融","区域性银行巨头，企业银行强"),
        ("USB","美国合众银行","中西区银行龙头，支付业务突出"),
        ("TFC","Truist","BB&T+SunTrust合并，东南区巨擘"),
    ],
    "生物科技": [
        ("AMGN","安进","全球最大独立生物技术公司"),
        ("GILD","吉利德","抗病毒药物之王，HIV/HCV/肿瘤"),
        ("REGN","再生元","抗体药物研发王者，Eylea+Dupixent"),
        ("VRTX","Vertex","囊性纤维化特效药垄断者"),
        ("BIIB","渤健","神经科学/多发性硬化症/阿尔茨海默"),
        ("MRNA","Moderna","mRNA技术平台，新冠疫苗+肿瘤疫苗"),
        ("ILMN","Illumina","基因测序仪全球垄断(市占>70%)"),
        ("ALNY","Alnylam","RNAi疗法先驱，罕见病药物"),
        ("BMRN","BioMarin","罕见病酶替代疗法专家"),
        ("EXAS","Exact Sciences","肠癌早筛Cologuard领导者"),
    ],
    "医疗器械": [
        ("ABT","雅培","医疗多元化巨头，诊断/器械/营养"),
        ("SYK","史赛克","骨科植入物+手术机器人领导者"),
        ("MDT","美敦力","全球最大医疗器械公司，心脏起搏器"),
        ("BSX","波士顿科学","心血管/内窥镜介入器械巨头"),
        ("ISRG","直觉外科","达芬奇手术机器人，全球装机>8000台"),
        ("BDX","碧迪","注射器/输液/诊断系统全球领导者"),
        ("EW","爱德华生命科学","心脏瓣膜(TAVR)全球垄断者"),
        ("ZBH","捷迈邦美","骨科关节植入物巨头"),
        ("HOLX","豪洛捷","女性健康诊断+乳腺影像领导者"),
        ("BAX","百特","肾脏透析/输液泵全球领导者"),
    ],
    "制药": [
        ("LLY","礼来","减肥药(GLP-1)王者，市值制药第一"),
        ("JNJ","强生","全球最大医疗保健公司，制药+器械"),
        ("MRK","默沙东","Keytruda(抗癌药王)制造商"),
        ("ABBV","艾伯维","Humira+Skyrizi免疫药物巨头"),
        ("PFE","辉瑞","新冠疫苗/口服药，多管线制药巨头"),
        ("BMY","百时美施贵宝","肿瘤/免疫/心血管药物"),
        ("NVS","诺华","瑞士制药巨头，基因治疗先驱"),
        ("AZN","阿斯利康","英瑞制药巨头，肿瘤药管线强劲"),
        ("GSK","葛兰素史克","疫苗+呼吸/艾滋药物领导者"),
        ("SNY","赛诺菲","法国制药巨头，疫苗/糖尿病/罕见病"),
    ],
    "油气勘探": [
        ("XOM","埃克森美孚","全球最大上市油气公司"), ("CVX","雪佛龙","综合性能源巨头"),
        ("COP","康菲石油","最大独立油气勘探生产商"), ("EOG","EOG资源","页岩油低成本王者"),
        ("PXD","先锋自然资源","二叠纪盆地最大生产商"), ("FANG","Diamondback","二叠纪纯页岩油"),
        ("DVN","戴文能源","多盆地多元化油气生产"), ("OXY","西方石油","巴菲特重仓，碳捕获先驱"),
        ("HES","赫斯","圭亚那海上油田重磅资产"), ("MRO","马拉松石油","炼油+营销一体化"),
    ],
    "太阳能": [
        ("ENPH","Enphase","微型逆变器全球领导者"), ("FSLR","First Solar","薄膜碲化镉组件龙头"),
        ("SEDG","SolarEdge","组串式逆变器+优化器"), ("RUN","Sunrun","美国最大户用太阳能安装商"),
        ("CSIQ","阿特斯","中国光伏组件/储能全球出货"), ("JKS","晶科能源","全球光伏组件出货量第一"),
        ("NXT","Nextracker","全球最大光伏跟踪支架商"), ("DQ","大全新能源","全球领先多晶硅生产商"),
        ("XPEV","小鹏汽车","不做光伏但投资光储充"), ("NOVA","Sunnova","户用太阳能+储能服务"),
    ],
    "清洁能源": [
        ("ENPH","Enphase","微逆+储能"), ("FSLR","First Solar","薄膜光伏"),
        ("PLUG","Plug Power","氢燃料电池+绿氢"), ("BE","Bloom Energy","固体氧化物燃料电池"),
        ("NEE","NextEra","全球最大风电/光伏运营商"), ("BEP","Brookfield Renewable","全球可再生能源基础设施"),
        ("CWEN","Clearway","美国清洁能源IPP"), ("AY","Atlantica","全球可再生能源资产"),        ("SEDG","SolarEdge","光伏逆变器+储能解决方案"),
        ("RUN","Sunrun","美国最大户用光伏+储能安装商"),

    ],
    "航空航天": [
        ("BA","波音","全球最大航空航天公司，客机+军工"), ("RTX","雷神技术","普惠发动机+柯林斯航空+导弹"),
        ("LMT","洛克希德马丁","全球最大军工企业，F-35制造商"), ("GD","通用动力","军用车辆/核潜艇/湾流公务机"),
        ("NOC","诺斯罗普格鲁曼","B-2/B-21隐身轰炸机制造商"), ("HWM","Howmet","航空发动机精密铸件领导者"),
        ("TDG","TransDigm","航空零部件售后市场垄断者"), ("HEI","海科","航空电子/MRO零部件"),        ("SPR","Spirit Aerosystems","波音/空客核心机身结构件供应商"),
        ("AXON","Axon","执法记录仪+泰瑟枪,航空航天国防科技"),

    ],
    "国防军工": [
        ("LMT","洛克希德马丁","F-35,导弹防御,太空系统"), ("RTX","雷神","爱国者导弹,发动机,传感器"),
        ("GD","通用动力","核潜艇,坦克,湾流"), ("NOC","诺斯罗普","隐身轰炸机,太空系统"),
        ("LHX","L3哈里斯","军用通信/电子战/ISR"), ("HII","亨廷顿英戈尔斯","美国最大军用造船商(航母/核潜艇)"),
        ("KTOS","Kratos","高性价比无人机/靶机"), ("AVAV","AeroVironment","小型无人机/巡飞弹"),        ("CW","Curtiss-Wright","军用电子/执行器/核能控制"),
        ("BWXT","BWX","核反应堆部件+海军核推进"),

    ],
    "零售": [
        ("AMZN","亚马逊","全球最大电商+云计算"), ("WMT","沃尔玛","全球最大实体零售商"),
        ("COST","好市多","会员制仓储零售之王"), ("HD","家得宝","全球最大家居建材零售商"),
        ("LOW","劳氏","第二大家居建材连锁"), ("TGT","塔吉特","时尚折扣百货零售商"),
        ("TJX","TJX","全球最大折扣服装/家居零售商"), ("ROST","罗斯百货","折扣服装连锁"),        ("EBAY","eBay","全球C2C/B2C电商交易平台"),
        ("BBY","百思买","美国最大消费电子零售商"),

    ],
    "在线零售": [
        ("AMZN","亚马逊","全球电商+云计算霸主"), ("MELI","MercadoLibre","拉丁美洲最大电商+支付平台"),
        ("BABA","阿里巴巴","中国电商老大+阿里云"), ("JD","京东","中国自营电商+物流王者"),
        ("PDD","拼多多","Temu全球扩张+拼团电商"), ("CPNG","Coupang","韩国最大电商+火箭配送"),
        ("SE","Sea Limited","东南亚电商Shopee+游戏Garena"), ("EBAY","eBay","全球C2C/B2C拍卖平台"),
        ("CHWY","Chewy","宠物在线零售之王"), ("ETSY","Etsy","全球手工创意品电商"),
    ],
    "房屋建筑": [
        ("DHI","DR Horton","美国最大住宅建筑商"), ("LEN","Lennar","第二大住宅建筑商"),
        ("PHM","PulteGroup","住宅建筑+金融服务"), ("NVR","NVR","高端住宅建筑+抵押贷款"),
        ("TOL","Toll Brothers","美国最大豪华住宅建筑商"), ("KBH","KB Home","定制化住宅建筑商"),        ("BLD","TopBuild","美国最大住宅隔热安装商"),
        ("MTH","Meritage Homes","节能住宅建筑商"),
        ("TMHC","Taylor Morrison","全美大建商"),
        ("MDC","MDC Holdings","Richmond American Homes母公司"),

    ],
    "5G通信": [
        ("QCOM","高通","5G基带/射频芯片霸主"), ("AVGO","博通","RF前端/交换机芯片"),
        ("CSCO","思科","全球网络设备第一"), ("VZ","Verizon","美国最大无线运营商"),
        ("T","AT&T","美国第二大电信运营商"), ("TMUS","T-Mobile","美国第三大运营商(增长最快)"),
        ("ERIC","爱立信","全球5G基站设备三强"), ("NOK","诺基亚","5G/光纤/IP网络设备"),
        ("CIEN","Ciena","光网络/光传输设备领导者"), ("JNPR","瞻博","高端路由器/交换机"),
    ],
    "社交媒体": [
        ("META","Meta","Facebook+Instagram+WhatsApp+Threads"), ("SNAP","Snap","Snapchat,AR社交先驱"),
        ("PINS","Pinterest","图片社交/电商发现平台"), ("MTCH","Match Group","Tinder/Hinge在线约会王者"),
        ("BMBL","Bumble","女性优先约会社交平台"), ("GRND","Grindr","LGBTQ+社交平台"),        ("RDDT","Reddit","美国最大社区论坛平台"),
        ("DASH","DoorDash","本地配送+社交电商平台"),
        ("SPOT","Spotify","全球最大音频流媒体+社交发现"),
        ("TME","腾讯音乐","中国最大在线音乐社交平台"),

    ],
    "金矿": [
        ("NEM","纽蒙特","全球最大金矿企业"), ("GOLD","巴里克黄金","全球第二大金矿"),
        ("AEM","Agnico Eagle","加拿大金矿龙头"), ("FNV","Franco-Nevada","黄金权利金公司(不采矿,只收租)"),
        ("WPM","Wheaton","白银/黄金权利金公司"), ("GFI","Gold Fields","南非金矿巨头"),        ("KGC","Kinross Gold","加拿大大型金矿"),
        ("AU","AngloGold Ashanti","南非/非洲金矿巨头"),
        ("RGLD","Royal Gold","黄金权利金+流转协议"),
        ("HMY","Harmony Gold","南非金矿商"),

    ],
    "综合REITs": [
        ("PLD","Prologis","全球最大工业物流REIT"), ("AMT","American Tower","全球最大通信铁塔REIT"),
        ("EQIX","Equinix","全球最大数据中心REIT"), ("SPG","Simon","全球最大购物中心REIT"),
        ("O","Realty Income","净租赁REIT之王(按月派息)"), ("PSA","Public Storage","全球最大自助仓储REIT"),
        ("WELL","Welltower","医疗养老REIT领导者"),        ("AVB","AvalonBay","高端公寓REIT"),
        ("EQR","Equity Residential","美国最大公寓REIT"),
        ("DLR","Digital Realty","全球最大数据中心REIT之二"),

    ],
    "存储芯片": [
        ("MU","美光科技","全球DRAM/NAND存储三巨头，HBM3E供不应求"),
        ("WDC","西部数据","全球最大硬盘+闪存制造商，拆分闪存业务"),
        ("STX","希捷科技","全球最大机械硬盘制造商，HAMR技术领先"),
        ("NTAP","NetApp","企业级全闪存/混合存储阵列领导者"),
        ("PSTG","Pure Storage","全闪存阵列先驱，Evergreen订阅模式"),
        ("SGH","SMART Global","特种内存/CXL/AI存储解决方案"),
        ("RMBL","Rambus","高速内存接口芯片IP授权领导者"),
        ("FORM","FormFactor","存储芯片探针卡/测试设备龙头"),        ("HPE","慧与","企业存储/超融合/HPC-AI服务器"),
        ("SMCI","超微电脑","AI服务器/存储解决方案,营收爆发"),

    ],
    "全球半导体": [
        ("TSM","台积电","全球最大晶圆代工厂，先进制程垄断"),
        ("ASML","阿斯麦","荷兰光刻机巨头，全球EUV独家供应商"),
        ("NVDA","英伟达","AI GPU全球霸主"), ("AVGO","博通","网络芯片+AI定制芯片"),
        ("AMD","超威半导体","CPU+GPU双线"), ("QCOM","高通","5G移动芯片"),
        ("ARM","Arm Holdings","英国芯片IP授权王者，全球99%手机用ARM架构"),
        ("STM","意法半导体","欧洲最大半导体公司，MCU+传感器"),
        ("UMC","联电","台湾第二大晶圆代工厂"), ("GFS","格芯","美国晶圆代工,A MD剥离"),
    ],
    "半导体设备": [
        ("ASML","阿斯麦","荷兰EUV光刻机独占全球100%市场"),
        ("AMAT","应用材料","全球最大半导体设备商，沉积/刻蚀/检测全系列"),
        ("LRCX","泛林研究","刻蚀设备全球龙头"), ("KLAC","科磊","检测量测王者"),
        ("TER","泰瑞达","测试设备+工业机器人"), ("ENTG","英特格","半导体材料/化学品"),
        ("ONTO","Onto Innovation","先进封装检测设备"), ("ACLS","Axcelis","离子注入设备龙头"),        ("AMKR","Amkor","全球第二大封测厂"),
        ("IPGP","IPG Photonics","光纤激光器龙头,芯片制造用激光"),

    ],
    "中国互联网": [
        ("BABA","阿里巴巴","中国最大电商+云计算(AliCloud)"),
        ("JD","京东","中国最大自营电商+供应链物流"),
        ("PDD","拼多多","中国增长最快电商+Temu全球扩张"),
        ("BIDU","百度","中国AI+搜索引擎+自动驾驶Apollo"),
        ("BILI","哔哩哔哩","中国年轻世代视频社区+游戏"),
        ("TME","腾讯音乐","中国最大在线音乐娱乐平台"),
        ("VIPS","唯品会","中国品牌折扣电商"), ("ATHM","汽车之家","中国最大汽车垂直平台"),
        ("TAL","好未来","中国K12教育科技龙头"), ("NIO","蔚来","中国高端电动车新势力"),
    ],
    "铀与核能": [
        ("CCJ","Cameco","全球最大上市铀矿公司"), ("UEC","Uranium Energy","美国铀矿+ISR技术"),
        ("BWXT","BWX Technologies","核反应堆部件+核燃料"), ("CEG","Constellation Energy","美国最大核电运营商"),
        ("VST","Vistra","核电+可再生能源+储能"), ("TLN","Talen Energy","核电+数据中心供电"),        ("DNN","Denison Mines","加拿大铀矿开发商+ISR"),
        ("NXE","NexGen Energy","加拿大高品位铀矿"),
        ("SMR","NuScale Power","小型模块化核反应堆SMR先驱"),
        ("LEU","Centrus Energy","美国唯一铀浓缩公司,HALEU"),

    ],
    "光模块光通信": [
        ("COHR","Coherent","全球光模块龙头，800G/1.6T光器件先驱"),
        ("LITE","Lumentum","3D传感+光通信激光器芯片领导者"),
        ("CIEN","Ciena","光传输/光网络设备全球领导者"),
        ("JNPR","瞻博","高端路由器/交换机/光网络"),
        ("FN","Fabrinet","光模块OEM代工龙头,NVidia供应商"),
        ("AAOI","Applied Optoelectronics","光模块/光纤接入设备制造商"),
        ("INFN","Infinera","相干光传输设备领导者"),
        ("HLIT","Harmonic","视频+宽带光纤接入解决方案"),        ("VIAV","Viavi Solutions","光通信测试/网络性能监测"),
        ("MRVL","美满电子","数据中心光模块DSP/硅光引擎"),

    ],
    "AI人工智能": [
        ("NVDA","英伟达","AI训练/推理GPU全球垄断"), ("MSFT","微软","OpenAI合作,Copilot+Azure AI"),
        ("GOOGL","Alphabet","Gemini AI+TPU芯片+DeepMind"), ("AMZN","亚马逊","AWS AI/Bedrock+Alexa"),
        ("META","Meta","Llama开源大模型+AI社交"), ("PLTR","Palantir","AI大数据分析平台"),
        ("AVGO","博通","AI定制芯片(TPU)+网络芯片"), ("AMD","超威半导体","MI300X AI GPU挑战NVDA"),
        ("MRVL","美满","AI数据中心DSP/光模块芯片"), ("ANET","Arista","AI数据中心交换机王者"),
    ],
    "颠覆创新": [
        ("TSLA","特斯拉","电动车+机器人+能源+FSD自动驾驶"), ("RXRX","Recursion","AI药物发现平台先驱"),
        ("CRSP","CRISPR Therapeutics","基因编辑(CRISPR-Cas9)疗法领导者"),
        ("ROKU","Roku","流媒体电视平台,美国客厅入口"), ("ZM","Zoom","视频会议SaaS先驱"),
        ("SHOP","Shopify","全球电商独立站SaaS龙头"), ("U","Unity","3D游戏引擎+元宇宙基础设施"),
        ("PATH","UiPath","RPA机器人流程自动化领导者"), ("HOOD","Robinhood","零佣金交易平台,Z世代券商"),        ("ABNB","Airbnb","全球最大民宿共享平台"),

    ],
    "机器人AI": [
        ("NVDA","英伟达","机器人AI芯片+Isaac平台"), ("ISRG","直觉外科","达芬奇手术机器人全球装机>8000台"),
        ("TSLA","特斯拉","Optimus人形机器人"), ("TER","泰瑞达","工业机器人+半导体测试设备"),
        ("PATH","UiPath","软件RPA机器人流程自动化"), ("ROK","罗克韦尔","工业自动化+智能制造领导者"),
        ("EMR","艾默生","工业自动化+过程控制全球巨头"), ("ZBRA","斑马技术","仓储/物流机器人+自动识别"),
        ("CGNX","康耐视","机器视觉/工业读码系统全球领导者"),        ("AMBA","Ambarella","AI视觉芯片/自动驾驶/机器人感知"),

    ],
    # ── 全球市场 ──
    "日本市场": [
        ("TM","丰田汽车","全球最大汽车制造商，混动技术领导者"),
        ("SONY","索尼","游戏/影像/半导体/娱乐综合巨头"),
        ("NTDOY","任天堂","Switch/塞尔达/马里奥，游戏IP之王"),
        ("HMC","本田汽车","摩托车+汽车+电动化全球巨头"),
        ("MUFG","三菱日联","日本最大银行集团"),
        ("SMFG","三井住友","日本三大银行之一"),
        ("TAK","武田制药","日本最大制药公司，全球化布局"),
        ("KYOCY","京瓷","精密陶瓷/电子零部件/太阳能"),
        ("SFTBY","软银集团","科技投资巨头，Arm大股东"),
        ("CAJ","佳能","影像/光学/医疗设备全球领导者"),
    ],
    "德国市场": [
        ("SAP","SAP","欧洲最大软件公司，ERP全球第一"),
        ("SIEGY","西门子","工业自动化/医疗/能源综合巨头"),
        ("DTEGY","德国电信","欧洲最大电信运营商"),
        ("ALIZY","安联","全球最大保险和资产管理集团"),
        ("BAMXF","宝马","豪华汽车三巨头之一"),
        ("MBGYY","奔驰","豪华车龙头，电动化转型"),
        ("VWAGY","大众","全球最大汽车集团，多品牌矩阵"),
        ("BASFY","巴斯夫","全球最大化工公司"),
        ("ADS","阿迪达斯","全球第二大运动品牌"),
        ("IFNNY","英飞凌","欧洲最大半导体公司，汽车芯片"),
    ],
    "英国市场": [
        ("AZN","阿斯利康","英瑞制药巨头，肿瘤药管线强劲"),
        ("SHEL","壳牌","全球最大能源公司之一"),
        ("UL","联合利华","全球消费品巨头，日化/食品"),
        ("HSBC","汇丰","欧洲最大银行，亚洲业务重心"),
        ("DEO","帝亚吉欧","全球最大洋酒公司"),
        ("RELX","励讯","专业信息/展览/数据分析服务"),
        ("LSEG","伦交所","伦敦证券交易所集团，金融数据"),
        ("BTI","英美烟草","全球烟草四巨头之一"),
        ("CPNG","Coupang","韩国最大电商（注：部分英国指数成分）"),
        ("GSK","葛兰素史克","疫苗+呼吸/艾滋药物领导者"),
    ],
    "中国大盘": [
        ("BABA","阿里巴巴","中国最大电商+云计算(AliCloud)"),
        ("TCEHY","腾讯","中国最大互联网公司，微信/游戏/投资"),
        ("JD","京东","中国最大自营电商+供应链物流"),
        ("PDD","拼多多","中国增长最快电商+Temu全球扩张"),
        ("BIDU","百度","中国AI+搜索引擎+自动驾驶Apollo"),
        ("NIO","蔚来","中国高端电动车新势力"),
        ("LI","理想汽车","中国增程式电动车领导者"),
        ("XPEV","小鹏汽车","智能驾驶+飞行汽车"),
        ("BEKE","贝壳","中国最大房产交易服务平台"),
        ("ZTO","中通快递","中国最大快递物流公司之一"),
    ],
    "巴西市场": [
        ("VALE","淡水河谷","全球最大铁矿石生产商"),
        ("PBR","巴西石油","南美最大石油公司"),
        ("ITUB","伊塔乌联合银行","巴西最大私营银行"),
        ("ABEV","安贝夫","全球最大啤酒公司（百威英博旗下）"),
        ("BBD","布拉德斯科银行","巴西大型银行集团"),
        ("PETZ","Petz","巴西最大宠物零售连锁"),
        ("GGB","盖尔道","巴西最大钢铁公司"),
        ("CX","西麦斯","墨西哥水泥巨头，拉美业务广泛"),
        ("SBS","塞比斯帕","巴西最大水务公司"),
        ("UGP","Ultrapar","巴西最大石化分销商"),
    ],
    "印度市场": [
        ("INFY","Infosys","印度第二大IT外包服务巨头"),
        ("WIT","Wipro","印度IT服务/咨询全球领导者"),
        ("HDB","HDFC银行","印度最大私营银行"),
        ("IBN","ICICI银行","印度第二大私营银行"),
        ("TTM","塔塔汽车","印度最大汽车集团，捷豹路虎母公司"),
        ("VEDL","韦丹塔","印度最大矿业/金属集团"),
        ("MIND","Mindtree","印度IT咨询与数字转型"),
        ("MM","马恒达","印度最大汽车/拖拉机集团"),
        ("RELIANCE","信实工业","印度最大民营企业，能源/零售/电信"),
        ("SUNPHARMA","太阳制药","印度最大制药公司"),
    ],
    "韩国市场": [
        ("TSM","台积电","注：韩国市场ETF含部分台湾/韩国半导体"),
        ("LPL","LG显示","全球液晶面板领导者"),
        ("KB","KB金融","韩国最大金融集团"),
        ("SKM","SK电讯","韩国最大电信运营商"),
        ("PKX","浦项制铁","全球最大钢铁公司之一"),
        ("SHG","新韩金融","韩国大型银行集团"),
        ("KEP","韩国电力","韩国最大电力公用事业"),
        ("IX","ORIX","日本/韩国综合金融（部分持仓）"),
        ("WP","Worley","韩国/澳洲能源工程服务"),
        ("SSL","Sasol","南非/韩国能源化工（部分持仓）"),
    ],
    # ── 金融 ──
    "区域银行": [
        ("SCHW","嘉信理财","最大折扣券商，零售投资平台"),
        ("PNC","PNC金融","区域性银行巨头，企业银行强"),
        ("USB","美国合众银行","中西区银行龙头"),
        ("TFC","Truist","BB&T+SunTrust合并，东南区巨擘"),
        ("FITB","五三银行","俄亥俄/中西部区域银行"),
        ("RF","Regions金融","美国东南区银行"),
        ("CFG","公民金融","东北区银行， student lending"),
        ("KEY","KeyCorp","俄亥俄/纽约区域银行"),
        ("HBAN","Huntington","俄亥俄/密歇根社区银行"),
        ("ZION","Zions Bancorp","犹他/西部区域银行"),
    ],
    "券商投行": [
        ("GS","高盛","华尔街顶级投行，交易/并购王者"),
        ("MS","摩根士丹利","财富管理+机构证券双支柱"),
        ("SCHW","嘉信理财","最大零售券商+财富管理"),
        ("IBKR","盈透证券","全球电子交易/做市商"),
        ("HOOD","Robinhood","零佣金交易平台，Z世代券商"),
        ("LAZR","Lazard","精品投行/资产管理"),
        ("BLK","贝莱德","全球最大资产管理公司"),
        ("STT","道富","全球托管/资产管理巨头"),
        ("NTRS","北方信托","资产管理+私人银行"),
        ("APO","阿波罗","另类投资/私募股权巨头"),
    ],
    "保险": [
        ("BRK.B","伯克希尔","巴菲特旗下，保险+投资帝国"),
        ("UNH","联合健康","美国最大健康保险，Optum医疗"),
        ("PGR","前进保险","美国最大汽车保险之一"),
        ("CI","信诺","健康保险+药房福利管理"),
        ("HUM","哈门那","Medicare Advantage保险巨头"),
        ("ELV","Elevance Health","蓝十字蓝盾协会最大成员"),
        ("MET","大都会人寿","美国最大人寿保险之一"),
        ("AFL","Aflac","补充保险/癌症保险全球知名"),
        ("ALL","好事达","美国最大财产/意外险之一"),
        ("TRV","旅行者","财产/意外险，道指成分股"),
    ],
    "抵押REITs": [
        ("NLY","Annaly","美国最大抵押REIT，机构MBS"),
        ("AGNC","AGNC投资","抵押REIT，机构MBS策略"),
        ("STWD","Starwood","商业抵押/贷款REIT"),
        ("LADR","Ladder Capital","商业抵押/贷款+证券"),
        ("BXMT","Blackstone抵押","黑石旗下商业抵押REIT"),
        ("ARI","Apollo Commercial","阿波罗旗下商业抵押"),
        ("RC","Ready Capital","小型商业抵押/贷款"),
        ("MFA","MFA Financial","住宅/商业抵押混合"),
        ("ARR","ARMOUR Residential","住宅抵押REIT"),
        ("CHMI","Cherry Hill","住宅抵押/服务"),
    ],
    # ── 医疗健康 ──
    "生技": [
        ("VRTX","Vertex","囊性纤维化特效药垄断者"),
        ("REGN","再生元","抗体药物研发王者"),
        ("MRNA","Moderna","mRNA技术平台，新冠+肿瘤疫苗"),
        ("GILD","吉利德","抗病毒药物之王"),
        ("BIIB","渤健","神经科学/阿尔茨海默药物"),
        ("ALNY","Alnylam","RNAi疗法先驱"),
        ("BMRN","BioMarin","罕见病酶替代疗法"),
        ("EXAS","Exact Sciences","肠癌早筛Cologuard领导者"),
        ("SRPT","Sarepta","杜氏肌营养不良基因疗法"),
        ("IONS","Ionis","反义RNA药物平台"),
    ],
    "医疗设备": [
        ("ABT","雅培","医疗多元化，诊断/器械/营养"),
        ("MDT","美敦力","全球最大医疗器械，心脏起搏器"),
        ("SYK","史赛克","骨科植入物+手术机器人"),
        ("BSX","波士顿科学","心血管/内窥镜介入器械"),
        ("ISRG","直觉外科","达芬奇手术机器人"),
        ("BDX","碧迪","注射器/输液/诊断系统"),
        ("EW","爱德华生命科学","心脏瓣膜(TAVR)垄断者"),
        ("ZBH","捷迈邦美","骨科关节植入物"),
        ("HOLX","豪洛捷","女性健康诊断+乳腺影像"),
        ("BAX","百特","肾脏透析/输液泵"),
    ],
    # ── 能源 ──
    "油服": [
        ("SLB","斯伦贝谢","全球最大油田服务公司"),
        ("BKR","贝克休斯","第二大油服，旋转设备"),
        ("HAL","哈里伯顿","全球三大油服之一"),
        ("NOV","国民油井","钻井设备/完井解决方案"),
        ("FTI","TechnipFMC","海底生产/完井系统"),
        ("PTEN","Patterson-UTI","美国陆地钻井承包商"),
        ("HP","Helmerich","美国最大陆地钻井承包商"),
        ("NBR","Nabors","全球钻井/油服设备"),
        ("OII","Oceaneering","海底机器人/油气工程"),
        ("RES","RPC","美国压裂/完井服务"),
    ],
    "风能": [
        ("GE","GE Vernova","全球最大风机制造商之一"),
        ("VWDRY","维斯塔斯","全球最大风机制造商"),
        ("NPI","Northland Power","加拿大风电/可再生能源"),
        ("ORA","Ormat","地热/储能/可再生能源"),
        ("GNRC","Generac","备用发电机+储能/微电网"),
        ("AME","安默生","工业自动化+风电控制"),
        ("BWXT","BWX","核反应堆+清洁能源部件"),
        ("CEG","Constellation","美国最大核电运营商"),
        ("NEE","NextEra","全球最大风电/光伏运营商"),
        ("BEP","Brookfield Renewable","全球可再生能源基础设施"),
    ],
    # ── 工业 ──
    "基建工程": [
        ("CAT","卡特彼勒","全球最大工程机械/矿山设备"),
        ("DE","约翰迪尔","全球最大农业机械"),
        ("VMC","火神材料","美国最大建筑骨料/沥青"),
        ("MLM","Martin Marietta","第二大建筑骨料/水泥"),
        ("FLR","Fluor","全球工程/建筑/项目管理巨头"),
        ("J","Jacobs","工程/建筑/环保服务"),
        ("DY","Dycom","电信基础设施施工"),
        ("APG","APi Group","消防/安全/特种建筑"),
        ("EME","EMCOR","机电/能源/基础设施服务"),
        ("FIX","Comfort Systems","暖通/电气/管道承包"),
    ],
    # ── 消费品 ──
    "娱乐餐饮": [
        ("DIS","迪士尼","全球最大娱乐集团，影视/乐园/流媒体"),
        ("NFLX","奈飞","全球最大流媒体平台"),
        ("CMCSA","康卡斯特","NBC环球+最大有线电视"),
        ("WBD","华纳兄弟探索","HBO/Discovery/DC影业"),
        ("LYV","Live Nation","全球最大演唱会/票务平台"),
        ("DKNG","DraftKings","美国最大数字体育博彩"),
        ("MGM","美高梅","博彩/酒店/娱乐度假村"),
        ("CZR","凯撒娱乐","美国最大博彩娱乐集团"),
        ("WYNN","永利度假村","高端博彩/酒店"),
        ("LVS","拉斯维加斯金沙","澳门/新加坡博彩度假村"),
    ],
    "食品饮料": [
        ("KO","可口可乐","全球最大饮料公司"),
        ("PEP","百事","饮料+零食全球巨头"),
        ("MDLZ","亿滋","奥利奥/吉百利/趣多多全球"),
        ("GIS","通用磨坊","麦片/酸奶/宠物食品"),
        ("K","家乐氏","谷物早餐/零食/植物肉"),
        ("CPB","金宝汤","罐头汤/零食/餐食"),
        ("HSY","好时","北美最大巧克力制造商"),
        ("MKC","味好美","全球最大香料/调味品"),
        ("CAG","康尼格拉","冷冻食品/零食/配菜"),
        ("LW","Lamb Weston","全球最大冷冻薯条供应商"),
    ],
    # ── 房地产 ──
    "住宅REITs": [
        ("AVB","AvalonBay","美国最大公寓REIT之一"),
        ("EQR","Equity Residential","公寓REIT，高端住宅"),
        ("UDR","UDR","公寓REIT， Sun Belt布局"),
        ("CPT","Camden","公寓REIT， Sun Belt/德州"),
        ("ESS","Essex","西海岸公寓REIT"),
        ("MAA","Mid-America","东南部公寓REIT"),
        ("AIRC","Apartment Income","中端公寓REIT"),
        ("THO","Thor Industries","全美最大房车制造商（部分住宅指数）"),
        ("CVCO","Cavco","模块化/预制房屋制造"),
        ("SKY","Skyline Champion","预制房屋+HUD代码住房"),
    ],
    "工业REITs": [
        ("PLD","Prologis","全球最大工业物流REIT"),
        ("DRE","Duke Realty","工业/物流地产REIT（被PLD收购）"),
        ("EGP","EastGroup"," Sun Belt工业REIT"),
        ("FR","First Industrial","中西部/东部工业地产"),
        ("TRNO","Terreno","沿海港口工业地产"),
        ("REXR","Rexford","南加州工业REIT"),
        ("CCI","Crown Castle","通信基础设施/铁塔REIT"),
        ("AMT","American Tower","全球最大通信铁塔REIT"),
        ("SBAC","SBA通信","通信铁塔/小型蜂窝"),
        ("UNIT","Uniti","通信基础设施/光纤"),
    ],
    # ── 基础材料 ──
    "白银": [
        ("SLV","iShares白银","全球最大白银ETF（信托本身）"),
        ("PAAS","泛美白银","全球最大原生银矿商之一"),
        ("AG","First Majestic","墨西哥白银生产商"),
        ("CDE","Coeur Mining","美国白银+黄金生产商"),
        ("HL","Hecla Mining","美国最大白银生产商"),
        ("SVM","希尔威金属","中国/加拿大白银矿商"),
        ("EXK","Endeavour Silver","墨西哥中型白银矿商"),
        ("MAG","MAG Silver","墨西哥白银勘探/开发"),
        ("FSM","Fortuna Silver","拉美白银+金矿商"),
        ("WPM","Wheaton Precious","白银/黄金权利金公司"),
    ],
    "矿业": [
        ("BHP","必和必拓","全球最大矿业公司"),
        ("RIO","力拓","全球第二大矿业巨头"),
        ("VALE","淡水河谷","全球最大铁矿石生产商"),
        ("TECK","泰克资源","加拿大多元化矿业"),
        ("MT","安赛乐米塔尔","全球最大钢铁公司"),
        ("CLF","Cleveland-Cliffs","美国最大扁钢生产商"),
        ("MP","MP Materials","美国最大稀土生产商"),
        ("SCCO","南方铜业","秘鲁最大铜矿/冶炼"),
        ("FCX","自由港麦克莫兰","全球最大上市铜矿商"),
        ("NEM","纽蒙特","全球最大金矿企业"),
    ],
    "金属矿业": [
        ("FCX","自由港麦克莫兰","全球最大上市铜矿商"),
        ("NEM","纽蒙特","全球最大金矿企业"),
        ("SCCO","南方铜业","秘鲁最大铜矿"),
        ("STLD","Steel Dynamics","美国电炉钢领导者"),
        ("NUE","纽柯钢铁","美国最大钢铁回收/电炉钢"),
        ("CLF","Cleveland-Cliffs","美国最大扁钢生产商"),
        ("RS","Reliance Steel","美国最大金属服务中心"),
        ("CMC","Commercial Metals","回收钢铁/建筑钢筋"),
        ("X","美国钢铁","传统综合钢铁厂（被日铁收购中）"),
        ("ATI","ATI","特种不锈钢/钛合金航空材料"),
    ],
    # ── 公用事业 ──
    "水务": [
        ("AWK","American Water","美国最大上市水务公司"),
        ("CWT","California Water","加州水务公用事业"),
        ("SJW","SJW Group","加州/德州水务"),
        ("MSEX","Middlesex","新泽西/特拉华水务"),
        ("YORW","York Water","美国最古老投资者持有水务公司"),
        ("ARTNA","Artesian","特拉华水务/废水处理"),
        ("GWRS","Global Water","亚利桑那/新墨西哥水务"),
        ("CTWS","Connecticut Water","康涅狄格水务"),
        ("WTRG","Essential Utilities","宾州/俄亥俄水务+天然气"),
        ("XLU","公用事业ETF","代表整个公用事业板块（指数）"),
    ],
}

# ── China A-Share Sector ETFs ───────────────────────────────
CN_SECTORS = [
    # 科技
    ("科技", "512480.SS", "半导体"),
    ("科技", "515980.SS", "人工智能"), ("科技", "515050.SS", "5G通信"),
    ("科技", "515880.SS", "通信服务"), ("科技", "515230.SS", "软件"),
    ("科技", "516010.SS", "游戏"), ("科技", "515260.SS", "消费电子"),
    # 金融
    ("金融", "512000.SS", "券商"), ("金融", "512800.SS", "银行"),
    ("金融", "516100.SS", "金融科技"),
    # 消费
    ("消费", "512690.SS", "白酒"), ("消费", "515710.SS", "食品饮料"),
    ("消费", "159928.SZ", "大消费"), ("消费", "159996.SZ", "家电"),
    ("消费", "159766.SZ", "旅游酒店"), ("消费", "512980.SS", "传媒"),
    # 医疗
    ("医疗", "512170.SS", "医疗服务"), ("医疗", "512290.SS", "生物医药"),
    ("医疗", "159992.SZ", "创新药"), ("医疗", "159898.SZ", "医疗器械"),
    ("医疗", "159647.SZ", "中药"),
    # 新能源/材料
    ("新能源材料", "515790.SS", "光伏"), ("新能源材料", "515030.SS", "新能源车"),
    ("新能源材料", "515700.SS", "动力电池"), ("新能源材料", "512400.SS", "有色金属"),
    ("新能源材料", "516150.SS", "稀土"), ("新能源材料", "515210.SS", "钢铁"),
    ("新能源材料", "515220.SS", "煤炭"), ("新能源材料", "516020.SS", "化工"),
    # 工业/制造
    ("工业制造", "512660.SS", "军工"), ("工业制造", "516320.SS", "高端制造"),
    ("工业制造", "516960.SS", "机械设备"), ("工业制造", "516950.SS", "基建"),
    ("工业制造", "516910.SS", "物流"),
    # 房地产
    ("房地产", "512200.SS", "房地产"),
    # 公用事业
    ("公用事业", "159611.SZ", "电力"), ("公用事业", "512580.SS", "环保"),
    # 指数
    ("指数", "510050.SS", "上证50"), ("指数", "510300.SS", "沪深300"),
    ("指数", "510500.SS", "中证500"), ("指数", "159915.SZ", "创业板"),
    ("指数", "588000.SS", "科创板50"), ("指数", "512100.SS", "中证1000"),
    ("指数", "510880.SS", "红利"), ("指数", "518880.SS", "黄金"),
    # 跨境
    ("跨境", "513100.SS", "纳指"), ("跨境", "513050.SS", "中概互联"),
    ("跨境", "159892.SZ", "恒生科技"), ("跨境", "513520.SS", "日经"),
]

CN_SECTOR_STOCKS = {
    "半导体": [
        ("688981.SS","中芯国际","中国大陆最大晶圆代工厂"),
        ("603501.SS","韦尔股份","CMOS图像传感器全球前三"),
        ("603986.SS","兆易创新","NOR Flash+MCU存储芯片龙头"),
        ("002371.SZ","北方华创","半导体设备平台型龙头"),
        ("600703.SS","三安光电","LED+化合物半导体龙头"),
        ("600584.SS","长电科技","全球第三大封测厂"),
        ("002049.SZ","紫光国微","特种集成电路+安全芯片"),
        ("300661.SZ","圣邦股份","模拟芯片设计龙头"),
        ("300782.SZ","卓胜微","射频前端芯片龙头"),
        ("600745.SS","闻泰科技","ODM+功率半导体双龙头"),
    ],
    "人工智能": [
        ("002230.SZ","科大讯飞","国内语音识别AI龙头"),
        ("603019.SS","中科曙光","国产服务器/超算龙头"),
        ("000938.SZ","浪潮信息","AI服务器出货量国内第一"),
        ("688787.SS","海天瑞声","AI训练数据服务"),
        ("300308.SZ","中际旭创","800G光模块全球龙头"),
        ("300502.SZ","新易盛","光模块核心供应商"),
        ("603496.SS","恒为科技","网络可视化+算力可视化"),
        ("300418.SZ","昆仑万维","大模型+海外互联网"),
        ("688561.SS","奇安信","网络安全龙头"),
        ("300033.SZ","同花顺","金融AI+数据服务"),
    ],
    "5G通信": [
        ("600498.SS","烽火通信","光通信设备龙头"),
        ("000063.SZ","中兴通讯","5G通信设备双寡头之一"),
        ("300502.SZ","新易盛","光模块核心供应商"),
        ("300308.SZ","中际旭创","800G光模块全球龙头"),
        ("002281.SZ","光迅科技","光器件/光模块龙头"),
        ("600487.SS","亨通光电","光纤光缆龙头"),
        ("300394.SZ","天孚通信","光器件细分领域龙头"),
        ("603236.SS","移远通信","物联网模组全球龙头"),
        ("002402.SZ","和而泰","智能控制器龙头"),
        ("600522.SS","中天科技","海缆+光纤光缆"),
    ],
    "通信服务": [
        ("000063.SZ","中兴通讯","通信设备全球第四"),
        ("600941.SS","中国移动","全球最大运营商"),
        ("600050.SS","中国联通","国内三大运营商之一"),
        ("601728.SS","中国电信","国内三大运营商之一"),
        ("300628.SZ","亿联网络","SIP话机全球第一"),
        ("603236.SS","移远通信","物联网模组全球龙头"),
        ("300502.SZ","新易盛","光模块核心供应商"),
        ("300308.SZ","中际旭创","800G光模块全球龙头"),
        ("600498.SS","烽火通信","光通信设备龙头"),
        ("300394.SZ","天孚通信","光器件细分领域龙头"),
    ],
    "软件": [
        ("600536.SS","中国软件","国产操作系统核心"),
        ("002153.SZ","石基信息","酒店/餐饮SaaS龙头"),
        ("300033.SZ","同花顺","金融信息服务商龙头"),
        ("002230.SZ","科大讯飞","语音识别AI龙头"),
        ("600845.SS","宝信软件","钢铁信息化+IDC"),
        ("300496.SZ","中科创达","智能操作系统龙头"),
        ("688111.SS","金山办公","WPS办公软件龙头"),
        ("600588.SS","用友网络","企业管理软件龙头"),
        ("002912.SZ","中新赛克","网络可视化"),
        ("300229.SZ","拓尔思","NLP自然语言处理"),
    ],
    "游戏": [
        ("002602.SZ","世纪华通","游戏出海+IDC"),
        ("002555.SZ","三七互娱","手游研运一体化龙头"),
        ("603444.SS","吉比特","精品游戏研运"),
        ("300418.SZ","昆仑万维","大模型+海外游戏"),
        ("002624.SZ","完美世界","端游/手游/影视"),
        ("300031.SZ","宝通科技","游戏+工业互联网"),
        ("300052.SZ","中青宝","云游戏概念"),
        ("002174.SZ","游族网络","卡牌游戏研运"),
        ("300459.SZ","汤姆猫","休闲游戏出海"),
        ("002517.SZ","恺英网络","传奇类游戏研运"),
    ],
    "消费电子": [
        ("002475.SZ","立讯精密","消费电子精密制造龙头"),
        ("601138.SS","工业富联","全球电子制造服务龙头"),
        ("000725.SZ","京东方A","全球面板龙头"),
        ("002384.SZ","东山精密","PCB+精密结构件"),
        ("002241.SZ","歌尔股份","VR/AR设备代工龙头"),
        ("300433.SZ","蓝思科技","消费电子玻璃盖板龙头"),
        ("603501.SS","韦尔股份","CMOS图像传感器全球前三"),
        ("600745.SS","闻泰科技","ODM+功率半导体"),
        ("000100.SZ","TCL科技","面板+半导体显示"),
        ("002273.SZ","水晶光电","光学光电子龙头"),
    ],
    # ── 金融 ──
    "券商": [
        ("600030.SS","中信证券","国内券商龙头"),
        ("300059.SZ","东方财富","互联网券商+基金销售"),
        ("600837.SS","海通证券","头部综合券商"),
        ("601688.SS","华泰证券","科技驱动型券商"),
        ("600999.SS","招商证券","央企背景头部券商"),
        ("000776.SZ","广发证券","投行/资管实力强"),
        ("601211.SS","国泰君安","老牌头部券商"),
        ("600958.SS","东方证券","资管特色券商"),
        ("601377.SS","兴业证券","福建区域龙头券商"),
        ("002736.SZ","国信证券","深圳国资背景券商"),
    ],
    "银行": [
        ("600036.SS","招商银行","零售银行之王"),
        ("601398.SS","工商银行","全球最大银行"),
        ("601288.SS","农业银行","县域金融龙头"),
        ("601939.SS","建设银行","基建金融特色"),
        ("601988.SS","中国银行","全球化程度最高"),
        ("600016.SS","民生银行","民营资本银行"),
        ("601166.SS","兴业银行","同业业务起家"),
        ("600000.SS","浦发银行","上海国资背景"),
        ("601998.SS","中信银行","对公/投行特色"),
        ("601818.SS","光大银行","光大集团背景"),
    ],
    "金融科技": [
        ("300059.SZ","东方财富","互联网券商+基金销售龙头"),
        ("300033.SZ","同花顺","金融信息服务商龙头"),
        ("600570.SS","恒生电子","金融IT系统龙头"),
        ("000948.SZ","南天信息","银行IT解决方案"),
        ("300348.SZ","长亮科技","银行核心系统"),
        ("300339.SZ","润和软件","金融科技+开源鸿蒙"),
        ("300377.SZ","赢时胜","资管IT系统"),
        ("300380.SZ","安硕信息","信贷管理系统"),
        ("300674.SZ","宇信科技","银行IT解决方案"),
        ("600536.SS","中国软件","国产操作系统核心"),
    ],
    # ── 消费 ──
    "白酒": [
        ("600519.SS","贵州茅台","白酒绝对龙头"),
        ("000858.SZ","五粮液","浓香型白酒龙头"),
        ("000568.SZ","泸州老窖","高端白酒三强"),
        ("600809.SS","山西汾酒","清香型白酒龙头"),
        ("002304.SZ","洋河股份","苏酒龙头"),
        ("000596.SZ","古井贡酒","徽酒龙头"),
        ("600702.SS","舍得酒业","次高端白酒"),
        ("603369.SS","今世缘","江苏区域名酒"),
        ("600779.SS","水井坊","外资控股高端白酒"),
        ("000860.SZ","顺鑫农业","牛栏山二锅头"),
    ],
    "食品饮料": [
        ("603288.SS","海天味业","调味品龙头"),
        ("600887.SS","伊利股份","乳制品龙头"),
        ("002714.SZ","牧原股份","生猪养殖龙头"),
        ("300498.SZ","温氏股份","黄鸡+生猪养殖"),
        ("600298.SS","安琪酵母","酵母全球第二"),
        ("002507.SZ","涪陵榨菜","榨菜细分龙头"),
        ("603517.SS","绝味食品","休闲卤味龙头"),
        ("600872.SS","中炬高新","酱油第二梯队"),
        ("300999.SZ","金龙鱼","粮油龙头"),
        ("002557.SZ","洽洽食品","瓜子坚果龙头"),
    ],
    "大消费": [
        ("600519.SS","贵州茅台","白酒绝对龙头"),
        ("000858.SZ","五粮液","浓香型白酒龙头"),
        ("600887.SS","伊利股份","乳制品龙头"),
        ("603288.SS","海天味业","调味品龙头"),
        ("000333.SZ","美的集团","家电综合龙头"),
        ("000651.SZ","格力电器","空调龙头"),
        ("002714.SZ","牧原股份","生猪养殖龙头"),
        ("600690.SS","海尔智家","全球化家电龙头"),
        ("002568.SZ","百润股份","预调鸡尾酒龙头"),
        ("603195.SS","公牛集团","民用电工龙头"),
    ],
    "家电": [
        ("000333.SZ","美的集团","家电综合龙头"),
        ("000651.SZ","格力电器","空调龙头"),
        ("600690.SS","海尔智家","全球化家电龙头"),
        ("002032.SZ","苏泊尔","小家电龙头"),
        ("603486.SS","科沃斯","扫地机器人龙头"),
        ("688169.SS","石头科技","扫地机器人出海"),
        ("002242.SZ","九阳股份","豆浆机/小家电"),
        ("002508.SZ","老板电器","厨电龙头"),
        ("603868.SS","飞科电器","个人护理电器"),
        ("300911.SZ","亿田智能","集成灶"),
    ],
    "旅游酒店": [
        ("600009.SS","上海机场","国际航空枢纽"),
        ("601888.SS","中国中免","免税龙头"),
        ("600754.SS","锦江酒店","国内酒店龙头"),
        ("600258.SS","首旅酒店","酒店第二梯队"),
        ("002707.SZ","众信旅游","出境游龙头"),
        ("000524.SZ","岭南控股","旅行社+酒店"),
        ("600138.SS","中青旅","旅行社+景区"),
        ("002033.SZ","丽江股份","丽江景区运营"),
        ("600054.SS","黄山旅游","黄山景区运营"),
        ("002159.SZ","三特索道","索道运营"),
    ],
    "传媒": [
        ("002027.SZ","分众传媒","楼宇媒体龙头"),
        ("300413.SZ","芒果超媒","长视频平台"),
        ("601928.SS","凤凰传媒","出版发行龙头"),
        ("600373.SS","中文传媒","出版+游戏"),
        ("601098.SS","中南传媒","教育出版龙头"),
        ("601801.SS","皖新传媒","安徽出版发行"),
        ("300133.SZ","华策影视","电视剧制作龙头"),
        ("600088.SS","中视传媒","央视背景传媒"),
        ("601949.SS","中国出版","国家级出版"),
        ("603533.SS","掌阅科技","数字阅读平台"),
    ],
    # ── 医疗 ──
    "医疗服务": [
        ("300760.SZ","迈瑞医疗","医疗器械龙头"),
        ("603259.SS","药明康德","CXO全球龙头"),
        ("600276.SS","恒瑞医药","创新药龙头"),
        ("300015.SZ","爱尔眼科","眼科连锁龙头"),
        ("300896.SZ","爱美客","医美注射剂龙头"),
        ("002001.SZ","新和成","原料药/维生素龙头"),
        ("600436.SS","片仔癀","中药稀缺品种"),
        ("000538.SZ","云南白药","中药品牌龙头"),
        ("600196.SS","复星医药","综合医药集团"),
        ("300003.SZ","乐普医疗","心血管器械"),
    ],
    "生物医药": [
        ("603259.SS","药明康德","CXO全球龙头"),
        ("600276.SS","恒瑞医药","创新药龙头"),
        ("000661.SZ","长春高新","生长激素龙头"),
        ("300122.SZ","智飞生物","疫苗代理+自研"),
        ("300142.SZ","沃森生物","mRNA疫苗"),
        ("688185.SS","康希诺","疫苗研发"),
        ("300601.SZ","康泰生物","疫苗平台型"),
        ("603392.SS","万泰生物","HPV疫苗"),
        ("688520.SS","神州细胞","生物药研发"),
        ("300841.SZ","康华生物","狂犬病疫苗"),
    ],
    "创新药": [
        ("600276.SS","恒瑞医药","创新药龙头"),
        ("688235.SS","百济神州","全球化创新药"),
        ("688266.SS","泽璟制药","小分子创新药"),
        ("688302.SS","海创药业","创新药研发"),
        ("688176.SS","亚虹医药","泌尿生殖系统创新药"),
        ("688197.SS","首药控股","小分子创新药"),
        ("688578.SS","艾力斯","肺癌靶向药"),
        ("688331.SS","荣昌生物","ADC药物"),
        ("688062.SS","迈威生物","生物创新药"),
        ("688443.SS","智翔金泰","抗体药物"),
    ],
    "医疗器械": [
        ("300760.SZ","迈瑞医疗","医疗器械龙头"),
        ("688271.SS","联影医疗","医学影像设备"),
        ("300003.SZ","乐普医疗","心血管器械"),
        ("688016.SS","心脉医疗","血管介入器械"),
        ("688198.SS","佰仁医疗","动物源性植介入"),
        ("300326.SZ","凯利泰","骨科微创器械"),
        ("300453.SZ","三鑫医疗","血液净化器械"),
        ("688289.SS","圣湘生物","分子诊断"),
        ("300482.SZ","万孚生物","POCT快速诊断"),
        ("688575.SS","亚辉龙","化学发光诊断"),
    ],
    "中药": [
        ("600436.SS","片仔癀","中药稀缺品种"),
        ("000538.SZ","云南白药","中药品牌龙头"),
        ("600085.SS","同仁堂","中药老字号"),
        ("000999.SZ","华润三九","中药OTC龙头"),
        ("600332.SS","白云山","南派中药"),
        ("600535.SS","天士力","现代中药"),
        ("600976.SS","健民集团","儿科中药"),
        ("603896.SS","寿仙谷","灵芝孢子粉"),
        ("300181.SZ","佐力药业","乌灵胶囊"),
        ("600771.SS","广誉远","中药老字号"),
    ],
    # ── 新能源/材料 ──
    "光伏": [
        ("601012.SS","隆基绿能","单晶硅片龙头"),
        ("600438.SS","通威股份","硅料+电池片龙头"),
        ("002459.SZ","晶澳科技","一体化光伏组件"),
        ("688599.SS","天合光能","光伏组件龙头"),
        ("601865.SS","福莱特","光伏玻璃龙头"),
        ("603806.SS","福斯特","光伏胶膜龙头"),
        ("300274.SZ","阳光电源","光伏逆变器龙头"),
        ("688223.SS","晶科能源","N型组件龙头"),
        ("002129.SZ","TCL中环","硅片双寡头"),
        ("600732.SS","爱旭股份","ABC电池"),
    ],
    "新能源车": [
        ("002594.SZ","比亚迪","新能源汽车全球龙头"),
        ("300750.SZ","宁德时代","动力电池全球龙头"),
        ("601127.SS","赛力斯","华为智选车"),
        ("002050.SZ","三花智控","热管理龙头"),
        ("002709.SZ","天赐材料","电解液龙头"),
        ("603659.SS","璞泰来","负极材料+隔膜"),
        ("300014.SZ","亿纬锂能","锂原电池+动力电池"),
        ("688005.SS","容百科技","三元正极材料"),
        ("002812.SZ","恩捷股份","锂电池隔膜龙头"),
        ("603993.SS","洛阳钼业","钴镍资源"),
    ],
    "动力电池": [
        ("300750.SZ","宁德时代","动力电池全球龙头"),
        ("300014.SZ","亿纬锂能","锂原电池+动力电池"),
        ("002709.SZ","天赐材料","电解液龙头"),
        ("002812.SZ","恩捷股份","锂电池隔膜龙头"),
        ("603659.SS","璞泰来","负极材料+隔膜"),
        ("688005.SS","容百科技","三元正极材料"),
        ("300073.SZ","当升科技","正极材料"),
        ("002074.SZ","国轩高科","动力电池第二梯队"),
        ("300919.SZ","中伟股份","三元前驱体"),
        ("688567.SS","孚能科技","软包动力电池"),
    ],
    "有色金属": [
        ("601899.SS","紫金矿业","金铜锌综合矿业龙头"),
        ("603993.SS","洛阳钼业","铜钴钼综合矿业"),
        ("600362.SS","江西铜业","国内铜业龙头"),
        ("000878.SZ","云南铜业","铜冶炼"),
        ("600547.SS","山东黄金","黄金矿业龙头"),
        ("600489.SS","中金黄金","央企黄金龙头"),
        ("601600.SS","中国铝业","铝业龙头"),
        ("002460.SZ","赣锋锂业","锂盐龙头"),
        ("002466.SZ","天齐锂业","锂矿资源龙头"),
        ("603799.SS","华友钴业","钴镍新能源材料"),
    ],
    "稀土": [
        ("600111.SS","北方稀土","轻稀土龙头"),
        ("600259.SS","广晟有色","中重稀土"),
        ("600392.SS","盛和资源","稀土冶炼分离"),
        ("000831.SZ","中国稀土","中重稀土整合平台"),
        ("600549.SS","厦门钨业","钨+稀土"),
        ("600010.SS","包钢股份","稀土精矿"),
        ("000612.SZ","焦作万方","电解铝+稀土"),
        ("600366.SS","宁波韵升","稀土永磁材料"),
        ("300748.SZ","金力永磁","高性能钕铁硼"),
        ("688077.SS","大地熊","烧结钕铁硼"),
    ],
    "钢铁": [
        ("600019.SS","宝钢股份","国内钢铁龙头"),
        ("000932.SZ","华菱钢铁","湖南钢铁龙头"),
        ("600808.SS","马钢股份","安徽钢铁"),
        ("000709.SZ","河钢股份","河北钢铁"),
        ("600022.SS","山东钢铁","山东钢铁整合"),
        ("600282.SS","南钢股份","特钢+板材"),
        ("000898.SZ","鞍钢股份","东北钢铁龙头"),
        ("600010.SS","包钢股份","稀土+钢铁"),
        ("601003.SS","柳钢股份","广西钢铁"),
        ("600507.SS","方大特钢","弹簧扁钢"),
    ],
    "煤炭": [
        ("601088.SS","中国神华","煤炭+电力一体化龙头"),
        ("601225.SS","陕西煤业","优质动力煤"),
        ("600188.SS","兖矿能源","国际化煤企"),
        ("601699.SS","潞安环能","喷吹煤龙头"),
        ("600123.SS","兰花科创","无烟煤"),
        ("600395.SS","盘江股份","西南煤炭"),
        ("000552.SZ","甘肃能化","甘肃煤炭"),
        ("600971.SS","恒源煤电","安徽煤炭"),
        ("601015.SS","陕西黑猫","焦化"),
        ("600408.SS","红阳能源","辽宁煤炭"),
    ],
    "化工": [
        ("600309.SS","万华化学","MDI全球龙头"),
        ("002648.SZ","卫星化学","C2/C3轻烃化工"),
        ("600426.SS","华鲁恒升","煤化工龙头"),
        ("002493.SZ","荣盛石化","民营炼化龙头"),
        ("000703.SZ","恒逸石化","PTA-聚酯龙头"),
        ("600346.SS","恒力石化","炼化+新材料"),
        ("601233.SS","桐昆股份","涤纶长丝龙头"),
        ("603225.SS","新凤鸣","涤纶长丝"),
        ("002001.SZ","新和成","维生素/香精香料"),
        ("600486.SS","扬农化工","农药原药龙头"),
    ],
    # ── 工业制造 ──
    "军工": [
        ("600893.SS","航发动力","航空发动机龙头"),
        ("600760.SS","中航沈飞","战斗机整机"),
        ("000768.SZ","中航西飞","大中型运输机"),
        ("600372.SS","中航机载","航空机载系统"),
        ("600391.SS","航发科技","航空发动机零部件"),
        ("600879.SS","航天电子","航天电子配套"),
        ("002179.SZ","中航光电","军用连接器龙头"),
        ("300114.SZ","中航电测","军工传感器"),
        ("600435.SS","北方导航","导航控制"),
        ("000519.SZ","中兵红箭","智能弹药+培育钻石"),
    ],
    "高端制造": [
        ("601766.SS","中国中车","轨道交通装备全球龙头"),
        ("600031.SS","三一重工","工程机械龙头"),
        ("000425.SZ","徐工机械","工程机械第二"),
        ("601100.SS","恒立液压","液压件龙头"),
        ("300124.SZ","汇川技术","工业自动化龙头"),
        ("002008.SZ","大族激光","激光加工设备"),
        ("688017.SS","绿的谐波","谐波减速器"),
        ("688305.SS","科德数控","五轴联动数控机床"),
        ("300607.SZ","拓斯达","工业机器人"),
        ("688698.SS","伟创电气","变频器/伺服系统"),
    ],
    "机械设备": [
        ("600031.SS","三一重工","工程机械龙头"),
        ("000425.SZ","徐工机械","工程机械第二"),
        ("000157.SZ","中联重科","工程机械第三"),
        ("601100.SS","恒立液压","液压件龙头"),
        ("603338.SS","浙江鼎力","高空作业平台"),
        ("600761.SS","安徽合力","叉车龙头"),
        ("603298.SS","杭叉集团","叉车第二"),
        ("600835.SS","上海机电","电梯龙头"),
        ("002367.SZ","康力电梯","民族电梯品牌"),
        ("300091.SZ","金通灵","流体机械"),
    ],
    "基建": [
        ("601668.SS","中国建筑","全球最大建筑公司"),
        ("601390.SS","中国中铁","铁路基建龙头"),
        ("601186.SS","中国铁建","铁路基建第二"),
        ("601800.SS","中国交建","港口/公路基建"),
        ("601618.SS","中国中冶","冶金工程+资源开发"),
        ("601669.SS","中国电建","水利水电龙头"),
        ("601117.SS","中国化学","化工工程"),
        ("600970.SS","中材国际","水泥工程全球第一"),
        ("601868.SS","中国能建","能源电力建设"),
        ("600820.SS","隧道股份","上海隧道工程"),
    ],
    "物流": [
        ("002352.SZ","顺丰控股","快递龙头"),
        ("600233.SS","圆通速递","快递第二"),
        ("002120.SZ","韵达股份","快递第三"),
        ("002468.SZ","申通快递","快递第四"),
        ("603056.SS","德邦股份","大件快递"),
        ("600057.SS","厦门象屿","供应链物流"),
        ("600787.SS","中储股份","仓储物流"),
        ("603128.SS","华贸物流","国际货代"),
        ("002010.SZ","传化智联","公路港物流"),
        ("600153.SS","建发股份","供应链+地产"),
    ],
    # ── 房地产 ──
    "房地产": [
        ("000002.SZ","万科A","房地产开发龙头"),
        ("600048.SS","保利发展","央企地产龙头"),
        ("001979.SZ","招商蛇口","园区开发+地产"),
        ("600606.SS","绿地控股","综合性地产"),
        ("600383.SS","金地集团","稳健型房企"),
        ("601155.SS","新城控股","住宅+商业"),
        ("000961.SZ","中南建设","建筑+地产"),
        ("600340.SS","华夏幸福","产业新城"),
        ("000656.SZ","金科股份","西南房企"),
        ("600325.SS","华发股份","珠海国资地产"),
    ],
    # ── 公用事业 ──
    "电力": [
        ("600900.SS","长江电力","水电龙头"),
        ("600011.SS","华能国际","火电龙头"),
        ("600795.SS","国电电力","央企电力"),
        ("601985.SS","中国核电","核电龙头"),
        ("003816.SZ","中国广核","核电运营"),
        ("600886.SS","国投电力","水电+火电"),
        ("600674.SS","川投能源","雅砻江水电"),
        ("600023.SS","浙能电力","浙江火电"),
        ("600027.SS","华电国际","山东火电"),
        ("601016.SS","节能风电","风电运营"),
    ],
    "环保": [
        ("600323.SS","瀚蓝环境","固废处理龙头"),
        ("601330.SS","绿色动力","垃圾焚烧发电"),
        ("002034.SZ","旺能环境","垃圾焚烧"),
        ("603588.SS","高能环境","危废处理"),
        ("300070.SZ","碧水源","水处理膜技术"),
        ("300266.SZ","兴源环境","水处理工程"),
        ("300137.SZ","先河环保","环境监测"),
        ("603568.SS","伟明环保","垃圾焚烧设备"),
        ("002672.SZ","东江环保","危废处理"),
        ("300422.SZ","博世科","水处理+土壤修复"),
    ],
}
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS sector_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, symbol TEXT, name TEXT,
            category TEXT, open REAL, high REAL, low REAL, close REAL,
            change_pct REAL, volume INTEGER, fetched_at TEXT DEFAULT (datetime('now')),
            UNIQUE(date, symbol))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS daily_summary (
            date TEXT PRIMARY KEY, total_sectors INTEGER, up_sectors INTEGER,
            down_sectors INTEGER, best_sector TEXT, best_pct REAL,
            worst_sector TEXT, worst_pct REAL, spy_change REAL)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS access_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ip TEXT, path TEXT,
            timestamp TEXT DEFAULT (datetime('now')))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS backtest_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT, strategy TEXT, symbol TEXT,
            start_date TEXT, end_date TEXT, total_return REAL, sharpe REAL,
            max_drawdown REAL, win_rate REAL, trades INTEGER,
            params TEXT, created_at TEXT DEFAULT (datetime('now')))''')
        # Add indexes for performance
        conn.execute('CREATE INDEX IF NOT EXISTS idx_sector_date ON sector_data(date)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_sector_sym ON sector_data(symbol)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_sector_cat ON sector_data(category)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_sector_date_sym ON sector_data(date, symbol)')
        conn.commit()

def init_cn_db():
    with sqlite3.connect(CN_DB_PATH) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS sector_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, symbol TEXT, name TEXT,
            category TEXT, open REAL, high REAL, low REAL, close REAL,
            change_pct REAL, volume INTEGER, fetched_at TEXT DEFAULT (datetime('now')),
            UNIQUE(date, symbol))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS daily_summary (
            date TEXT PRIMARY KEY, total_sectors INTEGER, up_sectors INTEGER,
            down_sectors INTEGER, best_sector TEXT, best_pct REAL,
            worst_sector TEXT, worst_pct REAL, spy_change REAL)''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_cn_sector_date ON sector_data(date)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_cn_sector_sym ON sector_data(symbol)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_cn_sector_cat ON sector_data(category)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_cn_sector_date_sym ON sector_data(date, symbol)')
        # Auto-migration: if old data with 'ETF' suffix exists, clear it
        try:
            old = conn.execute("SELECT COUNT(*) FROM sector_data WHERE name LIKE '%ETF'").fetchone()[0]
            if old > 0:
                conn.execute("DELETE FROM sector_data")
                conn.execute("DELETE FROM daily_summary")
        except: pass
        conn.commit()

# ── Auto-seed A-share DB from bundled backup ─────────────────
def auto_seed_cn_db():
    backup_path = os.path.join(os.path.dirname(__file__), 'static', 'cn_sectors.db.bak')
    if not os.path.exists(backup_path):
        print("  ⚠️ CN DB backup not found at", backup_path)
        return
    try:
        import shutil
        shutil.copy(backup_path, CN_DB_PATH)
        print(f"  ✅ Restored cn_sectors.db from backup ({os.path.getsize(backup_path)} bytes)")
    except Exception as e:
        print(f"  ⚠️ CN DB restore error: {e}")

auto_seed_cn_db()

# ── Security Middleware ──────────────────────────────────────
@app.before_request
def security():
    ip = request.remote_addr
    now = datetime.now()
    if not request.path.startswith('/static'):
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("INSERT INTO access_log (ip, path) VALUES (?, ?)", (ip, request.path))
    mk = f"{ip}:{now.strftime('%Y%m%d%H%M')}"
    rate_store[mk] = rate_store.get(mk, 0) + 1
    if rate_store[mk] > MAX_REQUESTS: abort(429)
    if len(rate_store) > 10000:
        for k in list(rate_store):
            if k.split(':')[1] < now.strftime('%Y%m%d%H%M'): del rate_store[k]

@app.after_request
def headers(resp):
    resp.headers['X-Content-Type-Options'] = 'nosniff'
    resp.headers['X-Frame-Options'] = 'DENY'
    resp.headers['X-XSS-Protection'] = '1; mode=block'
    resp.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    resp.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://s.tradingview.com; style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; frame-src https://s.tradingview.com; img-src 'self' data: https://s.tradingview.com"
    return resp

# ── Data Fetching ────────────────────────────────────────────
def fetch_sector_data(target_date=None):
    """Fetch US ETF data from Yahoo Finance (more reliable than stockanalysis)."""
    results = []
    for category, symbol, name in SUB_SECTORS:
        try:
            url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"
            resp = requests.get(url, headers=YF_HEADERS, timeout=15)
            if resp.status_code != 200: continue
            data = resp.json()
            result = data.get('chart', {}).get('result', [None])[0]
            if not result: continue
            timestamps = result.get('timestamp', [])
            quotes = result.get('indicators', {}).get('quote', [{}])[0]
            if len(timestamps) < 2: continue
            close_prices = quotes.get('close', [])
            open_prices = quotes.get('open', [])
            highs = quotes.get('high', [])
            lows = quotes.get('low', [])
            vols = quotes.get('volume', [])
            # Get last two valid trading days
            valid = [(i, close_prices[i]) for i in range(len(close_prices)-1, -1, -1) if close_prices[i] is not None]
            if len(valid) < 2: continue
            latest_idx = valid[0][0]; prev_idx = valid[1][0]
            close = close_prices[latest_idx]; prev_close = close_prices[prev_idx]
            change_pct = ((close - prev_close) / prev_close) * 100 if prev_close else 0
            results.append({
                'symbol': symbol, 'name': name, 'category': category,
                'open': round(open_prices[latest_idx] if open_prices[latest_idx] else 0, 2),
                'high': round(highs[latest_idx] if highs[latest_idx] else 0, 2),
                'low': round(lows[latest_idx] if lows[latest_idx] else 0, 2),
                'close': round(close, 2),
                'change_pct': round(change_pct, 2),
                'volume': int(vols[latest_idx] if vols[latest_idx] else 0),
            })
            time.sleep(0.2)
        except Exception as e:
            app.logger.warning(f"Fetch fail {symbol}: {e}")
    return results

def fetch_stock_quotes(stock_list):
    """Fetch current stock prices including after-hours via 1-minute Yahoo data."""
    results = []
    for item in stock_list:
        sym, name, desc = item[0], item[1] if len(item)>1 else sym, item[2] if len(item)>2 else ""
        try:
            current_price = None; prev_close = None
            # Always get previous close from daily data (most accurate)
            url_d = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=5d"
            resp_d = requests.get(url_d, headers=YF_HEADERS, timeout=10)
            if resp_d.status_code == 200:
                data_d = resp_d.json()
                r_d = data_d.get('chart', {}).get('result', [None])[0]
                if r_d:
                    cd = r_d.get('indicators', {}).get('quote', [{}])[0].get('close', [])
                    vd = [c for c in cd if c is not None]
                    if len(vd) >= 1: prev_close = vd[-1]  # latest daily close as baseline
            # Get current price from 1-minute data (includes after-hours)
            url_1m = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1m&range=1d&includePrePost=true"
            resp = requests.get(url_1m, headers=YF_HEADERS, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                r = data.get('chart', {}).get('result', [None])[0]
                if r:
                    c1m = r.get('indicators', {}).get('quote', [{}])[0].get('close', [])
                    v1m = [c for c in c1m if c is not None]
                    if v1m: current_price = v1m[-1]
            # Fallback: use daily close if no 1m data
            if not current_price and vd: current_price = vd[-1]
            if current_price and prev_close and prev_close > 0:
                results.append({
                    'symbol': sym, 'name': name, 'desc': desc,
                    'close': round(current_price, 2),
                    'change_pct': round((current_price - prev_close) / prev_close * 100, 2),
                    'volume': 0,
                })
            time.sleep(0.1)
        except: pass
    return sorted(results, key=lambda x: x['change_pct'], reverse=True)

def save_to_db(data, target_date):
    with sqlite3.connect(DB_PATH) as conn:
        for row in data:
            conn.execute('''INSERT OR REPLACE INTO sector_data
                (date, symbol, name, category, open, high, low, close, change_pct, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (target_date, row['symbol'], row['name'], row['category'],
                 row['open'], row['high'], row['low'], row['close'],
                 row['change_pct'], row['volume']))
        sectors_only = [r for r in data if r['category'] != '指数']
        up = sum(1 for r in sectors_only if r['change_pct'] > 0)
        down = sum(1 for r in sectors_only if r['change_pct'] < 0)
        best = max(sectors_only, key=lambda r: r['change_pct']) if sectors_only else None
        worst = min(sectors_only, key=lambda r: r['change_pct']) if sectors_only else None
        spy = next((r for r in data if r['symbol'] == 'SPY'), None)
        conn.execute('''INSERT OR REPLACE INTO daily_summary VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (target_date, len(sectors_only), up, down,
             best['name'] if best else None, best['change_pct'] if best else None,
             worst['name'] if worst else None, worst['change_pct'] if worst else None,
             spy['change_pct'] if spy else None))
        conn.commit()

# ── China A-Share Data Fetching ──────────────────────────────
def fetch_cn_sector_data(target_date=None):
    """Fetch A-share ETF data from Yahoo Finance."""
    results = []
    fail_count = 0
    for category, symbol, name in CN_SECTORS:
        try:
            # Use full symbol with suffix for Yahoo Finance CN ETFs
            url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"
            resp = requests.get(url, headers=YF_HEADERS, timeout=12)
            if resp.status_code != 200:
                fail_count += 1; continue
            data = resp.json()
            r = data.get('chart', {}).get('result', [None])[0]
            if not r or 'timestamp' not in r:
                fail_count += 1; continue
            timestamps = r.get('timestamp', [])
            quotes = r.get('indicators', {}).get('quote', [{}])[0]
            if len(timestamps) < 2: continue
            closes = quotes.get('close', [])
            opens = quotes.get('open', [])
            highs = quotes.get('high', [])
            lows = quotes.get('low', [])
            vols = quotes.get('volume', [])
            valid = [(i, closes[i]) for i in range(len(closes)-1, -1, -1) if closes[i] is not None]
            if len(valid) < 2: continue
            latest_idx, prev_idx = valid[0][0], valid[1][0]
            latest_close = closes[latest_idx]
            prev_close = closes[prev_idx]
            change_pct = ((latest_close - prev_close) / prev_close) * 100 if prev_close else 0
            results.append({
                'symbol': symbol, 'name': name, 'category': category,
                'open': round(opens[latest_idx] if opens[latest_idx] else 0, 2),
                'high': round(highs[latest_idx] if highs[latest_idx] else 0, 2),
                'low': round(lows[latest_idx] if lows[latest_idx] else 0, 2),
                'close': round(latest_close, 2),
                'change_pct': round(change_pct, 2),
                'volume': int(vols[latest_idx] if vols[latest_idx] else 0),
            })
            time.sleep(0.15)
        except Exception as e:
            fail_count += 1
            app.logger.warning(f"CN Fetch fail {symbol}: {e}")
    if fail_count > 0:
        print(f"[Data] CN fetch: {len(results)} ok, {fail_count} failed out of {len(CN_SECTORS)}")
    return results

def fetch_cn_stock_quotes(stock_list):
    """Fetch A-share stock quotes from Yahoo Finance."""
    results = []
    for item in stock_list:
        sym, name, desc = item[0], item[1] if len(item)>1 else sym, item[2] if len(item)>2 else ""
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=5d"
            resp = requests.get(url, headers=YF_HEADERS, timeout=10)
            if resp.status_code != 200: continue
            data = resp.json()
            result = data.get('chart', {}).get('result', [None])[0]
            if not result: continue
            quotes = result.get('indicators', {}).get('quote', [{}])[0]
            close = quotes.get('close', [])
            volume = quotes.get('volume', [])
            if len(close) < 2 or close[-1] is None or close[-2] is None: continue
            change_pct = ((close[-1] - close[-2]) / close[-2]) * 100 if close[-2] else 0
            results.append({
                'symbol': sym, 'name': name, 'desc': desc,
                'close': round(close[-1], 2), 'change_pct': round(change_pct, 2),
                'volume': int(volume[-1] or 0),
            })
            time.sleep(0.1)
        except: pass
    return sorted(results, key=lambda x: x['change_pct'], reverse=True)

def save_cn_to_db(data, target_date):
    with sqlite3.connect(CN_DB_PATH) as conn:
        for row in data:
            conn.execute('''INSERT OR REPLACE INTO sector_data
                (date, symbol, name, category, open, high, low, close, change_pct, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (target_date, row['symbol'], row['name'], row['category'],
                 row['open'], row['high'], row['low'], row['close'],
                 row['change_pct'], row['volume']))
        sectors_only = [r for r in data if r['category'] != '指数']
        up = sum(1 for r in sectors_only if r['change_pct'] > 0)
        down = sum(1 for r in sectors_only if r['change_pct'] < 0)
        best = max(sectors_only, key=lambda r: r['change_pct']) if sectors_only else None
        worst = min(sectors_only, key=lambda r: r['change_pct']) if sectors_only else None
        spy = next((r for r in data if r['symbol'] == '510300.SS'), None)  # 沪深300 as benchmark
        conn.execute('''INSERT OR REPLACE INTO daily_summary VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (target_date, len(sectors_only), up, down,
             best['name'] if best else None, best['change_pct'] if best else None,
             worst['name'] if worst else None, worst['change_pct'] if worst else None,
             spy['change_pct'] if spy else None))
        conn.commit()

def daily_fetch_job():
    """美股数据抓取 - 当日数据"""
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"[Scheduler] US fetch for {today}")
    data = fetch_sector_data(today)
    if data:
        save_to_db(data, today)
        print(f"[Scheduler] US saved {len(data)} sectors for {today}")

def cn_daily_fetch_job():
    """A股数据抓取 - 当日数据"""
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"[Scheduler] CN fetch for {today}")
    data = fetch_cn_sector_data(today)
    if data:
        save_cn_to_db(data, today)
        print(f"[Scheduler] CN saved {len(data)} sectors for {today}")

def _warmup_kol_cache():
    """Prefetch KOL tweets and fetch latest data at startup."""
    print("[KOL] Warming up KOL tweet cache...")
    try:
        us_tweets = _fetch_all_kol_tweets('us')
        cn_tweets = _fetch_all_kol_tweets('cn')
        print(f"[KOL] Cache warmed: {len(us_tweets)} US + {len(cn_tweets)} CN tweets")
        with _news_cache_lock:
            _news_cache.pop('us', None)
            _news_cache.pop('cn', None)
    except Exception as e:
        print(f"[KOL] Cache warmup failed: {e}")
    # Also try to fetch latest market data at startup
    print("[Data] Fetching latest market data...")
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        us_data = fetch_sector_data(today)
        if us_data:
            save_to_db(us_data, today)
            print(f"[Data] US: {len(us_data)} sectors saved for {today}")
        cn_data = fetch_cn_sector_data(today)
        if cn_data:
            save_cn_to_db(cn_data, today)
            print(f"[Data] CN: {len(cn_data)} sectors saved for {today}")
    except Exception as e:
        print(f"[Data] Startup fetch failed: {e}")

def _refresh_kol_cache():
    """Periodic refresh of KOL tweet cache."""
    print("[KOL] Refreshing KOL tweet cache...")
    try:
        with _news_cache_lock:
            _kol_cache.pop('us', None)
            _kol_cache.pop('cn', None)
            _news_cache.pop('us', None)
            _news_cache.pop('cn', None)
        _fetch_all_kol_tweets('us')
        _fetch_all_kol_tweets('cn')
        print("[KOL] Cache refreshed")
    except Exception as e:
        print(f"[KOL] Cache refresh failed: {e}")

def _refresh_monthly_reports():
    """Clear technicals cache on 1st of each month so stock reports regenerate."""
    print("[Report] Monthly refresh: clearing stock report cache...")
    with _news_cache_lock:
        count = len(_technicals_cache)
        _technicals_cache.clear()
    print(f"[Report] Cleared {count} cached reports for monthly refresh")

# ── Smart Stock Screener ──
_screener_cache = {'ts': 0, 'results': []}

def _quick_score_stock(symbol, name, sector, closes, vols=None):
    """Score on: low RSI + high Sharpe + MACD uptrend + volume expansion + BB proximity."""
    n = len(closes)
    if n < 60: return None

    # Daily returns
    rets = [(closes[i]-closes[i-1])/closes[i-1]*100 for i in range(1,n) if closes[i-1]>0]
    if len(rets) < 50: return None
    m = len(rets)

    # RSI(14)
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i]-closes[i-1]; gains.append(max(d,0)); losses.append(max(-d,0))
    avg_g = sum(gains[-14:])/14; avg_l = sum(losses[-14:])/14
    rs = avg_g/avg_l if avg_l>0 else 100
    rsi = round(100-100/(1+rs), 1)

    # Sharpe proxy (annualized)
    mu_d = sum(rets)/m; sigma_d = (sum((r-mu_d)**2 for r in rets)/m)**0.5
    sharpe = round((mu_d*252 - 4.0)/(sigma_d*(252**0.5)), 2) if sigma_d>0 else 0

    # MACD(12,26,9)
    def ema(data, period):
        k=2/(period+1); r=[data[0]]
        for i in range(1,len(data)): r.append(data[i]*k+r[-1]*(1-k))
        return r
    e12=ema(closes,12); e26=ema(closes,26)
    macd=[e12[i]-e26[i] for i in range(n)]
    sig=ema(macd,9)
    hist=[macd[i]-sig[i] for i in range(n)]

    macd_now = macd[-1]; sig_now = sig[-1]; hist_now = hist[-1]
    macd_uptrend = macd_now > sig_now and hist_now > 0
    # Check if MACD is at extreme highs
    hist_max = max(abs(h) for h in hist[-60:]) if len(hist)>=60 else max(abs(h) for h in hist)
    macd_extreme = abs(hist_now) > hist_max * 0.9 if hist_max > 0 else False

    # Scoring
    score = 0
    # RSI: lower is better for buying opportunity
    if rsi < 30: score += 30
    elif rsi < 40: score += 20
    elif rsi < 50: score += 10

    # Sharpe: higher is better
    if sharpe > 2.0: score += 30
    elif sharpe > 1.0: score += 20
    elif sharpe > 0.5: score += 10
    elif sharpe < 0: score -= 10

    # MACD: uptrend + not extreme
    if macd_uptrend and not macd_extreme: score += 20
    elif macd_uptrend: score += 10

    # Volume expansion: current vol vs 20-day avg
    vol_exp = 1.0
    if vols and len(vols) >= 20:
        vol_20_avg = sum(v or 0 for v in vols[-21:-1]) / 20 if len(vols) >= 21 else 0
        vol_now = vols[-1] or 0
        if vol_20_avg > 0:
            vol_exp = round(vol_now / vol_20_avg, 2)

    # Bollinger Band position
    bb_lower = None; bb_mid = None; bb_pos_pct = None
    if n >= 20:
        w = closes[-20:]
        ma20 = sum(w)/20; std20 = (sum((x-ma20)**2 for x in w)/20)**0.5
        bb_lower = round(ma20 - 2*std20, 2)
        bb_mid = round(ma20, 2)
        if (2*std20) > 0:
            bb_pos_pct = round((closes[-1] - bb_lower) / (2*std20) * 100, 1)
            # Clamp to 0-100 range for display
            bb_pos_pct = max(0, min(100, bb_pos_pct))

    # Volume expansion scoring
    if vol_exp > 2.0: score += 20      # 2x+ avg volume → strong interest
    elif vol_exp > 1.5: score += 15    # 50%+ above avg → notable
    elif vol_exp > 1.2: score += 10    # slightly elevated

    # BB lower band proximity scoring
    if bb_pos_pct is not None:
        if bb_pos_pct < 10: score += 25       # price near/at lower band → oversold bounce
        elif bb_pos_pct < 20: score += 15     # approaching lower band
        elif bb_pos_pct < 30: score += 10     # in lower third

    return {
        'symbol': symbol, 'name': name, 'sector': sector,
        'rsi': rsi, 'sharpe': sharpe,
        'macd_uptrend': macd_uptrend, 'macd_extreme': macd_extreme,
        'macd_hist': round(hist_now, 6),
        'vol_exp': vol_exp, 'bb_pos_pct': bb_pos_pct,
        'score': score, 'close': round(closes[-1], 2)
    }

def _run_screener(market='us'):
    """Scan sector stocks and return top picks."""
    results = []
    symbols_processed = set()
    sectors = SECTOR_STOCKS if market != 'cn' else CN_SECTOR_STOCKS

    for sector_name, stocks in sectors.items():
        for info in stocks[:3]:
            sym = info[0]; name = info[1]
            if sym in symbols_processed: continue
            symbols_processed.add(sym)
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=6mo"
                resp = requests.get(url, headers=YF_HEADERS, timeout=10)
                if resp.status_code != 200: continue
                data = resp.json()
                r = data.get('chart',{}).get('result',[None])[0]
                if not r: continue
                quotes = r.get('indicators',{}).get('quote',[{}])[0]
                closes = [c for c in quotes.get('close',[]) if c is not None]
                vols = quotes.get('volume', [])
                if len(closes) < 60: continue
                scored = _quick_score_stock(sym, name, sector_name, closes, vols)
                if scored: results.append(scored)
                time.sleep(0.1)
            except: continue

    filtered = [r for r in results if r['rsi'] < 55 and r['sharpe'] > 0 and r['macd_uptrend']]
    filtered.sort(key=lambda x: x['score'], reverse=True)
    return filtered[:25]

def _run_backtest_screener(market='us'):
    """Run backtest strategies on top stocks, return those with buy signals."""
    picks = []
    strategies = [
        {'name': '均线交叉', 'key': 'ma_cross', 'params': {'fast': 5, 'slow': 20}},
        {'name': 'RSI均值回归', 'key': 'rsi', 'params': {'period': 14, 'oversold': 30, 'overbought': 70}},
        {'name': '动量突破', 'key': 'momentum', 'params': {'lookback': 20}},
        {'name': 'Skew套利', 'key': 'skew_arb', 'params': {'short_window': 5, 'long_window': 20, 'entry_ratio': 1.5}},
        {'name': '波动率曲面', 'key': 'vol_surface', 'params': {'bb_period': 20, 'bb_std': 2.0, 'entry_thresh': 2.0}},
    ]
    symbols_processed = set()
    sectors = SECTOR_STOCKS if market != 'cn' else CN_SECTOR_STOCKS

    for sector_name, stocks in sectors.items():
        for info in stocks[:3]:
            sym = info[0]; name = info[1]
            if sym in symbols_processed: continue
            symbols_processed.add(sym)
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=1y"
                resp = requests.get(url, headers=YF_HEADERS, timeout=10)
                if resp.status_code != 200: continue
                data = resp.json()
                r = data.get('chart',{}).get('result',[None])[0]
                if not r: continue
                closes = [c for c in r.get('indicators',{}).get('quote',[{}])[0].get('close',[]) if c is not None]
                if len(closes) < 60: continue
                close = closes[-1]
                for strat in strategies:
                    result = run_backtest(closes, strat['key'], strat['params'])
                    if not result or 'signals' not in result: continue
                    signals = result['signals']
                    if signals and signals[-1] == 1:
                        picks.append({
                            'symbol': sym, 'name': name, 'sector': sector_name,
                            'strategy': strat['name'], 'price': round(close, 2),
                            'total_return': result.get('total_return', 0),
                            'sharpe': result.get('sharpe', 0),
                        })
                        break  # one signal per stock
                time.sleep(0.1)
            except: continue

    picks.sort(key=lambda x: x.get('sharpe', 0), reverse=True)
    return picks[:15]

_screener_cache_cn = {'ts': 0, 'results': []}
_bt_screener_cache = {'ts': 0, 'results': []}

@app.route('/api/screener/us')
def api_screener():
    global _screener_cache, _bt_screener_cache
    now = time.time()
    if now - _screener_cache['ts'] < 86400:
        results = _screener_cache['results']
    else:
        results = _run_screener('us')
        _screener_cache = {'ts': now, 'results': results}
    # Backtest screener (separate cache)
    if now - _bt_screener_cache['ts'] < 86400:
        bt_results = _bt_screener_cache['results']
    else:
        bt_results = _run_backtest_screener('us')
        _bt_screener_cache = {'ts': now, 'results': bt_results}
    return jsonify({
        "results": results,
        "backtest": bt_results,
        "updated": datetime.fromtimestamp(_screener_cache['ts']).isoformat()
    })
    _screener_cache = {'ts': now, 'results': results}
    return jsonify({"results": results, "updated": datetime.now().isoformat()})

@app.route('/api/cn/screener/us')
def api_cn_screener():
    global _screener_cache_cn
    now = time.time()
    if now - _screener_cache_cn['ts'] < 86400:
        return jsonify({"results": _screener_cache_cn['results'], "updated": datetime.fromtimestamp(_screener_cache_cn['ts']).isoformat()})
    results = _run_screener('cn')
    _screener_cache_cn = {'ts': now, 'results': results}
    return jsonify({"results": results, "updated": datetime.now().isoformat()})

def _refresh_screener():
    global _screener_cache, _screener_cache_cn
    print("[Screener] Refreshing...")
    _screener_cache = {'ts': time.time(), 'results': _run_screener('us')}
    _screener_cache_cn = {'ts': time.time(), 'results': _run_screener('cn')}
    print(f"[Screener] Done: {len(_screener_cache['results'])} US + {len(_screener_cache_cn['results'])} CN")

_last_fetch_time = {'us': None, 'cn': None, 'kol': None}

scheduler = BackgroundScheduler(timezone='Asia/Shanghai')
# 美股(北京时间): 03:30收盘前半小时 + 04:30收盘后半小时
scheduler.add_job(daily_fetch_job, 'cron', hour=3, minute=30)
scheduler.add_job(daily_fetch_job, 'cron', hour=4, minute=30)
# A股(北京时间): 14:30收盘前半小时 + 15:30收盘后半小时
scheduler.add_job(cn_daily_fetch_job, 'cron', hour=14, minute=30)
scheduler.add_job(cn_daily_fetch_job, 'cron', hour=15, minute=30)
# KOL tweets: refresh every 30 minutes
scheduler.add_job(_refresh_kol_cache, 'interval', minutes=30)
# Stock reports: refresh on 1st of each month at 8:00 AM
scheduler.add_job(_refresh_monthly_reports, 'cron', day=1, hour=8, minute=0)
scheduler.start()
print("[Scheduler] Started: US 03:30/04:00, CN 14:30/15:00 (Beijing time)")

# Warm up KOL cache shortly after startup (in background thread)
def _delayed_warmup():
    time.sleep(10)  # Wait 10s for app to fully start
    _build_ticker_sector_map()
    _warmup_kol_cache()

threading.Thread(target=_delayed_warmup, daemon=True).start()

# ── Quant Backtest Engine ────────────────────────────────────
def run_backtest(prices, strategy, params):
    """Simple vectorized backtest engine. Returns performance metrics."""
    n = len(prices)
    if n < 50: return None

    signals = [0] * n
    position = [0] * n
    returns = [0] * n

    if strategy == 'ma_cross':
        # Moving average crossover: fast MA crosses above slow MA = buy, below = sell
        fast = params.get('fast', 5)
        slow = params.get('slow', 20)
        for i in range(slow, n):
            ma_fast = sum(prices[i-fast+1:i+1]) / fast
            ma_slow = sum(prices[i-slow+1:i+1]) / slow
            prev_fast = sum(prices[i-fast:i]) / fast
            prev_slow = sum(prices[i-slow:i]) / slow
            if prev_fast <= prev_slow and ma_fast > ma_slow:
                signals[i] = 1
            elif prev_fast >= prev_slow and ma_fast < ma_slow:
                signals[i] = -1

    elif strategy == 'rsi':
        # RSI mean reversion: oversold (<30) = buy, overbought (>70) = sell
        period = params.get('period', 14)
        oversold = params.get('oversold', 30)
        overbought = params.get('overbought', 70)
        gains, losses = [], []
        for i in range(1, n):
            diff = prices[i] - prices[i-1]
            gains.append(max(diff, 0))
            losses.append(max(-diff, 0))
        for i in range(period, n):
            avg_gain = sum(gains[i-period:i]) / period
            avg_loss = sum(losses[i-period:i]) / period
            if avg_loss == 0: rsi = 100
            else: rsi = 100 - 100 / (1 + avg_gain / avg_loss)
            if rsi < oversold: signals[i] = 1
            elif rsi > overbought: signals[i] = -1

    elif strategy == 'momentum':
        # Momentum: buy if price > N-day high, sell if < N-day low
        lookback = params.get('lookback', 20)
        for i in range(lookback, n):
            highest = max(prices[i-lookback:i])
            lowest = min(prices[i-lookback:i])
            if prices[i] > highest: signals[i] = 1
            elif prices[i] < lowest: signals[i] = -1

    elif strategy == 'sector_rotation':
        # Buy & hold with weekly rebalance check
        lookback = params.get('lookback', 5)
        for i in range(lookback, n):
            mom = (prices[i] - prices[i-lookback]) / prices[i-lookback]
            if mom > 0.02: signals[i] = 1
            elif mom < -0.02: signals[i] = -1

    elif strategy == 'skew_arb':
        # Skew Arbitrage: trade volatility mean-reversion using HV ratio
        # When short-term vol spikes vs long-term vol → fear → buy (bet on calm return)
        # When short-term vol collapses → complacency → sell
        short_window = params.get('short_window', 5)
        long_window = params.get('long_window', 20)
        entry_ratio = params.get('entry_ratio', 1.5)
        for i in range(long_window + 1, n):
            # Calculate historical volatility for short and long windows
            short_rets = [(prices[j] - prices[j-1]) / prices[j-1] for j in range(i-short_window+1, i+1) if prices[j-1] > 0]
            long_rets = [(prices[j] - prices[j-1]) / prices[j-1] for j in range(i-long_window+1, i+1) if prices[j-1] > 0]
            if len(short_rets) < 3 or len(long_rets) < 5: continue
            hv_short = (sum((r - sum(short_rets)/len(short_rets))**2 for r in short_rets) / len(short_rets)) ** 0.5
            hv_long = (sum((r - sum(long_rets)/len(long_rets))**2 for r in long_rets) / len(long_rets)) ** 0.5
            if hv_long <= 0: continue
            ratio = hv_short / hv_long
            if ratio > entry_ratio:
                signals[i] = 1   # vol spike → panic → buy
            elif ratio < (1 / entry_ratio):
                signals[i] = -1  # vol crush → complacency → sell
            elif ratio < 1.2 and ratio > 0.85:
                signals[i] = signals[i-1]  # hold position during normal vol

    elif strategy == 'vol_surface':
        # Vol Surface Arbitrage: trade term structure dislocation using Bollinger Band Width
        # BBW = (upper - lower) / middle, proxy for volatility term structure
        # Wide bands → dislocation → bet on contraction; Narrow bands → bet on expansion
        bb_period = params.get('bb_period', 20)
        bb_std = params.get('bb_std', 2.0)
        entry_thresh = params.get('entry_thresh', 2.0)
        for i in range(bb_period, n):
            window = prices[i-bb_period:i]
            ma = sum(window) / bb_period
            variance = sum((p - ma)**2 for p in window) / bb_period
            std = variance ** 0.5
            if ma <= 0: continue
            upper = ma + bb_std * std
            lower = ma - bb_std * std
            bbw = (upper - lower) / ma  # Bollinger Band Width
            if bbw > entry_thresh:
                signals[i] = 1   # extreme width → bet on volatility contraction → buy
            elif bbw < 0.03:
                signals[i] = -1  # extremely narrow → bet on expansion → sell

    # Calculate positions & returns
    for i in range(1, n):
        if signals[i] == 1: position[i] = 1
        elif signals[i] == -1: position[i] = 0
        else: position[i] = position[i-1]
        if prices[i-1] > 0:
            returns[i] = position[i] * (prices[i] - prices[i-1]) / prices[i-1]

    # Performance metrics
    total_ret = math.prod([1 + r for r in returns if r != 0]) - 1 if any(r != 0 for r in returns) else 0
    daily_rets = [r for r in returns[1:] if r != 0]
    if daily_rets:
        avg_ret = sum(daily_rets) / len(daily_rets)
        std_ret = (sum((r - avg_ret)**2 for r in daily_rets) / len(daily_rets)) ** 0.5
        sharpe = (avg_ret / std_ret * (252**0.5)) if std_ret > 0 else 0
    else:
        sharpe = 0

    # Max drawdown - calculate on strategy equity curve
    peak = 1.0
    max_dd = 0
    cum_ret = 1.0
    cum_rets = []
    for i in range(n):
        cum_ret *= (1 + returns[i])
        cum_rets.append(cum_ret)
        peak = max(peak, cum_ret)
        dd = (peak - cum_ret) / peak if peak > 0 else 0
        max_dd = max(max_dd, dd)

    # Win rate
    trades = [(i, signals[i]) for i in range(1, n) if signals[i] != 0 and signals[i] != signals[i-1]]
    wins = 0
    for j in range(1, len(trades)):
        entry_idx = trades[j-1][0]
        exit_idx = trades[j][0]
        if signals[entry_idx] == 1 and exit_idx > entry_idx:
            if cum_rets[exit_idx] > cum_rets[entry_idx]: wins += 1

    return {
        'total_return': round(total_ret * 100, 2),
        'sharpe': round(sharpe, 2),
        'max_drawdown': round(max_dd * 100, 2),
        'win_rate': round(wins / len(trades) * 100, 1) if len(trades) > 1 else 0,
        'trades': len(trades),
        'buy_hold_return': round((prices[-1] - prices[0]) / prices[0] * 100, 2),
        'equity_curve': [round(v, 4) for v in cum_rets],
        'signals': signals,
        'trade_list': [
            {'index': t[0], 'signal': t[1], 'price': round(prices[t[0]], 2)}
            for t in trades
        ],
    }

# ── Routes ───────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/latest')
def api_latest():
    target_date = request.args.get('date', '')
    force = request.args.get('force', '0') == '1'
    auto_refresh = not target_date
    if not target_date:
        with sqlite3.connect(DB_PATH) as conn:
            latest_date = conn.execute("SELECT MAX(date) FROM sector_data").fetchone()[0]
        target_date = latest_date or (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    # Force refresh: always fetch live
    if force:
        data = fetch_sector_data(target_date)
        if data:
            save_to_db(data, target_date)
            return jsonify({"date": target_date, "sectors": data, "fetched": "live", "updated_at": datetime.now().strftime('%m-%d %H:%M'), "next_update": _next_update_time('us')})
        return jsonify({"error": "no data", "date": target_date})
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT symbol, name, category, close, change_pct, volume, fetched_at FROM sector_data WHERE date = ? ORDER BY change_pct DESC", (target_date,)
        ).fetchall()
        summary = conn.execute(
            "SELECT * FROM daily_summary WHERE date = ?", (target_date,)
        ).fetchone()
    if not rows:
        data = fetch_sector_data(target_date)
        if data:
            save_to_db(data, target_date)
            return jsonify({"date": target_date, "sectors": data, "fetched": "live", "updated_at": datetime.now().strftime('%m-%d %H:%M'), "next_update": _next_update_time('us')})
        return jsonify({"error": "no data", "date": target_date})
    # Auto-refresh stale data when requesting latest
    if auto_refresh and rows:
        try:
            latest_fetched = rows[0]['fetched_at']
            if latest_fetched:
                fetched_dt = datetime.strptime(latest_fetched, '%Y-%m-%d %H:%M:%S')
                if (datetime.now() - fetched_dt).total_seconds() > 2 * 3600:
                    data = fetch_sector_data(target_date)
                    if data:
                        save_to_db(data, target_date)
                        with sqlite3.connect(DB_PATH) as conn:
                            conn.row_factory = sqlite3.Row
                            rows = conn.execute(
                                "SELECT symbol, name, category, close, change_pct, volume, fetched_at FROM sector_data WHERE date = ? ORDER BY change_pct DESC", (target_date,)
                            ).fetchall()
                            summary = conn.execute("SELECT * FROM daily_summary WHERE date = ?", (target_date,)).fetchone()
                        return jsonify({
                            "date": target_date,
                            "sectors": [dict(r) for r in rows],
                            "summary": dict(summary) if summary else None,
                            "fetched": "live",
                            "updated_at": datetime.now().strftime('%m-%d %H:%M'),
                            "next_update": _next_update_time('us')
                        })
        except Exception as e:
            app.logger.warning(f"Auto-refresh failed: {e}")
    updated_at = max(r['fetched_at'] for r in rows)[:16].replace('T', ' ') if rows else '-'
    return jsonify({
        "date": target_date,
        "sectors": [dict(r) for r in rows],
        "summary": dict(summary) if summary else None,
        "fetched": "cached",
        "updated_at": updated_at,
        "next_update": _next_update_time('us')
    })

@app.route('/api/dates')
def api_dates():
    """Get list of all available trading dates."""
    with sqlite3.connect(DB_PATH) as conn:
        dates = conn.execute(
            "SELECT DISTINCT date FROM sector_data ORDER BY date DESC"
        ).fetchall()
    return jsonify([d[0] for d in dates])

@app.route('/api/backfill', methods=['POST'])
def api_backfill():
    """Backfill 1 year of historical data for all symbols."""
    symbols = list(set(s[1] for s in SUB_SECTORS))
    total_rows = 0
    for sym in symbols:
        try:
            url = f"https://api.stockanalysis.com/api/symbol/s/{sym}/history?range=1y"
            resp = requests.get(url, headers=SA_HEADERS, timeout=20)
            if resp.status_code != 200: continue
            data = resp.json()
            if 'data' not in data or not data['data']: continue
            items = data['data']
            name = category = None
            for cat, s, n in SUB_SECTORS:
                if s == sym: name = n; category = cat; break
            with sqlite3.connect(DB_PATH) as conn:
                for j in range(len(items) - 1):
                    latest = items[j]; prev = items[j + 1]
                    date = latest.get('t', '')
                    if not date or date < '2026-01-01': continue
                    close = latest.get('c', 0); prev_close = prev.get('c', 0)
                    change_pct = ((close - prev_close) / prev_close) * 100 if prev_close else 0
                    conn.execute('''INSERT OR IGNORE INTO sector_data
                        (date, symbol, name, category, open, high, low, close, change_pct, volume)
                        VALUES (?,?,?,?,?,?,?,?,?,?)''',
                        (date, sym, name, category, round(latest.get('o',0),2),
                         round(latest.get('h',0),2), round(latest.get('l',0),2),
                         round(close,2), round(change_pct,2), int(latest.get('v',0))))
                conn.commit()
            total_rows += 1
            time.sleep(0.1)
        except Exception as e:
            app.logger.warning(f"Backfill fail {sym}: {e}")
    # Generate summaries
    with sqlite3.connect(DB_PATH) as conn:
        dates = [r[0] for r in conn.execute("SELECT DISTINCT date FROM sector_data").fetchall()]
        for date in dates:
            rows = conn.execute("SELECT * FROM sector_data WHERE date=?", (date,)).fetchall()
            if not rows: continue
            sects = [r for r in rows if r[3] != '指数']
            up = sum(1 for r in sects if r[8] > 0); down = sum(1 for r in sects if r[8] < 0)
            best = max(sects, key=lambda r: r[8]) if sects else None
            worst = min(sects, key=lambda r: r[8]) if sects else None
            spy = next((r for r in rows if r[1]=='SPY'), None)
            conn.execute('''INSERT OR IGNORE INTO daily_summary VALUES (?,?,?,?,?,?,?,?,?)''',
                (date, len(sects), up, down, best[2] if best else None, best[8] if best else None,
                 worst[2] if worst else None, worst[8] if worst else None, spy[8] if spy else None))
        conn.commit()
    return jsonify({"status": "ok", "symbols": total_rows})

@app.route('/api/sector/<sector_name>')
def api_sector_detail(sector_name):
    """Get top 10 stocks for a given sector with real-time quotes."""
    stocks = SECTOR_STOCKS.get(sector_name, [])
    if not stocks:
        return jsonify({"error": "sector not found", "stocks": []})
    quotes = fetch_stock_quotes(stocks)
    return jsonify({"sector": sector_name, "stocks": quotes})

@app.route('/api/stock/<symbol>/history')
def api_stock_history(symbol):
    """Return OHLC history for candlestick chart (3 months) via Yahoo Finance."""
    try:
        url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=3mo"
        resp = requests.get(url, headers=YF_HEADERS, timeout=15)
        if resp.status_code != 200:
            return jsonify({"error": "fetch failed"}), 500
        data = resp.json()
        r = data.get('chart', {}).get('result', [None])[0]
        if not r:
            return jsonify({"error": "no data"}), 500
        timestamps = r.get('timestamp', [])
        quotes = r.get('indicators', {}).get('quote', [{}])[0]
        opens = quotes.get('open', [])
        closes = quotes.get('close', [])
        lows = quotes.get('low', [])
        highs = quotes.get('high', [])
        volumes = quotes.get('volume', [])
        ohlc = []
        for i in range(len(timestamps)):
            if closes[i] is None: continue
            dt = datetime.fromtimestamp(timestamps[i]).strftime('%Y-%m-%d')
            ohlc.append([
                dt,
                round(opens[i] or 0, 2),
                round(closes[i] or 0, 2),
                round(lows[i] or 0, 2),
                round(highs[i] or 0, 2),
                int(volumes[i] or 0),
            ])
        return jsonify({"symbol": symbol, "ohlc": ohlc[-90:]})  # last 90 trading days
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/watchlist/quotes', methods=['POST'])
def api_watchlist_quotes():
    """Batch fetch real-time quotes + sparkline + quick RSI for watchlist."""
    symbols = request.get_json().get('symbols', []) if request.is_json else []
    if not symbols: return jsonify({"quotes": []})
    results = []
    for sym in symbols[:50]:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=1mo&includePrePost=true"
            resp = requests.get(url, headers=YF_HEADERS, timeout=8)
            if resp.status_code != 200: continue
            chart_data = resp.json().get('chart',{}).get('result',[None])[0]
            if not chart_data: continue
            meta = chart_data.get('meta',{})
            # Use meta for price, chart closes for change calculation
            price = meta.get('regularMarketPrice')
            quotes_raw = chart_data.get('indicators',{}).get('quote',[{}])[0]
            closes_raw = [c for c in quotes_raw.get('close',[]) if c is not None]
            if not price: price = closes_raw[-1] if closes_raw else 0
            if len(closes_raw) < 2 or not price: continue
            # Find last two closes that differ (skip zero-change bars from holidays)
            prev_close = closes_raw[-2]
            for i in range(len(closes_raw)-2, 0, -1):
                if closes_raw[i] != closes_raw[-1]:
                    prev_close = closes_raw[i]; break
            if prev_close <= 0: continue
            chg_pct = round((price-prev_close)/prev_close*100, 2)
            # Chart data for sparkline and RSI
            quotes_raw = chart_data.get('indicators',{}).get('quote',[{}])[0]
            closes = [c for c in quotes_raw.get('close',[]) if c is not None]
            highs = quotes_raw.get('high',[]); lows = quotes_raw.get('low',[])
            volumes = quotes_raw.get('volume',[])
            spark = [round(c,2) for c in closes[-20:]]
            day_high = round(max(highs[-5:]),2) if len(highs)>=5 else round(price,2)
            day_low = round(min(lows[-5:]),2) if len(lows)>=5 else round(price,2)
            rsi14 = None
            if len(closes) >= 15:
                gains, losses = [], []
                for i in range(1,len(closes)): d=closes[i]-closes[i-1]; gains.append(max(d,0)); losses.append(max(-d,0))
                if len(gains)>=14:
                    ag=sum(gains[-14:])/14; al=sum(losses[-14:])/14; rs=ag/al if al>0 else 100
                    rsi14=round(100-100/(1+rs),1)
            vol_ratio = 1.0
            if volumes and len(volumes) >= 6:
                vol_now = volumes[-1] or 0
                vol_avg = sum(v for v in volumes[-6:-1] if v)/5 if any(v for v in volumes[-6:-1] if v) else 1
                vol_ratio = round(vol_now/vol_avg,1) if vol_avg>0 else 1.0
            results.append({
                "symbol": sym, "price": round(price,2), "change_pct": chg_pct,
                "high": day_high, "low": day_low, "sparkline": spark,
                "rsi14": rsi14, "vol_ratio": vol_ratio
            })
            time.sleep(0.08)
        except: pass
    return jsonify({"quotes": results})

@app.route('/api/stock/<symbol>/intraday')
def api_intraday_bars(symbol):
    """Return today's 5-min intraday bars for charting (CORS-safe proxy)."""
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=5m&range=1d&includePrePost=true"
        resp = requests.get(url, headers=YF_HEADERS, timeout=10)
        if resp.status_code != 200: return jsonify({"error":"fetch failed"}), 500
        data = resp.json()
        r = data.get('chart',{}).get('result',[None])[0]
        if not r: return jsonify({"error":"no data"}), 500
        timestamps = r.get('timestamp',[]); quotes = r.get('indicators',{}).get('quote',[{}])[0]
        closes = quotes.get('close',[]); opens = quotes.get('open',[])
        highs = quotes.get('high',[]); lows = quotes.get('low',[])
        bars = []
        for i in range(len(timestamps)):
            if closes[i] is not None:
                bars.append({"t": timestamps[i], "o": round(opens[i] or 0, 2), "h": round(highs[i] or 0, 2), "l": round(lows[i] or 0, 2), "c": round(closes[i] or 0, 2)})
        return jsonify({"symbol": symbol, "bars": bars[-80:]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

_bt_job = {'status': 'idle', 'result': None, 'error': None}

def _run_backtest_thread():
    """Run backtest in background thread."""
    global _bt_job
    _bt_job = {'status': 'running', 'result': None, 'error': None}
    try:
        with app.app_context():
            result = _do_backtest()
            _bt_job = {'status': 'done', 'result': result, 'error': None}
    except Exception as e:
        _bt_job = {'status': 'error', 'result': None, 'error': str(e)}

@app.route('/api/backtest/mean-reversion', methods=['GET', 'POST'])
def api_mean_reversion_backtest():
    """POST to start backtest job, GET to poll for results."""
    global _bt_job
    if request.method == 'POST':
        if _bt_job['status'] == 'running':
            return jsonify({"status": "running"})
        threading.Thread(target=_run_backtest_thread, daemon=True).start()
        return jsonify({"status": "started"})
    return jsonify(_bt_job)

def _do_backtest():
    """Backtest the 60d-bottom-sector + low RSI + low vol strategy with MC sims."""
    try:
        # Fetch 6 months of sector data from DB
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            all_dates = [r[0] for r in conn.execute(
                "SELECT DISTINCT date FROM sector_data ORDER BY date ASC"
            ).fetchall()]
        if len(all_dates) < 80: return jsonify({"error": "insufficient data"}), 500

        # Get stock prices for all sector stocks (cached from screener data)
        stock_prices = {}  # {symbol: {date: close}}
        stock_volumes = {}  # {symbol: {date: volume}}
        symbols_to_fetch = set()
        for sector_name, stocks in SECTOR_STOCKS.items():
            for info in stocks[:3]: symbols_to_fetch.add(info[0])  # top 3 per sector

        def _fetch_one_stock(sym):
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=6mo"
                resp = requests.get(url, headers=YF_HEADERS, timeout=10)
                if resp.status_code != 200: return None
                r = resp.json().get('chart',{}).get('result',[None])[0]
                if not r: return None
                timestamps = r.get('timestamp',[])
                quotes = r.get('indicators',{}).get('quote',[{}])[0]
                closes = quotes.get('close',[]); volumes = quotes.get('volume',[])
                prices = {}; vols = {}
                for i in range(len(timestamps)):
                    dt = datetime.fromtimestamp(timestamps[i]).strftime('%Y-%m-%d')
                    if closes[i] is not None: prices[dt] = closes[i]
                    if volumes[i] is not None: vols[dt] = volumes[i]
                return (sym, prices, vols) if len(prices) >= 50 else None
            except: return None

        syms = list(symbols_to_fetch)[:100]
        print(f"[Backtest] Fetching {len(syms)} symbols in parallel...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(_fetch_one_stock, sym): sym for sym in syms}
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    sym, prices, vols = result
                    stock_prices[sym] = prices
                    stock_volumes[sym] = vols
        print(f"[Backtest] Got prices for {len(stock_prices)} stocks")

        # Build sector→stocks map with price data
        sector_stocks_prices = {}
        for sector_name, stocks in SECTOR_STOCKS.items():
            valid = []
            for info in stocks:
                if info[0] in stock_prices: valid.append(info)
            if valid: sector_stocks_prices[sector_name] = valid

        # Strategy: bottom 60d sectors → low RSI + low vol stocks
        def find_picks(date_str, bottom_n=3, rsi_max=40, vol_thresh=0.8, flat_days=3):
            """Find stocks matching criteria on a given date."""
            # Get 60d sector performance up to date_str
            sector_60d = {}
            with sqlite3.connect(DB_PATH) as conn:
                rows = conn.execute(
                    "SELECT date, name, change_pct FROM sector_data WHERE date <= ? ORDER BY date DESC",
                    (date_str,)
                ).fetchall()
            # Calculate cumulative 60d return per sector
            sector_rets = {}
            for row in rows:
                if row[1] not in sector_rets: sector_rets[row[1]] = []
                sector_rets[row[1]].append((row[0], row[2]))

            # Simple: use the 60d-period change from rotation API approach
            # Just pick sectors with worst recent performance
            sector_d60 = {}
            with sqlite3.connect(DB_PATH) as conn:
                dates_60 = [r[0] for r in conn.execute(
                    "SELECT DISTINCT date FROM sector_data WHERE date <= ? ORDER BY date DESC LIMIT 61",
                    (date_str,)
                ).fetchall()]
                if len(dates_60) < 2: return []
                d_recent, d_60ago = dates_60[0], dates_60[-1]
                for name in sector_stocks_prices:
                    r1 = conn.execute("SELECT change_pct FROM sector_data WHERE date=? AND name=?", (d_recent, name)).fetchone()
                    r2 = conn.execute("SELECT close FROM sector_data WHERE date=? AND name=?", (d_60ago, name)).fetchone()
                    if r1: sector_d60[name] = r1[0]

            bottom = sorted(sector_d60.items(), key=lambda x: x[1])[:bottom_n]
            bottom_names = [b[0] for b in bottom]

            # Screen stocks in bottom sectors
            picks = []
            for sector_name in bottom_names:
                stocks = sector_stocks_prices.get(sector_name, [])
                for info in stocks:
                    sym = info[0]; name = info[1]
                    prices = stock_prices.get(sym, {})
                    if date_str not in prices: continue
                    # Get recent closes
                    closes_list = [(d, prices[d]) for d in sorted(prices.keys()) if d <= date_str]
                    if len(closes_list) < 20: continue
                    closes_list = closes_list[-20:]
                    closes_vals = [c[1] for c in closes_list]

                    # RSI(14)
                    gains, losses = [], []
                    for i in range(1, len(closes_vals)):
                        d = closes_vals[i] - closes_vals[i-1]; gains.append(max(d,0)); losses.append(max(-d,0))
                    if len(gains) < 14: continue
                    ag = sum(gains[-14:])/14; al = sum(losses[-14:])/14
                    rsi = 100 - 100/(1+ag/al) if al > 0 else 100
                    if rsi > rsi_max: continue

                    # Volume check: current vol < 20-day avg * vol_thresh (缩量)
                    vols_data = stock_volumes.get(sym, {})
                    vol_dates = sorted(vols_data.keys())
                    recent_vols = [vols_data[d] for d in vol_dates if d <= date_str]
                    if len(recent_vols) >= 20:
                        cur_vol = recent_vols[-1] or 0
                        avg_vol = sum(v or 0 for v in recent_vols[-21:-1]) / 20
                        if avg_vol > 0 and cur_vol / avg_vol > vol_thresh:
                            continue  # Volume too high (not shrinking)
                    # Check if last N days are flat (not risen)
                    if len(closes_vals) < flat_days + 1: continue
                    recent = closes_vals[-(flat_days+1):]
                    risen = recent[-1] > recent[0]
                    if risen: continue

                    picks.append({"symbol": sym, "name": name, "sector": sector_name, "rsi": round(rsi,1), "price": closes_vals[-1]})
            return picks

        # Run backtest across all dates
        test_dates = all_dates[80:]  # skip first 80 days for warmup
        trades = []
        # Monte Carlo: vary parameters 1000 times
        mc_results = []
        import random as py_random
        py_random.seed(42)

        n_sims = 500
        for sim in range(n_sims):
            # Randomly sample parameters
            bottom_n = py_random.randint(2, 5)
            rsi_max = py_random.randint(30, 50)
            vol_thresh = round(py_random.uniform(0.5, 1.2), 1)
            flat_days = py_random.randint(2, 5)
            # Randomly sample a subset of test dates
            sample_size = min(40, len(test_dates))
            sim_dates = py_random.sample(test_dates, sample_size)

            sim_returns = []
            sim_wins = 0
            for date_str in sim_dates:
                picks = find_picks(date_str, bottom_n=bottom_n, rsi_max=rsi_max, vol_thresh=vol_thresh, flat_days=flat_days)
                if not picks: continue
                # Find next trading day
                date_idx = all_dates.index(date_str) if date_str in all_dates else -1
                if date_idx < 0 or date_idx + 1 >= len(all_dates): continue
                next_date = all_dates[date_idx + 1]
                # Calculate returns
                pnl = 0
                for pick in picks:
                    prices = stock_prices.get(pick['symbol'], {})
                    if date_str in prices and next_date in prices:
                        ret = (prices[next_date] - prices[date_str]) / prices[date_str] * 100
                        pnl += ret
                avg_ret = pnl / len(picks) if picks else 0
                sim_returns.append(avg_ret)
                if avg_ret > 0: sim_wins += 1

            if sim_returns:
                avg = sum(sim_returns)/len(sim_returns)
                std = (sum((r-avg)**2 for r in sim_returns)/len(sim_returns))**0.5
                sharpe = avg/std * (252**0.5) if std > 0 else 0
                mc_results.append({
                    "sim": sim+1, "avg_return": round(avg, 3),
                    "win_rate": round(sim_wins/len(sim_returns)*100, 1),
                    "sharpe": round(sharpe, 2), "n_trades": len(sim_returns),
                    "params": {"bottom_n": bottom_n, "rsi_max": rsi_max, "vol_thresh": vol_thresh, "flat_days": flat_days}
                })

        if not mc_results: return {"error": "no valid simulations"}

        # Aggregate results
        all_returns = [r["avg_return"] for r in mc_results]
        all_wins = [r["win_rate"] for r in mc_results]
        avg_return = sum(all_returns)/len(all_returns)
        win_rate = sum(all_wins)/len(all_wins)
        positive_pct = sum(1 for r in all_returns if r > 0) / len(all_returns) * 100
        best = max(mc_results, key=lambda r: r["avg_return"])
        worst = min(mc_results, key=lambda r: r["avg_return"])

        # SPY baseline
        spy_prices = stock_prices.get('SPY', {})
        spy_rets = []
        for date_str in test_dates:
            date_idx = all_dates.index(date_str) if date_str in all_dates else -1
            if date_idx < 0 or date_idx+1 >= len(all_dates): continue
            nd = all_dates[date_idx+1]
            if date_str in spy_prices and nd in spy_prices:
                spy_rets.append((spy_prices[nd]-spy_prices[date_str])/spy_prices[date_str]*100)
        spy_avg = sum(spy_rets)/len(spy_rets) if spy_rets else 0

        # Distribution stats
        rets_sorted = sorted(all_returns)
        p10 = rets_sorted[int(len(rets_sorted)*0.1)] if len(rets_sorted)>=10 else all_returns[0]
        p25 = rets_sorted[int(len(rets_sorted)*0.25)] if len(rets_sorted)>=4 else all_returns[0]
        p50 = rets_sorted[int(len(rets_sorted)*0.5)] if len(rets_sorted)>=2 else all_returns[0]
        p75 = rets_sorted[int(len(rets_sorted)*0.75)] if len(rets_sorted)>=4 else all_returns[-1]
        p90 = rets_sorted[int(len(rets_sorted)*0.9)] if len(rets_sorted)>=10 else all_returns[-1]

        return {
            "simulations": len(mc_results),
            "avg_daily_return": round(avg_return, 3),
            "win_rate": round(win_rate, 1),
            "positive_pct": round(positive_pct, 1),
            "best_sim": best,
            "worst_sim": worst,
            "sharpe_avg": round(sum(r["sharpe"] for r in mc_results)/len(mc_results), 2),
            "spy_baseline": round(spy_avg, 3),
            "outperform_pct": round(positive_pct, 1),
            "top_params": sorted(mc_results, key=lambda r: r["avg_return"], reverse=True)[:5],
            "distribution": {"p10": round(p10,3), "p25": round(p25,3), "p50": round(p50,3), "p75": round(p75,3), "p90": round(p90,3)},
            "all_simulations": sorted(mc_results, key=lambda r: r["avg_return"], reverse=True),
        }
    except Exception as e:
        return {"error": str(e)}

@app.route('/api/stock/<symbol>/t0')
def api_t0_signals(symbol):
    """Return T+0 intraday signals: RSI(6), MACD 5-min, Bollinger Bands, VWAP."""
    try:
        sym = symbol.upper()
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=5m&range=5d"
        resp = requests.get(url, headers=YF_HEADERS, timeout=12)
        if resp.status_code != 200: return jsonify({"error": "fetch failed"}), 500
        data = resp.json()
        r = data.get('chart',{}).get('result',[None])[0]
        if not r: return jsonify({"error": "no data"}), 500
        quotes = r.get('indicators',{}).get('quote',[{}])[0]
        closes = [c for c in quotes.get('close',[]) if c is not None]
        opens = quotes.get('open',[]); highs = quotes.get('high',[]); lows = quotes.get('low',[])
        volumes = quotes.get('volume',[]); timestamps = r.get('timestamp',[])
        n = len(closes)
        if n < 30: return jsonify({"error": f"insufficient bars: {n}"}), 500

        # RSI(6)
        gains, losses = [], []
        for i in range(1, n): d=closes[i]-closes[i-1]; gains.append(max(d,0)); losses.append(max(-d,0))
        avg_g = sum(gains[-6:])/6; avg_l = sum(losses[-6:])/6
        rs = avg_g/avg_l if avg_l>0 else 100
        rsi6 = round(100-100/(1+rs), 1)

        # MACD(12,26,9) on 5-min bars
        def ema(d,p): k=2/(p+1); r=[d[0]]; [r.append(d[i]*k+r[-1]*(1-k)) for i in range(1,len(d))]; return r
        e12=ema(closes,12); e26=ema(closes,26)
        macd=[e12[i]-e26[i] for i in range(n)]; sig=ema(macd,9)
        hist=[macd[i]-sig[i] for i in range(n)]
        macd_now, sig_now, hist_now = macd[-1], sig[-1], hist[-1]
        macd_prev, sig_prev = macd[-2], sig[-2]
        golden_cross = macd_prev <= sig_prev and macd_now > sig_now
        dead_cross = macd_prev >= sig_prev and macd_now < sig_now

        # Bollinger Bands(20,2)
        bb20 = closes[-20:]; ma20 = sum(bb20)/20
        std20 = (sum((x-ma20)**2 for x in bb20)/20)**0.5
        bb_upper = round(ma20+2*std20, 2); bb_lower = round(ma20-2*std20, 2)
        bb_pos = round((closes[-1]-bb_lower)/(bb_upper-bb_lower)*100, 1) if (bb_upper-bb_lower)>0 else 50
        touch_lower = closes[-1] <= bb_lower * 1.002
        touch_upper = closes[-1] >= bb_upper * 0.998

        # VWAP (today's volume-weighted avg price)
        today_cutoff = max(0, n-78)  # ~6.5 hours of 5-min bars
        today_closes = closes[today_cutoff:]; today_vols = volumes[today_cutoff:] if len(volumes)>=n else [0]*len(today_closes)
        vwap_num = sum(today_closes[i]*(today_vols[i] or 0) for i in range(len(today_closes)))
        vwap_den = sum(v or 0 for v in today_vols)
        vwap = round(vwap_num/vwap_den, 2) if vwap_den>0 else closes[-1]

        # Volume analysis
        vol_last = volumes[-1] or 0
        vol_avg_10 = sum(v or 0 for v in volumes[-11:-1])/10 if len(volumes)>=11 else vol_last
        vol_surge = round(vol_last/vol_avg_10, 2) if vol_avg_10>0 else 1.0

        # Price action
        price_change = round((closes[-1]-closes[-6])/closes[-6]*100, 2) if len(closes)>=6 and closes[-6]>0 else 0
        high_1d = max(closes[-78:]) if len(closes)>=78 else max(closes)
        low_1d = min(closes[-78:]) if len(closes)>=78 else min(closes)

        # ── Signal Strength ──
        long_signals = []; short_signals = []
        if rsi6 < 25: long_signals.append(("RSI超卖", 30))
        elif rsi6 < 35: long_signals.append(("RSI偏弱", 15))
        if rsi6 > 75: short_signals.append(("RSI超买", 30))
        elif rsi6 > 65: short_signals.append(("RSI偏强", 15))
        if touch_lower: long_signals.append(("布林下轨", 25))
        if touch_upper: short_signals.append(("布林上轨", 25))
        if bb_pos < 20: long_signals.append(("BB低位", 15))
        if bb_pos > 80: short_signals.append(("BB高位", 15))
        if golden_cross: long_signals.append(("MACD金叉", 20))
        if dead_cross: short_signals.append(("MACD死叉", 20))
        if closes[-1] < vwap: long_signals.append(("低于VWAP", 10))
        if closes[-1] > vwap: short_signals.append(("高于VWAP", 10))
        if vol_surge > 2 and price_change > 0: long_signals.append(("放量拉升", 15))
        if vol_surge > 2 and price_change < 0: short_signals.append(("放量杀跌", 15))

        long_score = sum(s[1] for s in long_signals)
        short_score = sum(s[1] for s in short_signals)
        net_score = long_score - short_score

        if net_score >= 40: action = "🟢 强烈做多"; color = "#22c55e"
        elif net_score >= 20: action = "🟡 偏多做T"; color = "#f59e0b"
        elif net_score <= -40: action = "🔴 强烈做空"; color = "#ef4444"
        elif net_score <= -20: action = "🟠 偏空做T"; color = "#f97316"
        else: action = "⚪ 观望"; color = "#94a3b8"

        from datetime import datetime
        last_ts = datetime.fromtimestamp(timestamps[-1]) if timestamps else datetime.now()

        return jsonify({
            "symbol": sym, "last_price": closes[-1], "last_time": last_ts.isoformat(),
            "rsi6": rsi6, "macd_golden_cross": golden_cross, "macd_dead_cross": dead_cross,
            "bb_lower": bb_lower, "bb_upper": bb_upper, "bb_pos": bb_pos,
            "touch_lower": touch_lower, "touch_upper": touch_upper,
            "vwap": vwap, "vol_surge": vol_surge, "price_change_30m": price_change,
            "day_high": high_1d, "day_low": low_1d,
            "long_signals": [{"name": s[0], "score": s[1]} for s in long_signals],
            "short_signals": [{"name": s[0], "score": s[1]} for s in short_signals],
            "long_score": long_score, "short_score": short_score,
            "action": action, "action_color": color,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/stock/<symbol>/technicals')
def api_stock_technicals(symbol):
    """Return comprehensive technical + quant indicators (refreshes on 1st of each month)."""
    # Check cache: refresh only when new month starts
    cached = _technicals_cache.get(symbol.upper())
    if cached:
        cached_month = datetime.fromtimestamp(cached[0]).strftime('%Y-%m')
        current_month = datetime.now().strftime('%Y-%m')
        if cached_month == current_month:
            resp = jsonify(cached[1])
            resp.headers['X-Cache'] = 'HIT'
            return resp

    try:
        # Fetch 1 year of daily data
        url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1y"
        resp = requests.get(url, headers=YF_HEADERS, timeout=15)
        if resp.status_code != 200:
            return jsonify({"error": "fetch failed"}), 500
        data = resp.json()
        r = data.get('chart', {}).get('result', [None])[0]
        if not r: return jsonify({"error": "no data"}), 500

        timestamps = r.get('timestamp', [])
        quotes = r.get('indicators', {}).get('quote', [{}])[0]
        closes = [c for c in quotes.get('close', []) if c is not None]
        opens = [o for o in quotes.get('open', []) if o is not None]
        highs = [h for h in quotes.get('high', []) if h is not None]
        lows = [l for l in quotes.get('low', []) if l is not None]
        vols = quotes.get('volume', [])

        if len(closes) < 60: return jsonify({"error": f"need 60+ days, got {len(closes)}"}), 500

        n = len(closes)
        dates = [datetime.fromtimestamp(ts).strftime('%Y-%m-%d') for ts in timestamps[:n]]

        # ─── Bollinger Bands (20, 2) ───
        bb_upper, bb_mid, bb_lower = [], [], []
        for i in range(n):
            if i < 19:
                bb_upper.append(None); bb_mid.append(None); bb_lower.append(None)
            else:
                w = closes[i-19:i+1]
                ma = sum(w)/20
                std = (sum((x-ma)**2 for x in w)/20)**0.5
                bb_upper.append(round(ma + 2*std, 4))
                bb_mid.append(round(ma, 4))
                bb_lower.append(round(ma - 2*std, 4))

        # ─── MA 20 & MA 50 ───
        ma20 = [round(sum(closes[max(0,i-19):i+1])/min(i+1,20), 4) for i in range(n)]
        ma50 = [round(sum(closes[max(0,i-49):i+1])/min(i+1,50), 4) if i>=49 else None for i in range(n)]

        # ─── Daily Returns ───
        daily_rets = [(closes[i]-closes[i-1])/closes[i-1]*100 if i>0 and closes[i-1]>0 else None for i in range(n)]

        # ─── RSI (14) ───
        rsi = [None]*n
        gains, losses = [], []
        for i in range(1, n):
            d = closes[i] - closes[i-1]
            gains.append(max(d,0)); losses.append(max(-d,0))
        for i in range(14, n):
            avg_g = sum(gains[i-14:i])/14
            avg_l = sum(losses[i-14:i])/14
            rs = avg_g/avg_l if avg_l>0 else 100
            rsi[i] = round(100-100/(1+rs), 2)

        # ─── MACD (12, 26, 9) ───
        def ema(data, period):
            k = 2/(period+1)
            result = [data[0]]
            for i in range(1,len(data)):
                result.append(data[i]*k + result[-1]*(1-k))
            return result
        ema12 = ema(closes, 12)
        ema26 = ema(closes, 26)
        macd_line = [ema12[i]-ema26[i] for i in range(n)]
        signal_line = ema(macd_line, 9)
        macd_hist = [macd_line[i]-signal_line[i] for i in range(n)]

        # ─── Returns Distribution ───
        rets = [r for r in daily_rets if r is not None]
        min_r, max_r = min(rets), max(rets)
        bin_count = 30
        bin_width = (max_r-min_r)/bin_count
        bins = [min_r + i*bin_width for i in range(bin_count+1)]
        hist = [0]*bin_count
        for r in rets:
            idx = min(int((r-min_r)//bin_width), bin_count-1)
            hist[idx] += 1

        # ─── Quant Metrics (PPT: 量化金融核心指标) ───
        m = len(rets)
        mu_daily = sum(rets)/m  # 日均收益率 (%)
        mu_annual = mu_daily * 252  # 年化收益 (%)
        sigma_daily = (sum((r-mu_daily)**2 for r in rets)/m) ** 0.5  # 日波动率 (%)
        sigma_annual = sigma_daily * (252**0.5)  # 年化波动率 (%)
        rf_daily = 0.04 / 252  # 假设无风险利率 4%
        sharpe = (mu_annual - 4.0) / sigma_annual if sigma_annual > 0 else 0

        # VaR & CVaR (Historical method, 95%)
        sorted_rets = sorted(rets)
        var_idx = int(m * 0.05)
        var_95 = sorted_rets[var_idx] if var_idx < m else sorted_rets[-1]
        cvar_95 = sum(sorted_rets[:var_idx]) / var_idx if var_idx > 0 else var_95

        # Skewness & Kurtosis
        skew = (sum((r-mu_daily)**3 for r in rets)/m) / (sigma_daily**3) if sigma_daily > 0 else 0
        kurt = (sum((r-mu_daily)**4 for r in rets)/m) / (sigma_daily**4) if sigma_daily > 0 else 0
        excess_kurt = kurt - 3

        # Jarque-Bera test
        jb_stat = m/6 * (skew**2 + (excess_kurt**2)/4)
        # p-value approximation for JB (chi-square with 2 df)
        jb_pvalue = 1.0 if jb_stat < 0.1 else math.exp(-jb_stat/2) * (1 + jb_stat/2) if jb_stat < 10 else 0.0
        is_normal = jb_pvalue > 0.05

        # Max Drawdown
        peak = closes[0]
        max_dd = 0.0
        for c in closes:
            if c > peak: peak = c
            dd = (peak - c) / peak * 100 if peak > 0 else 0
            if dd > max_dd: max_dd = dd

        # Win Rate (% of positive days)
        win_days = sum(1 for r in rets if r > 0)
        win_rate = win_days / m * 100 if m > 0 else 0

        # Best/Worst day
        best_day = max(rets)
        worst_day = min(rets)

        # Positive/Negative day count and avg
        pos_rets = [r for r in rets if r > 0]
        neg_rets = [r for r in rets if r < 0]
        avg_pos = sum(pos_rets)/len(pos_rets) if pos_rets else 0
        avg_neg = sum(neg_rets)/len(neg_rets) if neg_rets else 0

        # ─── Factor Decomposition: Beta & R² against SPY ───
        try:
            spy_url = f"https://query2.finance.yahoo.com/v8/finance/chart/SPY?interval=1d&range=1y"
            spy_resp = requests.get(spy_url, headers=YF_HEADERS, timeout=10)
            spy_closes = []
            if spy_resp.status_code == 200:
                spy_data = spy_resp.json()
                spy_r = spy_data.get('chart',{}).get('result',[None])[0]
                if spy_r:
                    spy_closes = [c for c in spy_r.get('indicators',{}).get('quote',[{}])[0].get('close',[]) if c is not None]
            # Align lengths
            min_len = min(len(rets), len(spy_closes)-1)
            spy_rets = [(spy_closes[i+1]-spy_closes[i])/spy_closes[i]*100 for i in range(min_len) if spy_closes[i]>0]
            stock_rets_for_beta = rets[-len(spy_rets):] if len(spy_rets) < len(rets) else rets[:len(spy_rets)]
            if len(spy_rets) >= 60 and len(stock_rets_for_beta) >= 60:
                mn = min(len(spy_rets), len(stock_rets_for_beta))
                x = spy_rets[:mn]; y = stock_rets_for_beta[:mn]
                x_mean = sum(x)/mn; y_mean = sum(y)/mn
                cov_xy = sum((x[i]-x_mean)*(y[i]-y_mean) for i in range(mn)) / mn
                var_x = sum((xi-x_mean)**2 for xi in x) / mn
                beta_spy = round(cov_xy/var_x, 3) if var_x > 0 else None
                r_squared = round((cov_xy/(var_x**0.5)/((sum((yi-y_mean)**2 for yi in y)/mn)**0.5))**2, 4) if var_x>0 and (sum((yi-y_mean)**2 for yi in y)/mn)>0 else None
                # Systematic vs idiosyncratic risk
                sys_risk = round((beta_spy**2 * var_x / (sum((yi-y_mean)**2 for yi in y)/mn) * 100), 1) if beta_spy and r_squared else None
                idiosyncratic_risk = round((1 - r_squared) * 100, 1) if r_squared else None
            else:
                beta_spy = None; r_squared = None; sys_risk = None; idiosyncratic_risk = None
        except:
            beta_spy = None; r_squared = None; sys_risk = None; idiosyncratic_risk = None

        # ─── GARCH(1,1) 10-day forward vol forecast ───
        garch_omega = 0.01; garch_alpha = 0.1; garch_beta = 0.85
        garch_vol = sigma_daily  # start with sample vol
        garch_forecast = []
        for _ in range(10):
            garch_vol = (garch_omega + garch_alpha * (sigma_daily**2) + garch_beta * (garch_vol**2)) ** 0.5
            garch_forecast.append(round(garch_vol, 4))
        garch_10d_vol = round(sum(garch_forecast)/len(garch_forecast), 4) if garch_forecast else sigma_daily

        # ─── Monte Carlo (1000 paths × 21 days) ───
        mc_paths = 1000; mc_days = 21
        mc_returns = []
        for _ in range(mc_paths):
            path = [closes[-1]]
            for __ in range(mc_days):
                shock = mu_daily/100 + sigma_daily/100 * __import__('random').gauss(0, 1)
                path.append(path[-1] * (1 + shock))
            mc_returns.append((path[-1] - path[0]) / path[0] * 100)
        mc_returns.sort()
        mc_median = round(mc_returns[len(mc_returns)//2], 2)
        mc_p5 = round(mc_returns[int(len(mc_returns)*0.05)], 2)
        mc_p95 = round(mc_returns[int(len(mc_returns)*0.95)], 2)
        mc_up_prob = round(sum(1 for r in mc_returns if r > 0) / mc_paths * 100, 1)

        resp_data = {
            "symbol": symbol,
            "dates": dates,
            "closes": [round(c,4) for c in closes],
            "opens": [round(o,4) for o in opens],
            "highs": [round(h,4) for h in highs],
            "lows": [round(l,4) for l in lows],
            "volumes": vols,
            "bb_upper": bb_upper, "bb_mid": bb_mid, "bb_lower": bb_lower,
            "ma20": ma20, "ma50": ma50,
            "daily_returns": [round(r,4) if r else None for r in daily_rets],
            "rsi": rsi,
            "macd_line": [round(m,6) for m in macd_line],
            "signal_line": [round(s,6) for s in signal_line],
            "macd_hist": [round(h,6) for h in macd_hist],
            "dist_bins": [round(b,4) for b in bins[:-1]],
            "dist_counts": hist,
            "dist_min": round(min_r,4), "dist_max": round(max_r,4),
            # Quant metrics
            "quant": {
                "mu_daily": round(mu_daily, 4),
                "mu_annual": round(mu_annual, 2),
                "sigma_daily": round(sigma_daily, 4),
                "sigma_annual": round(sigma_annual, 2),
                "sharpe": round(sharpe, 2),
                "var_95": round(var_95, 2),
                "cvar_95": round(cvar_95, 2),
                "skew": round(skew, 3),
                "excess_kurt": round(excess_kurt, 3),
                "jb_stat": round(jb_stat, 2),
                "jb_pvalue": round(jb_pvalue, 4),
                "is_normal": is_normal,
                "max_dd": round(max_dd, 2),
                "win_rate": round(win_rate, 1),
                "best_day": round(best_day, 2),
                "worst_day": round(worst_day, 2),
                "avg_pos": round(avg_pos, 2),
                "avg_neg": round(avg_neg, 2),
                "n_days": m,
                "beta_spy": beta_spy,
                "r_squared": r_squared,
                "sys_risk": sys_risk,
                "idiosyncratic_risk": idiosyncratic_risk,
                "garch_10d_vol": garch_10d_vol,
                "mc_median": mc_median,
                "mc_p5": mc_p5,
                "mc_p95": mc_p95,
                "mc_up_prob": mc_up_prob,
                "cached_at": datetime.now().strftime('%Y-%m-%d'),
                "next_refresh": (datetime.now().replace(day=1) + timedelta(days=32)).replace(day=1).strftime('%Y-%m-%d'),
            }
        }
        # Get current live price (including after-hours)
        try:
            url_1m = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1m&range=1d&includePrePost=true"
            resp_1m = requests.get(url_1m, headers=YF_HEADERS, timeout=8)
            if resp_1m.status_code == 200:
                d1m = resp_1m.json()
                r1m = d1m.get('chart', {}).get('result', [None])[0]
                if r1m:
                    c1m = r1m.get('indicators', {}).get('quote', [{}])[0].get('close', [])
                    v1m = [c for c in c1m if c is not None]
                    if v1m:
                        last_close = closes[-1] if closes else 0
                        resp_data['current_price'] = round(v1m[-1], 2)
                        resp_data['current_change'] = round((v1m[-1] - last_close) / last_close * 100, 2) if last_close else 0
        except: pass

        # Cache until next month
        _technicals_cache[symbol.upper()] = (time.time(), resp_data)
        return jsonify(resp_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/history')
def api_history():
    days = min(request.args.get('days', 7, type=int), 90)
    start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM sector_data WHERE date >= ? ORDER BY date DESC, change_pct DESC",
            (start,)).fetchall()
        summaries = conn.execute(
            "SELECT * FROM daily_summary WHERE date >= ? ORDER BY date DESC",
            (start,)).fetchall()
    return jsonify({
        "sectors": [dict(r) for r in rows],
        "summaries": [dict(r) for r in summaries],
    })

@app.route('/api/refresh', methods=['POST'])
def api_refresh():
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    data = fetch_sector_data(yesterday)
    if data:
        save_to_db(data, yesterday)
        return jsonify({"status": "ok", "count": len(data), "date": yesterday})
    return jsonify({"status": "error"}), 500

@app.route('/api/backtest', methods=['POST'])
def api_backtest():
    """Run a quant backtest on a symbol with a given strategy."""
    body = request.get_json() or {}
    symbol = body.get('symbol', 'SPY').upper()
    strategy = body.get('strategy', 'ma_cross')
    params = body.get('params', {})

    # Fetch historical data from Yahoo Finance (more reliable)
    try:
        url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1y"
        resp = requests.get(url, headers=YF_HEADERS, timeout=15)
        if resp.status_code != 200:
            return jsonify({"error": "无法获取数据"}), 500
        data = resp.json()
        result_data = data.get('chart', {}).get('result', [None])[0]
        if not result_data:
            return jsonify({"error": "无历史数据"}), 500
        timestamps = result_data.get('timestamp', [])
        quotes = result_data.get('indicators', {}).get('quote', [{}])[0]
        closes = quotes.get('close', [])
        prices = [c for c in closes if c is not None]
        if len(prices) < 50:
            return jsonify({"error": f"数据点不足 (只有{len(prices)}个)"}), 500
        start_date = datetime.fromtimestamp(timestamps[0]).strftime('%Y-%m-%d') if timestamps else ''
        end_date = datetime.fromtimestamp(timestamps[-1]).strftime('%Y-%m-%d') if timestamps else ''
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    result = run_backtest(prices, strategy, params)
    if not result:
        return jsonify({"error": "回测失败"}), 500

    # Save to DB
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('''INSERT INTO backtest_results
            (strategy, symbol, start_date, end_date, total_return, sharpe, max_drawdown, win_rate, trades, params)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (strategy, symbol, start_date, end_date,
             result['total_return'], result['sharpe'], result['max_drawdown'],
             result['win_rate'], result['trades'], json.dumps(params)))
        conn.commit()

    result['symbol'] = symbol
    result['strategy'] = strategy
    result['params'] = params
    return jsonify(result)

@app.route('/api/backtest/history')
def api_backtest_history():
    limit = min(request.args.get('limit', 20, type=int), 100)
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM backtest_results ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/cn/backtest', methods=['POST'])
def api_cn_backtest():
    """Run backtest for CN market."""
    body = request.get_json() or {}
    symbol = body.get('symbol', '512480.SS').upper()
    strategy = body.get('strategy', 'ma_cross')
    params = body.get('params', {})
    try:
        url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1y"
        resp = requests.get(url, headers=YF_HEADERS, timeout=15)
        if resp.status_code != 200:
            return jsonify({"error": "无法获取数据"}), 500
        data = resp.json()
        result_data = data.get('chart', {}).get('result', [None])[0]
        if not result_data:
            return jsonify({"error": "无历史数据"}), 500
        timestamps = result_data.get('timestamp', [])
        quotes = result_data.get('indicators', {}).get('quote', [{}])[0]
        closes = quotes.get('close', [])
        prices = [c for c in closes if c is not None]
        if len(prices) < 50:
            return jsonify({"error": f"数据点不足 (只有{len(prices)}个)"}), 500
        start_date = datetime.fromtimestamp(timestamps[0]).strftime('%Y-%m-%d') if timestamps else ''
        end_date = datetime.fromtimestamp(timestamps[-1]).strftime('%Y-%m-%d') if timestamps else ''
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    result = run_backtest(prices, strategy, params)
    if not result:
        return jsonify({"error": "回测失败"}), 500
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('''INSERT INTO backtest_results
            (strategy, symbol, start_date, end_date, total_return, sharpe, max_drawdown, win_rate, trades, params)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (strategy, symbol, start_date, end_date,
             result['total_return'], result['sharpe'], result['max_drawdown'],
             result['win_rate'], result['trades'], json.dumps(params)))
        conn.commit()
    result['symbol'] = symbol
    result['strategy'] = strategy
    result['params'] = params
    return jsonify(result)

@app.route('/api/health')
def api_health():
    """Return system health + data freshness status."""
    now = datetime.now()
    status = {"status": "healthy", "time": now.isoformat(), "checks": {}}

    # US data freshness
    with sqlite3.connect(DB_PATH) as conn:
        us_latest = conn.execute("SELECT MAX(date), MAX(fetched_at) FROM sector_data").fetchone()
        us_count = conn.execute("SELECT COUNT(*) FROM sector_data WHERE date=(SELECT MAX(date) FROM sector_data)").fetchone()[0]
    us_date, us_fetched = us_latest
    # Stale = no data in the last 2 calendar days (covers weekends/holidays)
    us_today = now.strftime('%Y-%m-%d')
    us_yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    us_stale = us_date != us_today and us_date != us_yesterday
    status["checks"]["us"] = {
        "name": "美股数据",
        "latest_date": us_date, "sectors": us_count,
        "fetched_at": us_fetched,
        "next_update": _next_update_time('us'),
        "ok": not us_stale
    }

    # CN data freshness
    with sqlite3.connect(CN_DB_PATH) as conn:
        cn_latest = conn.execute("SELECT MAX(date), MAX(fetched_at) FROM sector_data").fetchone()
        cn_count = conn.execute("SELECT COUNT(*) FROM sector_data WHERE date=(SELECT MAX(date) FROM sector_data)").fetchone()[0]
    cn_date, cn_fetched = cn_latest
    cn_today = now.strftime('%Y-%m-%d')
    cn_yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    cn_stale = cn_date != cn_today and cn_date != cn_yesterday
    status["checks"]["cn"] = {
        "name": "A股数据", "latest_date": cn_date, "sectors": cn_count,
        "fetched_at": cn_fetched,
        "next_update": _next_update_time('cn'),
        "ok": not cn_stale
    }

    # KOL tweets
    kol_cache_ts = _kol_cache.get('us', (0,))[0]
    kol_age = (time.time() - kol_cache_ts) / 60 if kol_cache_ts else 999
    status["checks"]["kol"] = {
        "name": "KOL推文", "last_refresh": f"{kol_age:.0f}分钟前",
        "interval": "30分钟",
        "ok": kol_age < 45
    }

    # Stock reports
    report_count = len(_technicals_cache)
    status["checks"]["reports"] = {
        "name": "个股报告", "cached_reports": report_count,
        "next_refresh": now.replace(day=1).strftime('%Y-%m-%d') if now.day > 1 else now.strftime('%Y-%m-%d'),
        "ok": True
    }

    # Overall
    all_ok = all(c["ok"] for c in status["checks"].values())
    status["status"] = "healthy" if all_ok else "stale"
    return jsonify(status)

# ── Market Intelligence API ──
_market_intel_cache = {'ts': 0, 'events': [], 'ipos': []}

def _fetch_market_events():
    """Fetch major market-moving events from financial RSS."""
    events = []
    try:
        url = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5ESPX&region=US&lang=en-US"
        resp = requests.get(url, headers=YF_HEADERS, timeout=10)
        if resp.status_code != 200: return events
        root = ET.fromstring(resp.content)
        keywords = ['earnings','fed','powell','gdp','cpi','inflation','jobs','rate','rally','sell','surge','plunge',
                     'ipo','merger','acquisition','split','dividend','guidance','upgrade','downgrade','billion']
        for item in root.findall('.//item')[:15]:
            title_el = item.find('title'); desc_el = item.find('description')
            title = title_el.text.strip() if title_el is not None and title_el.text else ''
            desc = desc_el.text.strip() if desc_el is not None and desc_el.text else ''
            if not title: continue
            score = sum(1 for kw in keywords if kw.lower() in (title+desc).lower())
            if score >= 2:
                pub_el = item.find('pubDate')
                events.append({
                    'title': title, 'desc': desc[:200],
                    'published': _parse_rss_date(pub_el.text.strip()) if pub_el is not None and pub_el.text else '',
                    'impact': 'high' if score >= 4 else 'medium'
                })
    except Exception as e:
        print(f"[Market] Events fetch error: {e}")
    return events

def _fetch_upcoming_ipos():
    """Return upcoming IPO calendar (curated list, refreshed periodically)."""
    ipos = [
        {"date": "2026-08-04", "ticker": "ATTO", "name": "Attovia Therapeutics", "sector": "生物科技", "size": "$2.1亿", "price": "$15-17"},
        {"date": "2026-08-05", "ticker": "BRVE", "name": "Braveheart Bio", "sector": "生物科技", "size": "$3.2亿", "price": "$15-17"},
        {"date": "2026-08-05", "ticker": "VOGX", "name": "Vogenx", "sector": "生物科技", "size": "$8100万", "price": "$11-13"},
        {"date": "2026-08-05", "ticker": "RBC", "name": "River City Bank", "sector": "银行", "size": "$1.4亿", "price": "$48-51"},
        {"date": "2026-08-03", "ticker": "XIIIU", "name": "Churchill Capital XIII (SPAC)", "sector": "SPAC", "size": "$3.6亿", "price": "$10.00"},
        {"date": "2026-08月初", "ticker": "JMKE", "name": "Jersey Mike's Subs 🥪", "sector": "餐饮连锁", "size": "$10.9亿", "price": "$21-25"},
        {"date": "2026-08月初", "ticker": "REF", "name": "Reformation", "sector": "时尚零售", "size": "$2.4亿", "price": "$15-17"},
        {"date": "2026-08月", "ticker": "TBD", "name": "Cumberland Farms", "sector": "便利店", "size": "待定", "price": "待定"},
        {"date": "2026-H2", "ticker": "TBD", "name": "ByteDance/抖音 (港股)", "sector": "社交媒体", "size": "预计>100亿", "price": "待定"},
        {"date": "2026-10月", "ticker": "TBD", "name": "Unitree 宇树科技 (A股)", "sector": "机器人", "size": "预计50亿+", "price": "待定"},
    ]
    return ipos

@app.route('/api/market/intel')
def api_market_intel():
    """Return market events + IPO calendar (cached 4 hours)."""
    global _market_intel_cache
    now = time.time()
    if now - _market_intel_cache['ts'] < 14400:
        return jsonify({"events": _market_intel_cache['events'], "ipos": _market_intel_cache['ipos']})

    events = _fetch_market_events()
    ipos = _fetch_upcoming_ipos()
    _market_intel_cache = {'ts': now, 'events': events, 'ipos': ipos}
    return jsonify({"events": events, "ipos": ipos})

# ── News API ──
@app.route('/api/news')
def api_news():
    """Return aggregated sector-stock news for US market."""
    limit = request.args.get('limit', 25, type=int)
    try:
        articles = _fetch_all_news('us')
        return jsonify(articles[:limit])
    except Exception as e:
        print(f"[News] Error: {e}")
        return jsonify([])

@app.route('/api/cn/news')
def api_cn_news():
    """Return aggregated sector-stock news for CN market."""
    limit = request.args.get('limit', 25, type=int)
    try:
        articles = _fetch_all_news('cn')
        return jsonify(articles[:limit])
    except Exception as e:
        print(f"[News CN] Error: {e}")
        return jsonify([])

@app.route('/api/rotation')
def api_rotation():
    """Return sector performance across 1d/5d/20d/60d for heatmap."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        dates = [r[0] for r in conn.execute(
            "SELECT DISTINCT date FROM sector_data ORDER BY date DESC LIMIT 70"
        ).fetchall()]
        if len(dates) < 5:
            return jsonify({"error": "insufficient data"})
        d1, d5, d20, d60 = dates[0], dates[4] if len(dates) > 4 else dates[-1], dates[19] if len(dates) > 19 else dates[-1], dates[59] if len(dates) > 59 else dates[-1]
        rows = conn.execute(
            "SELECT date, name, category, close, change_pct FROM sector_data WHERE date IN (?,?,?,?) AND category != '指数'",
            (d1, d5, d20, d60)
        ).fetchall()
    # Group by sector name
    sectors = {}
    for r in rows:
        key = r['name']
        if key not in sectors:
            sectors[key] = {'name': key, 'category': r['category'], 'd1': None, 'd5': None, 'd20': None, 'd60': None}
        if r['date'] == d1: sectors[key]['d1'] = r['change_pct']
        elif r['date'] == d5: sectors[key]['d5'] = r['change_pct']
        elif r['date'] == d20: sectors[key]['d20'] = r['change_pct']
        elif r['date'] == d60: sectors[key]['d60'] = r['change_pct']
    # Compute returns for multi-day windows using close prices
    with sqlite3.connect(DB_PATH) as conn:
        for key in sectors:
            for days, label in [(5, 'd5'), (20, 'd20'), (60, 'd60')]:
                if sectors[key][label] is None:
                    prev_date = dates[days-1] if len(dates) > days-1 else dates[-1]
                    r = conn.execute(
                        "SELECT close FROM sector_data WHERE name=? AND date=?",
                        (key, prev_date)
                    ).fetchone()
                    if r:
                        c1 = conn.execute("SELECT close FROM sector_data WHERE name=? AND date=?", (key, d1)).fetchone()
                        if c1 and r[0] and r[0] > 0:
                            sectors[key][label] = round(((c1[0] - r[0]) / r[0]) * 100, 2)
    return jsonify({"dates": {"d1": d1, "d5": d5, "d20": d20, "d60": d60}, "sectors": list(sectors.values())})

@app.route('/api/macro')
def api_macro():
    """Fetch key macro indicators."""
    indicators = {}
    # VIX
    try:
        r = requests.get("https://query2.finance.yahoo.com/v8/finance/chart/%5EVIX?interval=1d&range=2d", timeout=10)
        d = r.json()
        if d['chart']['result']:
            meta = d['chart']['result'][0]['meta']
            indicators['vix'] = round(meta.get('regularMarketPrice', 0), 2)
            indicators['vix_prev'] = round(meta.get('chartPreviousClose', 0), 2)
    except: pass
    # DXY
    try:
        r = requests.get("https://query2.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?interval=1d&range=2d", timeout=10)
        d = r.json()
        if d['chart']['result']:
            meta = d['chart']['result'][0]['meta']
            indicators['dxy'] = round(meta.get('regularMarketPrice', 0), 2)
            indicators['dxy_prev'] = round(meta.get('chartPreviousClose', 0), 2)
    except: pass
    # 10Y Treasury
    try:
        r = requests.get("https://query2.finance.yahoo.com/v8/finance/chart/%5ETNX?interval=1d&range=2d", timeout=10)
        d = r.json()
        if d['chart']['result']:
            meta = d['chart']['result'][0]['meta']
            indicators['tnx'] = round(meta.get('regularMarketPrice', 0), 2)
            indicators['tnx_prev'] = round(meta.get('chartPreviousClose', 0), 2)
    except: pass
    # Total market volume vs 20-day avg (from SPY)
    try:
        with sqlite3.connect(DB_PATH) as conn:
            vols = conn.execute(
                "SELECT volume FROM sector_data WHERE symbol='SPY' ORDER BY date DESC LIMIT 21"
            ).fetchall()
            if len(vols) >= 2:
                today_vol = vols[0][0]
                avg20 = sum(v[0] for v in vols[1:21]) / max(1, len(vols[1:21]))
                indicators['spy_volume'] = int(today_vol)
                indicators['spy_volume_avg20'] = int(avg20)
                indicators['spy_volume_ratio'] = round((today_vol / avg20 - 1) * 100, 1) if avg20 > 0 else 0
    except: pass
    return jsonify(indicators)

@app.route('/api/correlation')
def api_correlation():
    """Return sector correlation matrix based on 60-day returns."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        dates = [r[0] for r in conn.execute(
            "SELECT DISTINCT date FROM sector_data ORDER BY date DESC LIMIT 61"
        ).fetchall()]
        if len(dates) < 20:
            return jsonify({"error": "insufficient data"})
        dates.reverse()  # asc
        rows = conn.execute(
            f"SELECT date, name, close FROM sector_data WHERE date IN ({','.join('?'*len(dates))}) AND category != '指数'",
            dates
        ).fetchall()
    # Build price series per sector
    series = {}
    for r in rows:
        series.setdefault(r['name'], {})[r['date']] = r['close']
    # Compute returns
    rets = {}
    for name, prices in series.items():
        sorted_dates = sorted(prices.keys())
        if len(sorted_dates) < 10: continue
        r = []
        for i in range(1, len(sorted_dates)):
            if prices[sorted_dates[i-1]] > 0:
                r.append((prices[sorted_dates[i]] - prices[sorted_dates[i-1]]) / prices[sorted_dates[i-1]])
        if len(r) >= 5:
            rets[name] = r
    # Compute correlation matrix
    names = sorted(rets.keys())
    n = len(names)
    matrix = [[1.0]*n for _ in range(n)]
    for i in range(n):
        for j in range(i+1, n):
            a, b = rets[names[i]], rets[names[j]]
            m = min(len(a), len(b))
            if m < 3:
                matrix[i][j] = matrix[j][i] = 0
                continue
            a_s, b_s = a[-m:], b[-m:]
            ma, mb = sum(a_s)/m, sum(b_s)/m
            num = sum((a_s[k]-ma)*(b_s[k]-mb) for k in range(m))
            den = (sum((x-ma)**2 for x in a_s) * sum((x-mb)**2 for x in b_s)) ** 0.5
            corr = round(num/den, 2) if den > 0 else 0
            matrix[i][j] = matrix[j][i] = corr
    return jsonify({"names": names, "matrix": matrix})

# ── China A-Share API Routes ────────────────────────────────
def _next_update_time(market):
    now = datetime.now()
    if market == 'us':
        t1 = now.replace(hour=3, minute=30, second=0, microsecond=0)
        t2 = now.replace(hour=4, minute=30, second=0, microsecond=0)
        if now < t1: return t1.strftime('%m-%d %H:%M')
        if now < t2: return t2.strftime('%m-%d %H:%M')
        return (t1 + timedelta(days=1)).strftime('%m-%d %H:%M')
    else:
        t1 = now.replace(hour=14, minute=30, second=0, microsecond=0)
        t2 = now.replace(hour=15, minute=30, second=0, microsecond=0)
        if now < t1: return t1.strftime('%m-%d %H:%M')
        if now < t2: return t2.strftime('%m-%d %H:%M')
        return (t1 + timedelta(days=1)).strftime('%m-%d %H:%M')
        return (t14 + timedelta(days=1)).strftime('%m-%d %H:%M')

@app.route('/api/cn/latest')
def api_cn_latest():
    target_date = request.args.get('date', '')
    force = request.args.get('force', '0') == '1'
    auto_refresh = not target_date
    if not target_date:
        with sqlite3.connect(CN_DB_PATH) as conn:
            latest_date = conn.execute("SELECT MAX(date) FROM sector_data").fetchone()[0]
        target_date = latest_date or (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    if force:
        data = fetch_cn_sector_data(target_date)
        if data:
            save_cn_to_db(data, target_date)
            return jsonify({"date": target_date, "sectors": data, "fetched": "live", "updated_at": datetime.now().strftime('%m-%d %H:%M'), "next_update": _next_update_time('cn')})
        return jsonify({"error": "no data", "date": target_date})
    with sqlite3.connect(CN_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT symbol, name, category, close, change_pct, volume, fetched_at FROM sector_data WHERE date = ? ORDER BY change_pct DESC", (target_date,)
        ).fetchall()
        summary = conn.execute(
            "SELECT * FROM daily_summary WHERE date = ?", (target_date,)
        ).fetchone()
    if not rows:
        data = fetch_cn_sector_data(target_date)
        if data:
            save_cn_to_db(data, target_date)
            return jsonify({"date": target_date, "sectors": data, "fetched": "live", "updated_at": datetime.now().strftime('%m-%d %H:%M'), "next_update": _next_update_time('cn')})
        return jsonify({"error": "no data", "date": target_date})
    # Auto-refresh stale data when requesting latest
    if auto_refresh and rows:
        try:
            latest_fetched = rows[0]['fetched_at']
            if latest_fetched:
                fetched_dt = datetime.strptime(latest_fetched, '%Y-%m-%d %H:%M:%S')
                if (datetime.now() - fetched_dt).total_seconds() > 2 * 3600:
                    data = fetch_cn_sector_data(target_date)
                    if data:
                        save_cn_to_db(data, target_date)
                        with sqlite3.connect(CN_DB_PATH) as conn:
                            conn.row_factory = sqlite3.Row
                            rows = conn.execute(
                                "SELECT symbol, name, category, close, change_pct, volume, fetched_at FROM sector_data WHERE date = ? ORDER BY change_pct DESC", (target_date,)
                            ).fetchall()
                            summary = conn.execute("SELECT * FROM daily_summary WHERE date = ?", (target_date,)).fetchone()
                        return jsonify({
                            "date": target_date,
                            "sectors": [dict(r) for r in rows],
                            "summary": dict(summary) if summary else None,
                            "fetched": "live",
                            "updated_at": datetime.now().strftime('%m-%d %H:%M'),
                            "next_update": _next_update_time('cn')
                        })
        except Exception as e:
            app.logger.warning(f"CN auto-refresh failed: {e}")
    updated_at = max(r['fetched_at'] for r in rows)[:16].replace('T', ' ') if rows else '-'
    return jsonify({
        "date": target_date,
        "sectors": [dict(r) for r in rows],
        "summary": dict(summary) if summary else None,
        "fetched": "cached",
        "updated_at": updated_at,
        "next_update": _next_update_time('cn')
    })

@app.route('/api/cn/dates')
def api_cn_dates():
    with sqlite3.connect(CN_DB_PATH) as conn:
        dates = conn.execute("SELECT DISTINCT date FROM sector_data ORDER BY date DESC").fetchall()
    return jsonify([d[0] for d in dates])

@app.route('/api/cn/sector/<sector_name>')
def api_cn_sector_detail(sector_name):
    stocks = CN_SECTOR_STOCKS.get(sector_name, [])
    if not stocks:
        return jsonify({"error": "sector not found", "stocks": []})
    quotes = fetch_cn_stock_quotes(stocks)
    return jsonify({"sector": sector_name, "stocks": quotes})

@app.route('/api/cn/stock/<symbol>/history')
def api_cn_stock_history(symbol):
    try:
        url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=3mo"
        resp = requests.get(url, headers=YF_HEADERS, timeout=15)
        if resp.status_code != 200:
            return jsonify({"error": "fetch failed"}), 500
        data = resp.json()
        result = data.get('chart', {}).get('result', [None])[0]
        if not result:
            return jsonify({"error": "no data"}), 500
        timestamps = result.get('timestamp', [])
        quotes = result.get('indicators', {}).get('quote', [{}])[0]
        ohlc = []
        for i in range(len(timestamps)):
            ohlc.append([
                datetime.fromtimestamp(timestamps[i]).strftime('%Y-%m-%d'),
                quotes['open'][i] or 0,
                quotes['close'][i] or 0,
                quotes['low'][i] or 0,
                quotes['high'][i] or 0,
                quotes['volume'][i] or 0,
            ])
        return jsonify({"symbol": symbol, "ohlc": ohlc})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/cn/rotation')
def api_cn_rotation():
    with sqlite3.connect(CN_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        dates = [r[0] for r in conn.execute("SELECT DISTINCT date FROM sector_data ORDER BY date DESC LIMIT 70").fetchall()]
        if len(dates) < 5:
            return jsonify({"error": "insufficient data"})
        d1, d5, d20, d60 = dates[0], dates[4] if len(dates) > 4 else dates[-1], dates[19] if len(dates) > 19 else dates[-1], dates[59] if len(dates) > 59 else dates[-1]
        rows = conn.execute(
            "SELECT date, name, category, close, change_pct FROM sector_data WHERE date IN (?,?,?,?) AND category != '指数'",
            (d1, d5, d20, d60)
        ).fetchall()
    sectors = {}
    for r in rows:
        key = r['name']
        if key not in sectors:
            sectors[key] = {'name': key, 'category': r['category'], 'd1': None, 'd5': None, 'd20': None, 'd60': None}
        if r['date'] == d1: sectors[key]['d1'] = r['change_pct']
        elif r['date'] == d5: sectors[key]['d5'] = r['change_pct']
        elif r['date'] == d20: sectors[key]['d20'] = r['change_pct']
        elif r['date'] == d60: sectors[key]['d60'] = r['change_pct']
    with sqlite3.connect(CN_DB_PATH) as conn:
        for key in sectors:
            for days, label in [(5, 'd5'), (20, 'd20'), (60, 'd60')]:
                if sectors[key][label] is None:
                    prev_date = dates[days-1] if len(dates) > days-1 else dates[-1]
                    r = conn.execute("SELECT close FROM sector_data WHERE name=? AND date=?", (key, prev_date)).fetchone()
                    if r:
                        c1 = conn.execute("SELECT close FROM sector_data WHERE name=? AND date=?", (key, d1)).fetchone()
                        if c1 and r[0] and r[0] > 0:
                            sectors[key][label] = round(((c1[0] - r[0]) / r[0]) * 100, 2)
    return jsonify({"dates": {"d1": d1, "d5": d5, "d20": d20, "d60": d60}, "sectors": list(sectors.values())})

@app.route('/api/cn/correlation')
def api_cn_correlation():
    with sqlite3.connect(CN_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        dates = [r[0] for r in conn.execute("SELECT DISTINCT date FROM sector_data ORDER BY date DESC LIMIT 61").fetchall()]
        if len(dates) < 20:
            return jsonify({"error": "insufficient data"})
        dates.reverse()
        rows = conn.execute(
            f"SELECT date, name, close FROM sector_data WHERE date IN ({','.join('?'*len(dates))}) AND category != '指数'",
            dates
        ).fetchall()
    series = {}
    for r in rows:
        series.setdefault(r['name'], {})[r['date']] = r['close']
    rets = {}
    for name, prices in series.items():
        sorted_dates = sorted(prices.keys())
        if len(sorted_dates) < 10: continue
        r = []
        for i in range(1, len(sorted_dates)):
            if prices[sorted_dates[i-1]] > 0:
                r.append((prices[sorted_dates[i]] - prices[sorted_dates[i-1]]) / prices[sorted_dates[i-1]])
        if len(r) >= 5:
            rets[name] = r
    names = sorted(rets.keys())
    n = len(names)
    matrix = [[1.0]*n for _ in range(n)]
    for i in range(n):
        for j in range(i+1, n):
            a, b = rets[names[i]], rets[names[j]]
            m = min(len(a), len(b))
            if m < 3:
                matrix[i][j] = matrix[j][i] = 0
                continue
            a_s, b_s = a[-m:], b[-m:]
            ma, mb = sum(a_s)/m, sum(b_s)/m
            num = sum((a_s[k]-ma)*(b_s[k]-mb) for k in range(m))
            den = (sum((x-ma)**2 for x in a_s) * sum((x-mb)**2 for x in b_s)) ** 0.5
            corr = round(num/den, 2) if den > 0 else 0
            matrix[i][j] = matrix[j][i] = corr
    return jsonify({"names": names, "matrix": matrix})

@app.errorhandler(404)
def e404(e): return jsonify({"error": "404"}), 404
@app.errorhandler(429)
def e429(e): return jsonify({"error": "rate limited"}), 429

if __name__ == '__main__':
    init_db()
    init_cn_db()
    # Seed A-share DB from bundled backup if data is insufficient (Railway volume may be empty)
    backup_path = os.path.join(os.path.dirname(__file__), 'static', 'cn_sectors.db.bak')
    if os.path.exists(backup_path):
        try:
            with sqlite3.connect(CN_DB_PATH) as conn:
                cnt = conn.execute("SELECT COUNT(*) FROM sector_data").fetchone()[0]
            if cnt < 100:
                import shutil
                shutil.copy(backup_path, CN_DB_PATH)
                print(f"  ✅ Seeded cn_sectors.db from backup ({cnt} → restored)")
        except Exception as e:
            print(f"  ⚠️ DB seed skipped: {e}")
    print(f"\n  US + China A-Share Sector Tracker v3.0")
    print(f"  http://localhost:8080")
    print(f"  admin / {ADMIN_PASS}\n")
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
