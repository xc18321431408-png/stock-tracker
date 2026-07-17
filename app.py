"""
US Stock Market Sector Tracker v2.0
- 40+ sub-industry ETFs for granular sector tracking
- Top 10 constituent stocks per sector
- Quantitative trading backtest platform
- Security protections
"""
import os, json, sqlite3, secrets, re, time, requests, math
from datetime import datetime, timedelta
from functools import wraps
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

SA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

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
    ("公用事业", "URA", "铀/核能"), ("公用事业", "PHO", "水务"),
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
        ("CWEN","Clearway","美国清洁能源IPP"), ("AY","Atlantica","全球可再生能源资产"),
    ],
    "航空航天": [
        ("BA","波音","全球最大航空航天公司，客机+军工"), ("RTX","雷神技术","普惠发动机+柯林斯航空+导弹"),
        ("LMT","洛克希德马丁","全球最大军工企业，F-35制造商"), ("GD","通用动力","军用车辆/核潜艇/湾流公务机"),
        ("NOC","诺斯罗普格鲁曼","B-2/B-21隐身轰炸机制造商"), ("HWM","Howmet","航空发动机精密铸件领导者"),
        ("TDG","TransDigm","航空零部件售后市场垄断者"), ("HEI","海科","航空电子/MRO零部件"),
    ],
    "国防军工": [
        ("LMT","洛克希德马丁","F-35,导弹防御,太空系统"), ("RTX","雷神","爱国者导弹,发动机,传感器"),
        ("GD","通用动力","核潜艇,坦克,湾流"), ("NOC","诺斯罗普","隐身轰炸机,太空系统"),
        ("LHX","L3哈里斯","军用通信/电子战/ISR"), ("HII","亨廷顿英戈尔斯","美国最大军用造船商(航母/核潜艇)"),
        ("KTOS","Kratos","高性价比无人机/靶机"), ("AVAV","AeroVironment","小型无人机/巡飞弹"),
    ],
    "零售": [
        ("AMZN","亚马逊","全球最大电商+云计算"), ("WMT","沃尔玛","全球最大实体零售商"),
        ("COST","好市多","会员制仓储零售之王"), ("HD","家得宝","全球最大家居建材零售商"),
        ("LOW","劳氏","第二大家居建材连锁"), ("TGT","塔吉特","时尚折扣百货零售商"),
        ("TJX","TJX","全球最大折扣服装/家居零售商"), ("ROST","罗斯百货","折扣服装连锁"),
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
        ("TOL","Toll Brothers","美国最大豪华住宅建筑商"), ("KBH","KB Home","定制化住宅建筑商"),
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
        ("BMBL","Bumble","女性优先约会社交平台"), ("GRND","Grindr","LGBTQ+社交平台"),
    ],
    "金矿": [
        ("NEM","纽蒙特","全球最大金矿企业"), ("GOLD","巴里克黄金","全球第二大金矿"),
        ("AEM","Agnico Eagle","加拿大金矿龙头"), ("FNV","Franco-Nevada","黄金权利金公司(不采矿,只收租)"),
        ("WPM","Wheaton","白银/黄金权利金公司"), ("GFI","Gold Fields","南非金矿巨头"),
    ],
    "综合REITs": [
        ("PLD","Prologis","全球最大工业物流REIT"), ("AMT","American Tower","全球最大通信铁塔REIT"),
        ("EQIX","Equinix","全球最大数据中心REIT"), ("SPG","Simon","全球最大购物中心REIT"),
        ("O","Realty Income","净租赁REIT之王(按月派息)"), ("PSA","Public Storage","全球最大自助仓储REIT"),
        ("WELL","Welltower","医疗养老REIT领导者"),
    ],
    "存储芯片": [
        ("MU","美光科技","全球DRAM/NAND存储三巨头，HBM3E供不应求"),
        ("WDC","西部数据","全球最大硬盘+闪存制造商，拆分闪存业务"),
        ("STX","希捷科技","全球最大机械硬盘制造商，HAMR技术领先"),
        ("NTAP","NetApp","企业级全闪存/混合存储阵列领导者"),
        ("PSTG","Pure Storage","全闪存阵列先驱，Evergreen订阅模式"),
        ("SGH","SMART Global","特种内存/CXL/AI存储解决方案"),
        ("RMBL","Rambus","高速内存接口芯片IP授权领导者"),
        ("FORM","FormFactor","存储芯片探针卡/测试设备龙头"),
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
        ("ONTO","Onto Innovation","先进封装检测设备"), ("ACLS","Axcelis","离子注入设备龙头"),
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
    "铀/核能": [
        ("CCJ","Cameco","全球最大上市铀矿公司"), ("UEC","Uranium Energy","美国铀矿+ISR技术"),
        ("BWXT","BWX Technologies","核反应堆部件+核燃料"), ("CEG","Constellation Energy","美国最大核电运营商"),
        ("VST","Vistra","核电+可再生能源+储能"), ("TLN","Talen Energy","核电+数据中心供电"),
    ],
    "光模块光通信": [
        ("COHR","Coherent","全球光模块龙头，800G/1.6T光器件先驱"),
        ("LITE","Lumentum","3D传感+光通信激光器芯片领导者"),
        ("CIEN","Ciena","光传输/光网络设备全球领导者"),
        ("JNPR","瞻博","高端路由器/交换机/光网络"),
        ("FN","Fabrinet","光模块OEM代工龙头,NVidia供应商"),
        ("AAOI","Applied Optoelectronics","光模块/光纤接入设备制造商"),
        ("INFN","Infinera","相干光传输设备领导者"),
        ("HLIT","Harmonic","视频+宽带光纤接入解决方案"),
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
        ("PATH","UiPath","RPA机器人流程自动化领导者"), ("HOOD","Robinhood","零佣金交易平台,Z世代券商"),
    ],
    "机器人AI": [
        ("NVDA","英伟达","机器人AI芯片+Isaac平台"), ("ISRG","直觉外科","达芬奇手术机器人全球装机>8000台"),
        ("TSLA","特斯拉","Optimus人形机器人"), ("TER","泰瑞达","工业机器人+半导体测试设备"),
        ("PATH","UiPath","软件RPA机器人流程自动化"), ("ROK","罗克韦尔","工业自动化+智能制造领导者"),
        ("EMR","艾默生","工业自动化+过程控制全球巨头"), ("ZBRA","斑马技术","仓储/物流机器人+自动识别"),
        ("CGNX","康耐视","机器视觉/工业读码系统全球领导者"),
    ],
}

# ── DB Init ─────────────────────────────────────────────────
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
        conn.commit()

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
    results = []
    for category, symbol, name in SUB_SECTORS:
        try:
            url = f"https://api.stockanalysis.com/api/symbol/s/{symbol}/history?range=5d"
            resp = requests.get(url, headers=SA_HEADERS, timeout=15)
            if resp.status_code != 200: continue
            data = resp.json()
            if 'data' not in data or not data['data']: continue
            items = data['data']
            if len(items) < 2: continue
            latest, prev = items[0], items[1]
            close = latest.get('c', 0)
            prev_close = prev.get('c', close)
            change_pct = ((close - prev_close) / prev_close) * 100 if prev_close else 0
            results.append({
                'symbol': symbol, 'name': name, 'category': category,
                'open': round(latest.get('o', 0), 2), 'high': round(latest.get('h', 0), 2),
                'low': round(latest.get('l', 0), 2), 'close': round(close, 2),
                'change_pct': round(change_pct, 2), 'volume': int(latest.get('v', 0)),
            })
            time.sleep(0.25)
        except Exception as e:
            app.logger.warning(f"Fetch fail {symbol}: {e}")
    return results

def fetch_stock_quotes(stock_list):
    """Fetch real-time quotes for a list of (symbol, name, desc) tuples."""
    results = []
    for item in stock_list:
        sym, name, desc = item[0], item[1] if len(item)>1 else sym, item[2] if len(item)>2 else ""
        try:
            url = f"https://api.stockanalysis.com/api/symbol/s/{sym}/history?range=5d"
            resp = requests.get(url, headers=SA_HEADERS, timeout=10)
            if resp.status_code != 200: continue
            data = resp.json()
            if 'data' not in data or not data['data']: continue
            items = data['data']
            if len(items) < 2: continue
            latest, prev = items[0], items[1]
            close = latest.get('c', 0)
            prev_close = prev.get('c', close)
            change_pct = ((close - prev_close) / prev_close) * 100 if prev_close else 0
            results.append({
                'symbol': sym, 'name': name, 'desc': desc,
                'close': round(close, 2), 'change_pct': round(change_pct, 2),
                'volume': int(latest.get('v', 0)),
            })
            time.sleep(0.15)
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

def daily_fetch_job():
    y = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    data = fetch_sector_data(y)
    if data: save_to_db(data, y)

scheduler = BackgroundScheduler()
scheduler.add_job(daily_fetch_job, 'cron', hour=7, minute=0)
scheduler.start()

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
    }

# ── Routes ───────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/latest')
def api_latest():
    target_date = request.args.get('date', '')
    if not target_date:
        target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM sector_data WHERE date = ? ORDER BY change_pct DESC", (target_date,)
        ).fetchall()
        summary = conn.execute(
            "SELECT * FROM daily_summary WHERE date = ?", (target_date,)
        ).fetchone()
    if not rows:
        data = fetch_sector_data(target_date)
        if data:
            save_to_db(data, target_date)
            return jsonify({"date": target_date, "sectors": data, "fetched": "live"})
        return jsonify({"error": "no data", "date": target_date})
    return jsonify({
        "date": target_date,
        "sectors": [dict(r) for r in rows],
        "summary": dict(summary) if summary else None,
        "fetched": "cached"
    })

@app.route('/api/dates')
def api_dates():
    """Get list of all available trading dates."""
    with sqlite3.connect(DB_PATH) as conn:
        dates = conn.execute(
            "SELECT DISTINCT date FROM sector_data ORDER BY date DESC"
        ).fetchall()
    return jsonify([d[0] for d in dates])

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
    """Return OHLC history for candlestick chart (3 months)."""
    try:
        url = f"https://api.stockanalysis.com/api/symbol/s/{symbol}/history?range=3mo"
        resp = requests.get(url, headers=SA_HEADERS, timeout=15)
        if resp.status_code != 200:
            return jsonify({"error": "fetch failed"}), 500
        data = resp.json()
        if 'data' not in data or not data['data']:
            return jsonify({"error": "no data"}), 500
        # Return as [date, open, close, low, high] for ECharts candlestick
        ohlc = []
        for item in reversed(data['data']):  # reverse to ascending order
            ohlc.append([
                item.get('t', ''),
                item.get('o', 0),
                item.get('c', 0),
                item.get('l', 0),
                item.get('h', 0),
                item.get('v', 0),
            ])
        return jsonify({"symbol": symbol, "ohlc": ohlc})
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

    # Fetch historical data
    url = f"https://api.stockanalysis.com/api/symbol/s/{symbol}/history?range=1y"
    try:
        resp = requests.get(url, headers=SA_HEADERS, timeout=15)
        if resp.status_code != 200:
            return jsonify({"error": "无法获取数据"}), 500
        data = resp.json()
        if 'data' not in data or not data['data']:
            return jsonify({"error": "无历史数据"}), 500
        prices = [item['c'] for item in data['data'] if item.get('c')]
        if len(prices) < 50:
            return jsonify({"error": f"数据点不足 (只有{len(prices)}个)"}), 500
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
            (strategy, symbol, data['data'][0]['t'], data['data'][-1]['t'],
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

@app.route('/api/health')
def api_health():
    with sqlite3.connect(DB_PATH) as conn:
        count = conn.execute("SELECT COUNT(*) FROM sector_data").fetchone()[0]
    return jsonify({"status": "healthy", "records": count, "time": datetime.now().isoformat()})

@app.errorhandler(404)
def e404(e): return jsonify({"error": "404"}), 404
@app.errorhandler(429)
def e429(e): return jsonify({"error": "rate limited"}), 429

if __name__ == '__main__':
    init_db()
    print(f"\n  US Stock Sector Tracker v2.0")
    print(f"  http://localhost:8080")
    print(f"  admin / {ADMIN_PASS}\n")
    app.run(host='0.0.0.0', port=8080, debug=False)
