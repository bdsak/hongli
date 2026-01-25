import akshare as ak
import pandas as pd
import requests
import os
import time
import json
import concurrent.futures
from datetime import datetime, timedelta
import pytz
import logging

# ======================
# 日志设置
# ======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('monitor_full.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY")
CACHE_DIR = 'cache'
CONSECUTIVE_DAYS_FILE = os.path.join(CACHE_DIR, 'consecutive_hits.json')
os.makedirs(CACHE_DIR, exist_ok=True)

# ======================
# 判断交易日
# ======================
def is_trade_day():
    today = datetime.now(pytz.timezone("Asia/Shanghai")).date()
    try:
        cal = ak.tool_trade_date_hist_sina()
        trade_dates = set()
        for d in cal["trade_date"]:
            # 尝试 YYYYMMDD
            try:
                trade_dates.add(datetime.strptime(str(d), "%Y%m%d").date())
            except:
                # 尝试 YYYY-MM-DD
                try:
                    trade_dates.add(datetime.strptime(str(d), "%Y-%m-%d").date())
                except:
                    continue
        return today in trade_dates
    except Exception as e:
        logger.warning(f"交易日判断失败: {e}")
        return today.weekday() < 5  # 默认工作日

# ======================
# 缓存
# ======================
class DataCache:
    def __init__(self, cache_dir=CACHE_DIR):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

    def get(self, code):
        path = os.path.join(self.cache_dir, f"{code}.json")
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                cache_time = datetime.fromisoformat(data["cache_time"])
                if (datetime.now() - cache_time).total_seconds() < 3600:
                    return data["data"]
            except:
                return None
        return None

    def set(self, code, data):
        path = os.path.join(self.cache_dir, f"{code}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {"cache_time": datetime.now().isoformat(), "data": data},
                f,
                ensure_ascii=False,
                indent=2
            )

# ======================
# Server酱
# ======================
def send_wechat(title, content):
    if not SERVER_CHAN_KEY:
        logger.warning("未配置 SERVER_CHAN_KEY")
        return
    url = f"https://sctapi.ftqq.com/{SERVER_CHAN_KEY}.send"
    data = {
        "title": title[:32],
        "desp": content,
        "desp_type": "markdown"
    }
    try:
        requests.post(url, data=data, timeout=15)
        logger.info("微信通知已发送")
    except Exception as e:
        logger.error(f"通知失败: {e}")

# ======================
# 获取成分股
# ======================
def get_all_index_stocks(index_code, index_name):
    try:
        df = ak.index_stock_cons(symbol=index_code)
        stocks = [(str(row.iloc[0]), row.iloc[1] if len(row) > 1 else "") for _, row in df.iterrows()]
        logger.info(f"{index_name} 获取成分股 {len(stocks)} 只")
        return stocks
    except Exception as e:
        logger.error(f"{index_name} 成分股获取失败: {e}")
        return []

# ======================
# 获取股票行情 + 年线
# ======================
def get_stock_data_with_cache(code, name, cache):
    cached = cache.get(code)
    if cached:
        return cached
    try:
        end = datetime.now()
        start = end - timedelta(days=420)
        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            adjust="qfq"
        )
        if df is None or df.empty or len(df) < 260:
            return None
        df["MA250"] = df["收盘"].rolling(250).mean()
        df = df.dropna()
        if df.empty:
            return None
        latest = df.iloc[-1]
        result = {
            "code": code,
            "name": name,
            "close": float(latest["收盘"]),
            "ma250": float(latest["MA250"]),
            "date": str(latest["日期"])
        }
        cache.set(code, result)
        return result
    except Exception as e:
        logger.debug(f"{code} 行情失败: {e}")
        return None

# ======================
# 判断年线偏离
# ======================
def check_stock_condition(stock, threshold=0.06):
    close = stock["close"]
    ma250 = stock["ma250"]
    deviation = (close - ma250) / ma250
    if -threshold <= deviation <= 0.02:
        stock["deviation"] = deviation
        stock["deviation_percent"] = deviation * 100
        return stock
    return None

# ======================
# 连续命中跟踪
# ======================
def load_consecutive_hits():
    if os.path.exists(CONSECUTIVE_DAYS_FILE):
        try:
            with open(CONSECUTIVE_DAYS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_consecutive_hits(data):
    with open(CONSECUTIVE_DAYS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def update_consecutive_hits(hits):
    data = load_consecutive_hits()
    today_str = datetime.now().strftime("%Y-%m-%d")
    today_hits = {h["code"]: h for h in hits}

    # 更新计数
    new_data = {}
    for code, stock in today_hits.items():
        prev_count = data.get(code, {}).get("consecutive", 0)
        new_data[code] = {
            "name": stock["name"],
            "consecutive": prev_count + 1,
            "index": stock.get("index", "")
        }
    save_consecutive_hits(new_data)
    return new_data

# ======================
# 批量处理
# ======================
def process_stocks(stocks, index_name):
    cache = DataCache()
    hits = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(get_stock_data_with_cache, c, n, cache): (c, n) for c, n in stocks}
        for future in concurrent.futures.as_completed(futures):
            data = future.result()
            if not data:
                continue
            hit = check_stock_condition(data)
            if hit:
                hit["index"] = index_name
                hits.append(hit)
                logger.info(f"命中 {hit['code']} {hit['name']} {hit['deviation_percent']:.2f}%")
    return hits

# ======================
# 主程序
# ======================
def main():
    logger.info("红利指数监控启动")
    trade_day = is_trade_day()
    if not trade_day:
        logger.info("今天非交易日，无行情更新")
    else:
        logger.info("今天有行情更新")

    index_map = {
        "中证红利": "000922",
        "上证红利": "000015",
        "深证红利": "399324"
    }

    all_hits = []
    total = 0
    for name, code in index_map.items():
        stocks = get_all_index_stocks(code, name)
        total += len(stocks)
        hits = process_stocks(stocks, name) if trade_day else []
        all_hits.extend(hits)
        time.sleep(2)

    # 更新连续命中天数
    consecutive_data = update_consecutive_hits(all_hits)

    # 构建推送内容
    content = f"## 红利指数年线提醒（{datetime.now().strftime('%Y-%m-%d')}）\n\n"
    content += f"- 今天状态: {'有行情更新' if trade_day else '非交易日'}\n"
    content += f"- 检查数量: {total}\n\n"

    if not all_hits:
        content += "未发现股票接近年线\n"
    else:
        for h in sorted(all_hits, key=lambda x: x["deviation_percent"]):
            code = h["code"]
            consecutive = consecutive_data.get(code, {}).get("consecutive", 1)
            content += (
                f"- {h['code']} {h['name']}（{h['index']}）\n"
                f"  收盘 {h['close']:.2f} ｜ 年线 {h['ma250']:.2f}\n"
                f"  偏离 {h['deviation_percent']:.2f}% ｜ 连续命中 {consecutive} 天\n\n"
            )

    send_wechat("红利指数监控提醒", content)
    logger.info("运行完成")

if __name__ == "__main__":
    main()
