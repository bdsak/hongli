import akshare as ak
import pandas as pd
import requests
import os
import time
import json
import concurrent.futures
from datetime import datetime, timedelta
import logging
import pytz

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

# ======================
# 缓存
# ======================
class DataCache:
    def __init__(self, cache_dir='cache'):
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
# 成分股
# ======================
def get_all_index_stocks(index_code, index_name):
    try:
        df = ak.index_stock_cons(symbol=index_code)
        stocks = []
        for _, row in df.iterrows():
            code = str(row.iloc[0])
            name = row.iloc[1] if len(row) > 1 else ""
            stocks.append((code, name))
        logger.info(f"{index_name} 获取成分股 {len(stocks)} 只")
        return stocks
    except Exception as e:
        logger.error(f"{index_name} 成分股获取失败: {e}")
        return []

# ======================
# 行情 + 年线
# ======================
def get_stock_data_with_cache(code, name, cache):
    cached = cache.get(code)
    if cached:
        return cached

    try:
        symbol = code

        end = datetime.now()
        start = end - timedelta(days=420)

        df = ak.stock_zh_a_hist(
            symbol=symbol,
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
# 条件判断
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
# 批量处理
# ======================
def process_stocks(stocks, index_name):
    cache = DataCache()
    hits = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        futures = {
            pool.submit(get_stock_data_with_cache, c, n, cache): (c, n)
            for c, n in stocks
        }

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
# 是否交易日
# ======================
def is_trade_day():
    today = datetime.now(pytz.timezone("Asia/Shanghai")).date()
    # 使用 akshare 的交易日历判断
    try:
        cal = ak.tool_trade_date_hist_sina()
        cal = cal["trade_date"].apply(lambda x: datetime.strptime(str(x), "%Y%m%d").date())
        return today in set(cal)
    except Exception as e:
        logger.warning(f"交易日判断失败: {e}")
        # 默认工作日尝试抓取
        return today.weekday() < 5

# ======================
# 主程序
# ======================
def main():
    logger.info("红利指数监控启动")
    trade_day = is_trade_day()

    index_map = {
        "中证红利": "000922",
        "上证红利": "000015",
        "深证红利": "399324"
    }

    all_hits = []
    total = 0

    if trade_day:
        for name, code in index_map.items():
            stocks = get_all_index_stocks(code, name)
            total += len(stocks)
            hits = process_stocks(stocks, name)
            all_hits.extend(hits)
            time.sleep(2)
    else:
        logger.info("今天非交易日，无行情更新")
        total = sum(len(get_all_index_stocks(c, n)) for n, c in index_map.items())

    # 读取上次状态
    state_file = "state.json"
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                state = json.load(f)
        except:
            state = {}
    else:
        state = {}

    new_state = {}
    msg_content = "## 红利指数年线提醒\n\n"
    if trade_day and all_hits:
        for h in all_hits:
            code = h["code"]
            prev_count = state.get(code, 0)
            h["continuous_days"] = prev_count + 1
            new_state[code] = h["continuous_days"]
            msg_content += (
                f"- {h['code']} {h['name']}（{h['index']}）\n"
                f"  收盘 {h['close']:.2f} ｜ 年线 {h['ma250']:.2f}\n"
                f"  偏离 {h['deviation_percent']:.2f}% ｜ 连续命中 {h['continuous_days']} 天\n\n"
            )
    else:
        msg_content += f"今天非交易日或未发现股票接近年线\n\n检查数量: {total}\n时间: {datetime.now()}\n"
        # 非交易日不清空 state，连续天数保持

        if trade_day:
            # 交易日但无命中，清空 state
            new_state = {}
        else:
            new_state = state

    # 保存状态
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(new_state, f, ensure_ascii=False, indent=2)

    send_wechat(f"红利年线提醒（{len(all_hits)}只）", msg_content)
    logger.info("运行完成")

if __name__ == "__main__":
    main()
