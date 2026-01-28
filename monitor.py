import akshare as ak
import pandas as pd
import requests
import os
import json
import time
import logging
import concurrent.futures
from datetime import datetime, timedelta

# ======================
# 参数
# ======================
THRESHOLD = 0.06
HIT_DAYS = 3
SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY")

# ======================
# 日志
# ======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("monitor.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# ======================
# 最近交易日（✅ 已修复 tz 问题）
# ======================
def last_trade_date():
    cal = ak.tool_trade_date_hist_sina()
    cal["trade_date"] = pd.to_datetime(cal["trade_date"]).dt.date

    today = datetime.now().date()
    trade_day = cal[cal["trade_date"] <= today].iloc[-1]["trade_date"]

    return trade_day.strftime("%Y%m%d"), trade_day

# ======================
# 缓存（连续命中）
# ======================
class DataCache:
    def __init__(self, path="cache.json"):
        self.path = path
        self.data = {}
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                self.data = json.load(f)

    def save(self):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)

    def hit_days(self, code, hit):
        rec = self.data.get(code, {"days": 0})
        rec["days"] = rec["days"] + 1 if hit else 0
        self.data[code] = rec
        return rec["days"]

# ======================
# 微信
# ======================
def send_wechat(title, content):
    if not SERVER_CHAN_KEY:
        logger.warning("未配置 SERVER_CHAN_KEY")
        return
    url = f"https://sctapi.ftqq.com/{SERVER_CHAN_KEY}.send"
    requests.post(url, data={
        "title": title[:32],
        "desp": content,
        "desp_type": "markdown"
    }, timeout=15)
    logger.info("微信通知已发送")

# ======================
# 成分股
# ======================
def get_index_stocks(code, name):
    try:
        df = ak.index_stock_cons(symbol=code)
        stocks = [(str(r.iloc[0]), r.iloc[1]) for _, r in df.iterrows()]
        logger.info(f"{name} 成分股 {len(stocks)} 只")
        return stocks
    except Exception as e:
        logger.error(f"{name} 成分股失败: {e}")
        return []

# ======================
# 行情
# ======================
def get_stock(code, name, end_date):
    try:
        start = (
            datetime.strptime(end_date, "%Y%m%d") - timedelta(days=420)
        ).strftime("%Y%m%d")

        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start,
            end_date=end_date,
            adjust="qfq"
        )

        if df is None or df.empty:
            return None

        df["MA250"] = df["收盘"].rolling(250, min_periods=200).mean()
        df = df.dropna()
        if df.empty:
            return None

        last = df.iloc[-1]
        return {
            "code": code,
            "name": name,
            "close": float(last["收盘"]),
            "ma250": float(last["MA250"])
        }
    except:
        return None

# ======================
# 判断
# ======================
def check(stock):
    close, ma = stock["close"], stock["ma250"]
    deviation = (ma - close) / ma
    if 0 < deviation <= THRESHOLD:
        stock["deviation"] = deviation * 100
        return stock
    return None

# ======================
# 主程序
# ======================
def main():
    logger.info("红利指数监控启动")

    trade_str, trade_date = last_trade_date()
    today = datetime.now().date()
    is_trade_day = today == trade_date

    index_map = {
        "中证红利": "000922",
        "上证红利": "000015",
        "深证红利": "399324"
    }

    cache = DataCache()
    hits = []

    for name, code in index_map.items():
        stocks = get_index_stocks(code, name)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            tasks = [pool.submit(get_stock, c, n, trade_str) for c, n in stocks]
            for t in concurrent.futures.as_completed(tasks):
                data = t.result()
                if not data:
                    continue
                hit = check(data)
                days = cache.hit_days(data["code"], bool(hit))
                if hit and days >= HIT_DAYS:
                    hit["days"] = days
                    hit["index"] = name
                    hits.append(hit)
        time.sleep(1)

    cache.save()

    status = "📈 今天有行情更新" if is_trade_day else "🛑 今天是非交易日"

    if not hits:
        send_wechat(
            "红利指数监控",
            f"{status}\n\n未发现连续 {HIT_DAYS} 天命中股票\n\n时间：{datetime.now()}"
        )
        return

    content = f"## 红利指数年线提醒\n\n{status}\n\n"
    for h in sorted(hits, key=lambda x: x["deviation"]):
        content += (
            f"- {h['code']} {h['name']}（{h['index']}）\n"
            f"  收盘 {h['close']:.2f} ｜ 年线 {h['ma250']:.2f}\n"
            f"  偏离 {h['deviation']:.2f}% ｜ 连续 {h['days']} 天\n\n"
        )

    send_wechat(f"红利年线提醒（{len(hits)}只）", content)
    logger.info("运行完成")

if __name__ == "__main__":
    main()
