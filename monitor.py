import akshare as ak
import pandas as pd
import requests
import os
import time
import logging
import concurrent.futures
from datetime import datetime, timedelta

# ======================
# 参数
# ======================
THRESHOLD = 0.06
SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY")
GITHUB_SUMMARY = os.getenv("GITHUB_STEP_SUMMARY")

# ======================
# 日志
# ======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ======================
# 最近交易日
# ======================
def last_trade_date():
    cal = ak.tool_trade_date_hist_sina()
    cal["trade_date"] = pd.to_datetime(cal["trade_date"]).dt.date
    today = datetime.now().date()
    trade_day = cal[cal["trade_date"] <= today].iloc[-1]["trade_date"]
    return trade_day.strftime("%Y%m%d"), trade_day

# ======================
# 微信
# ======================
def send_wechat(title, content):
    if not SERVER_CHAN_KEY:
        return
    url = f"https://sctapi.ftqq.com/{SERVER_CHAN_KEY}.send"
    requests.post(url, data={
        "title": title[:32],
        "desp": content,
        "desp_type": "markdown"
    }, timeout=15)

# ======================
# 成分股（官方中证指数）
# ======================
def get_index_stocks(index_code, index_name):
    try:
        df = ak.index_stock_cons_csindex(symbol=index_code)
        stocks = list(
            df[["成分券代码", "成分券名称"]]
            .astype(str)
            .itertuples(index=False, name=None)
        )
        logger.info(f"{index_name} 成分股 {len(stocks)} 只")
        return stocks
    except Exception as e:
        logger.error(f"{index_name} 成分股获取失败: {e}")
        return []

# ======================
# 行情 + 年线（统一自己算）
# ======================
def get_stock(code, name, end_date):
    try:
        start = (
            datetime.strptime(end_date, "%Y%m%d") - timedelta(days=520)
        ).strftime("%Y%m%d")

        df = ak.stock_zh_a_hist(
            symbol=code,
            start_date=start,
            end_date=end_date,
            adjust="qfq"
        )
        if df is None or len(df) < 250:
            return None

        df["MA250"] = df["收盘"].rolling(250).mean()
        last = df.iloc[-1]

        return {
            "code": code,
            "name": name,
            "close": float(last["收盘"]),
            "ma250": float(last["MA250"])
        }
    except Exception:
        return None

# ======================
# 判断
# ======================
def check(stock):
    dev = (stock["ma250"] - stock["close"]) / stock["ma250"]
    if 0 < dev <= THRESHOLD:
        stock["deviation"] = dev * 100
        return stock
    return None

# ======================
# 主程序
# ======================
def main():
    logger.info("红利指数监控启动")

    trade_str, trade_date = last_trade_date()
    today = datetime.now().date()
    status = "📈 今天有行情更新" if today == trade_date else "🛑 今天是非交易日"

    # 只保留中证红利
    index_name = "中证红利"
    index_code = "000922"
    
    hits = []
    
    stocks = get_index_stocks(index_code, index_name)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        tasks = [
            pool.submit(get_stock, c, n, trade_str)
            for c, n in stocks
        ]
        for t in concurrent.futures.as_completed(tasks):
            data = t.result()
            if not data:
                continue
            hit = check(data)
            if hit:
                hit["index"] = index_name
                hits.append(hit)

    md = f"# 红利指数年线监控\n\n- 状态：{status}\n- 命中：{len(hits)} 只\n- 指数：{index_name}({index_code})\n\n"

    if not hits:
        md += "未发现符合条件的股票"
        send_wechat("红利指数监控", md)
    else:
        for h in sorted(hits, key=lambda x: x["deviation"]):
            md += (
                f"- {h['code']} {h['name']}（{h['index']}）  \n"
                f"  收盘 {h['close']:.2f} ｜ 年线 {h['ma250']:.2f}  \n"
                f"  偏离 {h['deviation']:.2f}%\n\n"
            )
        send_wechat(f"红利年线提醒（{len(hits)}只）", md)

    if GITHUB_SUMMARY:
        with open(GITHUB_SUMMARY, "a", encoding="utf-8") as f:
            f.write(md)

    logger.info("运行完成")

if __name__ == "__main__":
    main()
