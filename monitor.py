import akshare as ak
import pandas as pd
import requests
import os
import logging
from datetime import datetime

# ======================
# 参数
# ======================
THRESHOLD = 0.06
SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY")
GITHUB_SUMMARY = os.getenv("GITHUB_STEP_SUMMARY")
STOCK_FILE = "stocks.txt"   # 本地股票文件

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
# 读取本地股票
# ======================
def load_stocks():
    try:
        df = pd.read_csv(
            STOCK_FILE,
            sep=None,          # 自动识别分隔符
            engine="python",
            header=None,
            names=["code", "name"]
        )
        df["code"] = df["code"].astype(str).str.zfill(6)
        stocks = list(df.itertuples(index=False, name=None))
        logger.info(f"本地股票读取成功：{len(stocks)} 只")
        return stocks
    except Exception as e:
        logger.error(f"股票文件读取失败: {e}")
        return []

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
# 行情 + 官方 MA250
# ======================
def get_stock(code, name, end_date):
    try:
        df = ak.stock_zh_a_hist(
            symbol=code,
            end_date=end_date,
            adjust="qfq",
            indicator="MA"   # ⭐ 官方技术指标
        )

        if df is None or df.empty or "MA250" not in df.columns:
            return None

        last = df.iloc[-1]

        if pd.isna(last["MA250"]):
            return None

        return {
            "code": code,
            "name": name,
            "close": float(last["收盘"]),
            "ma250": float(last["MA250"]),
            "source": "官方MA250"
        }
    except Exception:
        return None

# ======================
# 判断条件
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
    logger.info("红利年线监控启动（官方MA250）")

    trade_str, trade_date = last_trade_date()
    today = datetime.now().date()
    status = "📈 今天有行情更新" if today == trade_date else "🛑 今天是非交易日"

    stocks = load_stocks()
    hits = []

    for code, name in stocks:
        data = get_stock(code, name, trade_str)
        if not data:
            continue

        hit = check(data)
        if hit:
            hits.append(hit)

    md = (
        f"# 年线监控结果\n\n"
        f"- 状态：{status}\n"
        f"- 年线来源：官方 MA250\n"
        f"- 命中：{len(hits)} 只\n\n"
    )

    if not hits:
        md += "未发现符合条件的股票"
        send_wechat("年线监控", md)
    else:
        for h in sorted(hits, key=lambda x: x["deviation"]):
            md += (
                f"- {h['code']} {h['name']}  \n"
                f"  收盘 {h['close']:.2f} ｜ 年线 {h['ma250']:.2f}  \n"
                f"  偏离 {h['deviation']:.2f}% ｜ {h['source']}\n\n"
            )
        send_wechat(f"年线提醒（{len(hits)}只）", md)

    if GITHUB_SUMMARY:
        with open(GITHUB_SUMMARY, "a", encoding="utf-8") as f:
            f.write(md)

    logger.info("运行完成")

if __name__ == "__main__":
    main()
