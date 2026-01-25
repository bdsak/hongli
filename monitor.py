import akshare as ak
import requests
import os
import json
import time
import logging
from datetime import datetime, timedelta
from pytz import timezone

# ======================
# 参数（你唯一需要关心）
# ======================
TRACK_DAYS = 3
THRESHOLD = 0.06
UPPER_LIMIT = 0.02
MIN_DIVIDEND = 0.03
TOP_RATIO = 0.30

CN_TZ = timezone("Asia/Shanghai")
STATE_FILE = "state.json"
SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY")

INDEX_MAP = {
    "中证红利": {"code": "000922", "etf": "510880"},
    "上证红利": {"code": "000015", "etf": "510880"},
    "深证红利": {"code": "399324", "etf": "512890"},
}

# ======================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ======================
def is_trade_day():
    return datetime.now(CN_TZ).weekday() < 5

def send(title, msg):
    if not SERVER_CHAN_KEY:
        return
    requests.post(
        f"https://sctapi.ftqq.com/{SERVER_CHAN_KEY}.send",
        data={"title": title[:32], "desp": msg, "desp_type": "markdown"},
        timeout=15
    )

def load_state():
    if os.path.exists(STATE_FILE):
        return json.load(open(STATE_FILE, "r", encoding="utf-8"))
    return {}

def save_state(s):
    json.dump(s, open(STATE_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

# ======================
def index_trend_ok(code):
    df = ak.stock_zh_index_daily(symbol=code)
    if df is None or len(df) < 250:
        return False
    ma = df["close"].rolling(250).mean().iloc[-1]
    return df["close"].iloc[-1] >= ma * 0.98

def get_stock(code):
    df = ak.stock_zh_a_hist(
        symbol=code,
        period="daily",
        start_date=(datetime.now()-timedelta(days=450)).strftime("%Y%m%d"),
        end_date=datetime.now().strftime("%Y%m%d"),
        adjust="qfq"
    )
    if df is None or len(df) < 250:
        return None
    df["MA"] = df["收盘"].rolling(250).mean()
    df["V5"] = df["成交量"].rolling(5).mean()
    df["V20"] = df["成交量"].rolling(20).mean()
    df = df.dropna()
    if df.empty:
        return None
    r = df.iloc[-1]
    return {
        "dev": (r["收盘"] - r["MA"]) / r["MA"],
        "vol": r["V5"] >= r["V20"] * 0.9
    }

def dividend_ok(code):
    try:
        df = ak.stock_dividend_cninfo(symbol=code)
        return not df.empty and float(df.iloc[-1]["股息率"]) >= MIN_DIVIDEND
    except:
        return False

# ======================
def main():
    if not is_trade_day():
        log.info("非交易日")
        return

    state = load_state()
    today = datetime.now(CN_TZ).strftime("%Y-%m-%d")
    signals = []

    for idx, info in INDEX_MAP.items():
        if not index_trend_ok(info["code"]):
            continue

        stocks = ak.index_stock_cons(symbol=info["code"])
        for _, r in stocks.iterrows():
            code = str(r.iloc[0])
            name = r.iloc[1]

            s = get_stock(code)
            if not s or not s["vol"]:
                state.pop(code, None)
                continue

            if not (-THRESHOLD <= s["dev"] <= UPPER_LIMIT):
                state.pop(code, None)
                continue

            if not dividend_ok(code):
                continue

            rec = state.get(code, {
                "name": name,
                "index": idx,
                "days": 0,
                "last": "",
                "best": 9
            })

            if rec["last"] != today:
                rec["days"] += 1
                rec["best"] = min(rec["best"], abs(s["dev"]))

            rec["last"] = today
            state[code] = rec

            if rec["days"] >= TRACK_DAYS:
                signals.append({
                    "code": code,
                    "name": name,
                    "index": idx,
                    "etf": info["etf"],
                    "days": rec["days"],
                    "dev": s["dev"],
                    "best": rec["best"]
                })

            time.sleep(0.15)

    save_state(state)

    if not signals:
        send("红利系统", "今日无强信号（趋势过滤后）")
        return

    # 指数内排序
    final = []
    for idx in INDEX_MAP:
        g = [s for s in signals if s["index"] == idx]
        g.sort(key=lambda x: x["best"])
        final.extend(g[:max(1, int(len(g)*TOP_RATIO))])

    msg = "## 🔔 红利指数 · 强确认信号\n\n"
    for s in final:
        msg += (
            f"- **{s['code']} {s['name']}**（{s['index']}）\n"
            f"  连续 {s['days']} 天｜最优偏离 {s['best']*100:.2f}%\n"
            f"  👉 ETF：`{s['etf']}`\n\n"
        )

    send(f"红利 ETF 信号 {len(final)}", msg)

# ======================
if __name__ == "__main__":
    main()
