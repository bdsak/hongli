import akshare as ak
import pandas as pd
import requests
import os
from datetime import datetime

SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY")

# ======================
# Server 酱推送
# ======================
def send_wechat(title, content):
    if not SERVER_CHAN_KEY:
        print("未配置 Server 酱 Key")
        return

    url = f"https://sctapi.ftqq.com/{SERVER_CHAN_KEY}.send"
    data = {
        "title": title,
        "desp": content
    }
    requests.post(url, data=data)

# ======================
# 获取指数成分股
# ======================
def get_index_stocks(index_name):
    df = ak.index_stock_cons(symbol=index_name)
    return df["symbol"].tolist(), df["name"].tolist()

# ======================
# 判断是否接近年线
# ======================
def check_stock(code, name):
    try:
        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date="20220101",
            adjust="qfq"
        )
        if len(df) < 250:
            return None

        df["ma250"] = df["收盘"].rolling(250).mean()
        latest = df.iloc[-1]

        close = latest["收盘"]
        ma250 = latest["ma250"]

        if pd.isna(ma250):
            return None

        if close <= ma250 * 1.06:
            return {
                "code": code,
                "name": name,
                "close": close,
                "ma250": ma250
            }
    except Exception as e:
        print(code, e)

    return None

# ======================
# 主逻辑
# ======================
def main():
    index_map = {
        "中证红利": "中证红利",
        "上证红利": "上证红利",
        "深证红利": "深证红利"
    }

    hits = []

    for index_name, symbol in index_map.items():
        codes, names = get_index_stocks(symbol)
        for code, name in zip(codes, names):
            res = check_stock(code, name)
            if res:
                res["index"] = index_name
                hits.append(res)

    if hits:
        lines = []
        for h in hits:
            lines.append(
                f"- {h['index']} | {h['name']}({h['code']})\n"
                f"  收盘价：{h['close']:.2f}\n"
                f"  年线：{h['ma250']:.2f}"
            )

        content = "\n\n".join(lines)
        send_wechat(
            title="📉 红利指数年线预警",
            content=content
        )
    else:
        print("无触发条件股票")

if __name__ == "__main__":
    main()
