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
# 最近交易日（已修复 tz 问题）
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
        logger.warning("未配置 SERVER_CHAN_KEY")
        return

    url = f"https://sctapi.ftqq.com/{SERVER_CHAN_KEY}.send"
    requests.post(
        url,
        data={
            "title": title[:32],
            "desp": content,
            "desp_type": "markdown"
        },
        timeout=15
    )
    logger.info("微信通知已发送")

# ======================
# 成分股
# ======================
def get_index_stocks(code, name):
    try:
        df = ak.index_stock_cons(symbol=code)
        stocks = [(str(r.iloc[0]), r.iloc[1]) for _, r in df.iterrows()]
        logger.info(f"{name} 获取成分股 {len(stocks)} 只")
        return stocks
    except Exception as e:
        logger.error(f"{name} 成分股获取失败: {e}")
        return []

# ======================
# 行情（关键修改：直接获取年线数据）
# ======================
def get_stock(code, name, end_date):
    """
    修改说明：
    1. 直接调用 ak.stock_a_lg_indicator 获取个股技术指标
    2. 该接口返回的 DataFrame 包含 'ma250' 等现成指标
    3. 无需本地计算，直接使用数据源提供的年线
    """
    try:
        # 直接获取个股技术指标数据（包含ma250）
        df = ak.stock_a_lg_indicator(symbol=code)
        
        if df is None or df.empty:
            logger.warning(f"股票 {code} {name} 无指标数据")
            return None
        
        # 获取最新的指标数据（DataFrame按日期倒序排列）
        latest = df.iloc[0]
        
        # 提取收盘价和ma250
        close = latest['close']
        ma250 = latest['ma250']
        
        # 检查数据有效性
        if pd.isna(close) or pd.isna(ma250):
            logger.warning(f"股票 {code} {name} 收盘价或ma250为空值")
            return None
            
        return {
            "code": code,
            "name": name,
            "close": float(close),
            "ma250": float(ma250)
        }
        
    except Exception as e:
        logger.error(f"获取股票 {code} {name} 指标失败: {e}")
        return None

# ======================
# 判断条件
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
    logger.info("红利指数监控启动（使用现成年线数据）")

    trade_str, trade_date = last_trade_date()
    today = datetime.now().date()
    is_trade_day = today == trade_date

    index_map = {
        "中证红利": "000922",
        "上证红利": "000015",
        "深证红利": "399324"
    }

    hits = []

    for index_name, index_code in index_map.items():
        stocks = get_index_stocks(index_code, index_name)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            tasks = [
                pool.submit(get_stock, code, name, trade_str)
                for code, name in stocks
            ]

            for t in concurrent.futures.as_completed(tasks):
                data = t.result()
                if not data:
                    continue

                hit = check(data)
                if hit:
                    hit["index"] = index_name
                    hits.append(hit)

        time.sleep(1)

    status = "📈 今天有行情更新" if is_trade_day else "🛑 今天是非交易日"

    if not hits:
        send_wechat(
            "红利指数监控",
            f"{status}\n\n未发现符合条件的股票\n\n时间：{datetime.now()}"
        )
        logger.info("无命中，已发送状态通知")
        return

    content = f"## 红利指数年线提醒\n\n{status}\n\n"

    for h in sorted(hits, key=lambda x: x["deviation"]):
        content += (
            f"- {h['code']} {h['name']}（{h['index']}）\n"
            f"  收盘 {h['close']:.2f} ｜ 年线 {h['ma250']:.2f}\n"
            f"  偏离 {h['deviation']:.2f}%\n\n"
        )

    send_wechat(f"红利年线提醒（{len(hits)}只）", content)
    logger.info(f"运行完成，共命中 {len(hits)} 只股票")

if __name__ == "__main__":
    main()
