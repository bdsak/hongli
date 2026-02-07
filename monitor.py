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

        close_price = float(last["收盘"])
        ma250_price = float(last["MA250"])
        
        # 计算偏离度（百分比）
        if ma250_price > 0:
            deviation = ((ma250_price - close_price) / ma250_price) * 100
        else:
            deviation = 0

        return {
            "code": code,
            "name": name,
            "close": close_price,
            "ma250": ma250_price,
            "deviation": deviation
        }
    except Exception as e:
        logger.warning(f"获取 {code} {name} 数据失败: {e}")
        return None

# ======================
# 判断
# ======================
def check(stock):
    dev = stock["deviation"]
    if 0 < dev <= THRESHOLD * 100:  # 转换为百分比
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
    all_stocks_data = []  # 存储所有成分股的价格和年线数据
    
    stocks = get_index_stocks(index_code, index_name)
    
    logger.info(f"开始获取 {len(stocks)} 只成分股的价格和年线数据...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        tasks = [
            pool.submit(get_stock, c, n, trade_str)
            for c, n in stocks
        ]
        for idx, t in enumerate(concurrent.futures.as_completed(tasks), 1):
            data = t.result()
            if data:
                all_stocks_data.append(data)
                # 检查是否符合条件
                hit = check(data)
                if hit:
                    hit["index"] = index_name
                    hits.append(hit)
                
                # 每完成10个打印一次进度
                if idx % 10 == 0:
                    logger.info(f"已获取 {idx}/{len(stocks)} 只股票数据")
            else:
                logger.warning(f"获取第 {idx} 只股票数据失败")

    # 按照偏离度对所有股票排序
    all_stocks_data.sort(key=lambda x: x["deviation"], reverse=True)
    
    # 生成消息内容
    md = f"# 红利指数年线监控\n\n"
    md += f"- **状态**: {status}\n"
    md += f"- **指数**: {index_name}({index_code})\n"
    md += f"- **成分股总数**: {len(stocks)} 只\n"
    md += f"- **成功获取数据**: {len(all_stocks_data)} 只\n"
    md += f"- **命中**: {len(hits)} 只\n"
    md += f"- **阈值**: 年线下方 {THRESHOLD*100:.1f}%\n\n"

    if not hits:
        md += "## 📊 符合条件的股票\n\n"
        md += "未发现符合条件的股票\n\n"
    else:
        md += "## 📊 符合条件的股票\n\n"
        md += f"| 序号 | 股票代码 | 股票名称 | 收盘价 | 年线 | 偏离度 |\n"
        md += f"|------|----------|----------|--------|------|--------|\n"
        for idx, h in enumerate(sorted(hits, key=lambda x: x["deviation"]), 1):
            md += f"| {idx} | {h['code']} | {h['name']} | {h['close']:.2f} | {h['ma250']:.2f} | {h['deviation']:.2f}% |\n"
        md += "\n"
    
    # 添加所有成分股的价格和年线数据
    md += "## 📋 所有成分股价格和年线数据\n\n"
    md += f"**数据获取时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    md += f"| 序号 | 股票代码 | 股票名称 | 收盘价 | 年线 | 偏离度 |\n"
    md += f"|------|----------|----------|--------|------|--------|\n"
    
    for idx, stock in enumerate(all_stocks_data, 1):
        # 标记符合条件的股票
        marker = " ✅" if 0 < stock["deviation"] <= THRESHOLD * 100 else ""
        md += f"| {idx} | {stock['code']} | {stock['name']}{marker} | {stock['close']:.2f} | {stock['ma250']:.2f} | {stock['deviation']:.2f}% |\n"
    
    md += f"\n**说明**: ✅ 标记表示该股票符合条件（偏离度在 0% 到 {THRESHOLD*100:.1f}% 之间）\n"
    
    # 添加统计信息
    md += f"\n## 📈 统计信息\n\n"
    md += f"- 最高偏离度: {all_stocks_data[0]['deviation']:.2f}% ({all_stocks_data[0]['code']} {all_stocks_data[0]['name']})\n"
    md += f"- 最低偏离度: {all_stocks_data[-1]['deviation']:.2f}% ({all_stocks_data[-1]['code']} {all_stocks_data[-1]['name']})\n"
    md += f"- 平均偏离度: {sum(s['deviation'] for s in all_stocks_data)/len(all_stocks_data):.2f}%\n"
    
    # 发送微信通知
    if not hits:
        send_wechat("红利指数监控", md)
    else:
        send_wechat(f"红利年线提醒（{len(hits)}只）", md)

    # 保存到GitHub摘要
    if GITHUB_SUMMARY:
        with open(GITHUB_SUMMARY, "a", encoding="utf-8") as f:
            f.write(md)

    logger.info(f"运行完成 - 成分股总数: {len(stocks)}, 成功获取: {len(all_stocks_data)}, 命中: {len(hits)}")

if __name__ == "__main__":
    main()
