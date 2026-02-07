import akshare as ak
import pandas as pd
import requests
import os
import time
import logging
import concurrent.futures
from datetime import datetime, timedelta
import random
import warnings
warnings.filterwarnings('ignore')

# ======================
# 参数
# ======================
THRESHOLD = 0.06
SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY")
GITHUB_SUMMARY = os.getenv("GITHUB_STEP_SUMMARY")
MAX_RETRIES = 2  # 降低重试次数，减少总时间
MAX_WORKERS = 1  # 单线程，避免连接被断开
REQUEST_DELAY = 1  # 每次请求之间的延迟

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
    try:
        cal = ak.tool_trade_date_hist_sina()
        cal["trade_date"] = pd.to_datetime(cal["trade_date"]).dt.date
        today = datetime.now().date()
        trade_day = cal[cal["trade_date"] <= today].iloc[-1]["trade_date"]
        return trade_day.strftime("%Y%m%d"), trade_day
    except Exception as e:
        logger.error(f"获取最近交易日失败: {e}")
        # 如果失败，使用今天的前一天作为备选
        yesterday = (datetime.now() - timedelta(days=1)).date()
        return yesterday.strftime("%Y%m%d"), yesterday

# ======================
# 微信
# ======================
def send_wechat(title, content):
    if not SERVER_CHAN_KEY:
        return
    try:
        url = f"https://sctapi.ftqq.com/{SERVER_CHAN_KEY}.send"
        requests.post(url, data={
            "title": title[:32],
            "desp": content,
            "desp_type": "markdown"
        }, timeout=15)
    except Exception as e:
        logger.error(f"发送微信通知失败: {e}")

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
        # 返回一个空的股票列表作为备选
        return []

# ======================
# 尝试多种数据源获取股票数据
# ======================
def get_stock_multi_source(code, name, end_date):
    # 方法1: 尝试使用腾讯接口
    try:
        start = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=520)).strftime("%Y%m%d")
        
        # 尝试多个接口
        df = None
        
        # 接口1: stock_zh_a_hist (新浪)
        try:
            df = ak.stock_zh_a_hist(
                symbol=code,
                start_date=start,
                end_date=end_date,
                adjust="qfq"
            )
        except:
            pass
        
        # 接口2: stock_zh_a_hist_sina (新浪备用接口)
        if df is None or len(df) == 0:
            try:
                df = ak.stock_zh_a_hist_sina(
                    symbol=code,
                    start_date=start,
                    end_date=end_date,
                    adjust="qfq"
                )
            except:
                pass
        
        # 接口3: 对于特定代码格式，可能需要调整
        if df is None or len(df) == 0:
            try:
                # 尝试不同的代码格式
                if code.startswith('6'):
                    symbol = f"sh{code}"
                elif code.startswith('0') or code.startswith('3'):
                    symbol = f"sz{code}"
                else:
                    symbol = code
                
                df = ak.stock_zh_a_hist(
                    symbol=symbol,
                    start_date=start,
                    end_date=end_date,
                    adjust="qfq"
                )
            except:
                pass
        
        if df is None or len(df) < 250:
            logger.warning(f"{code} {name} 数据不足250天或获取失败")
            return None
        
        # 确保数据列存在
        if '收盘' not in df.columns and 'close' in df.columns:
            df['收盘'] = df['close']
        
        df["MA250"] = df["收盘"].rolling(250).mean()
        
        # 检查是否有足够的数据计算MA250
        if pd.isna(df["MA250"].iloc[-1]):
            logger.warning(f"{code} {name} MA250计算失败，数据不足")
            return None
        
        last = df.iloc[-1]
        
        close_price = float(last["收盘"])
        ma250_price = float(last["MA250"])
        
        # 计算偏离度（百分比）
        if ma250_price > 0:
            deviation = ((ma250_price - close_price) / ma250_price) * 100
        else:
            deviation = 0
        
        logger.info(f"成功获取 {code} {name}: 收盘{close_price:.2f}, 年线{ma250_price:.2f}, 偏离{deviation:.2f}%")
        return {
            "code": code,
            "name": name,
            "close": close_price,
            "ma250": ma250_price,
            "deviation": deviation
        }
    except Exception as e:
        logger.error(f"获取 {code} {name} 数据失败: {e}")
        return None

# ======================
# 获取股票数据（带延迟的单线程版本）
# ======================
def get_stock_with_delay(code, name, end_date, delay=1):
    time.sleep(delay + random.uniform(0, 0.5))  # 添加随机延迟避免规律请求
    return get_stock_multi_source(code, name, end_date)

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
    failed_stocks = []    # 存储获取失败的股票
    
    stocks = get_index_stocks(index_code, index_name)
    
    if not stocks:
        logger.error("无法获取成分股列表，程序退出")
        return
    
    logger.info(f"开始获取 {len(stocks)} 只成分股的价格和年线数据...")
    logger.info(f"使用单线程模式，每次请求间隔约{REQUEST_DELAY}秒")
    
    # 使用单线程循环，避免并发问题
    success_count = 0
    for idx, (code, name) in enumerate(stocks, 1):
        logger.info(f"正在获取第 {idx}/{len(stocks)} 只股票: {code} {name}")
        
        # 添加请求延迟
        if idx > 1:
            time.sleep(REQUEST_DELAY)
        
        data = get_stock_multi_source(code, name, trade_str)
        
        if data:
            all_stocks_data.append(data)
            success_count += 1
            
            # 检查是否符合条件
            hit = check(data)
            if hit:
                hit["index"] = index_name
                hits.append(hit)
                logger.info(f"✅ 发现符合条件的股票: {code} {name}, 偏离度{hit['deviation']:.2f}%")
        else:
            failed_stocks.append((code, name))
            logger.warning(f"获取股票 {code} {name} 数据失败")
        
        # 每完成10个打印一次进度
        if idx % 10 == 0:
            logger.info(f"进度: {idx}/{len(stocks)} 只, 成功: {success_count} 只, 失败: {len(failed_stocks)} 只")
    
    # 按照偏离度对所有股票排序
    all_stocks_data.sort(key=lambda x: x["deviation"], reverse=True)
    
    # 生成消息内容
    md = f"# 红利指数年线监控\n\n"
    md += f"- **状态**: {status}\n"
    md += f"- **指数**: {index_name}({index_code})\n"
    md += f"- **成分股总数**: {len(stocks)} 只\n"
    md += f"- **成功获取数据**: {len(all_stocks_data)} 只\n"
    md += f"- **获取失败**: {len(failed_stocks)} 只\n"
    md += f"- **命中**: {len(hits)} 只\n"
    md += f"- **阈值**: 年线下方 {THRESHOLD*100:.1f}%\n"
    md += f"- **数据获取时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"

    # 如果有失败的股票，显示失败列表
    if failed_stocks:
        md += f"## ❌ 数据获取失败的股票 ({len(failed_stocks)}只)\n\n"
        md += f"| 股票代码 | 股票名称 |\n"
        md += f"|----------|----------|\n"
        for code, name in failed_stocks[:20]:  # 最多显示20只
            md += f"| {code} | {name} |\n"
        if len(failed_stocks) > 20:
            md += f"| ... | 还有{len(failed_stocks)-20}只失败股票 |\n"
        md += "\n"

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
    if all_stocks_data:
        md += "## 📋 成功获取数据的成分股\n\n"
        md += f"| 序号 | 股票代码 | 股票名称 | 收盘价 | 年线 | 偏离度 |\n"
        md += f"|------|----------|----------|--------|------|--------|\n"
        
        for idx, stock in enumerate(all_stocks_data, 1):
            # 标记符合条件的股票
            marker = " ✅" if 0 < stock["deviation"] <= THRESHOLD * 100 else ""
            md += f"| {idx} | {stock['code']} | {stock['name']}{marker} | {stock['close']:.2f} | {stock['ma250']:.2f} | {stock['deviation']:.2f}% |\n"
        
        md += f"\n**说明**: ✅ 标记表示该股票符合条件（偏离度在 0% 到 {THRESHOLD*100:.1f}% 之间）\n"
        
        # 添加统计信息
        md += f"\n## 📈 统计信息\n\n"
        md += f"- 成功获取数据股票数量: {len(all_stocks_data)}\n"
        if all_stocks_data:
            md += f"- 最高偏离度: {all_stocks_data[0]['deviation']:.2f}% ({all_stocks_data[0]['code']} {all_stocks_data[0]['name']})\n"
            md += f"- 最低偏离度: {all_stocks_data[-1]['deviation']:.2f}% ({all_stocks_data[-1]['code']} {all_stocks_data[-1]['name']})\n"
            md += f"- 平均偏离度: {sum(s['deviation'] for s in all_stocks_data)/len(all_stocks_data):.2f}%\n"
            
            # 统计偏离度分布
            below_threshold = len([s for s in all_stocks_data if 0 < s["deviation"] <= THRESHOLD * 100])
            above_threshold = len([s for s in all_stocks_data if s["deviation"] > THRESHOLD * 100])
            below_zero = len([s for s in all_stocks_data if s["deviation"] <= 0])
            md += f"- 偏离度分布: 低于年线{below_threshold}只, 高于年线{above_threshold}只, 低于0%{below_zero}只\n"
    else:
        md += "## 📋 股票数据\n\n"
        md += "⚠️ 未能成功获取任何股票数据，请检查网络连接或重试。\n\n"
    
    # 发送微信通知
    try:
        if not hits:
            send_wechat("红利指数监控", md)
        else:
            send_wechat(f"红利年线提醒（{len(hits)}只）", md)
    except Exception as e:
        logger.error(f"发送微信通知失败: {e}")

    # 保存到GitHub摘要
    if GITHUB_SUMMARY:
        try:
            with open(GITHUB_SUMMARY, "a", encoding="utf-8") as f:
                f.write(md)
        except Exception as e:
            logger.error(f"保存到GitHub摘要失败: {e}")

    logger.info(f"运行完成 - 成分股总数: {len(stocks)}, 成功获取: {len(all_stocks_data)}, 失败: {len(failed_stocks)}, 命中: {len(hits)}")

if __name__ == "__main__":
    main()
