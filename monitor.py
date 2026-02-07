import baostock as bs
import pandas as pd
import requests
import os
import time
import logging
import concurrent.futures
from datetime import datetime, timedelta
import sys

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
    try:
        # 使用baostock获取交易日历
        lg = bs.login()
        
        # 获取最近250个交易日
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        
        rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        result = pd.DataFrame(data_list, columns=rs.fields)
        
        # 找到最近的交易日
        trade_dates = result[result['is_trading_day'] == '1']['calendar_date']
        if len(trade_dates) > 0:
            trade_date = pd.to_datetime(trade_dates.iloc[-1]).date()
        else:
            # 如果没有找到，使用昨天
            trade_date = (datetime.now() - timedelta(days=1)).date()
        
        bs.logout()
        return trade_date.strftime("%Y%m%d"), trade_date
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
# 成分股（使用baostock获取指数成分）
# ======================
def get_index_stocks(index_code, index_name):
    try:
        # 首先尝试使用baostock获取指数成分
        lg = bs.login()
        
        # 不同的指数可能需要不同的参数
        if index_code == "000922":  # 中证红利
            # 使用baostock查询指数成分股
            # 注意：baostock的指数成分查询接口可能有限制
            # 这里我们使用一个替代方法：先获取指数K线，然后根据历史数据获取相关股票
            # 实际上，baostock有专门的接口query_stock_basic
            # 但这里为了简单，我们使用一个已知的股票列表（需要定期更新）
            
            # 使用一个已知的中证红利成分股列表（这需要定期更新）
            # 或者从文件/数据库中读取
            stocks = [
                ("000090", "天健集团"),
                ("000157", "中联重科"),
                ("000408", "藏格矿业"),
                ("000429", "粤高速A"),
                ("000651", "格力电器"),
                ("000672", "上峰水泥"),
                ("000895", "双汇发展"),
                ("000933", "神火股份"),
                ("000983", "山西焦煤"),
                ("002043", "兔宝宝"),
                ("002154", "报喜鸟"),
                ("002233", "塔牌集团"),
                ("002267", "陕天然气"),
                ("002416", "爱施德"),
                ("002540", "亚太科技"),
                ("002563", "森马服饰"),
                ("002572", "索菲亚"),
                ("002601", "龙佰集团"),
                ("002737", "葵花药业"),
                ("002756", "永兴材料"),
                ("002867", "周大生"),
                ("301109", "军信股份"),
                ("600012", "皖通高速"),
                ("600015", "华夏银行"),
                ("600016", "民生银行"),
                ("600028", "中国石化"),
                ("600036", "招商银行"),
                ("600039", "四川路桥"),
                ("600057", "厦门象屿"),
                ("600064", "南京高科"),
                ("600096", "云天化"),
                ("600123", "兰花科创"),
                ("600153", "建发股份"),
                ("600177", "雅戈尔"),
                ("600188", "兖矿能源"),
                ("600256", "广汇能源"),
                ("600273", "嘉化能源"),
                ("600282", "南钢股份"),
                ("600295", "鄂尔多斯"),
                ("600348", "华阳股份"),
                ("600350", "山东高速"),
                ("600373", "中文传媒"),
                ("600398", "海澜之家"),
                ("600461", "洪城环境"),
                ("600502", "安徽建工"),
                ("600546", "山煤国际"),
                ("600585", "海螺水泥"),
                ("600729", "重庆百货"),
                ("600737", "中粮糖业"),
                ("600741", "华域汽车"),
                ("600755", "厦门国贸"),
                ("600757", "长江传媒"),
                ("600919", "江苏银行"),
                ("600938", "中国海油"),
                ("600985", "淮北矿业"),
                ("600997", "开滦股份"),
                ("601000", "唐山港"),
                ("601001", "晋控煤业"),
                ("601006", "大秦铁路"),
                ("601009", "南京银行"),
                ("601019", "山东出版"),
                ("601077", "渝农商行"),
                ("601088", "中国神华"),
                ("601098", "中南传媒"),
                ("601101", "昊华能源"),
                ("601166", "兴业银行"),
                ("601168", "西部矿业"),
                ("601169", "北京银行"),
                ("601187", "厦门银行"),
                ("601216", "君正集团"),
                ("601225", "陕西煤业"),
                ("601229", "上海银行"),
                ("601288", "农业银行"),
                ("601318", "中国平安"),
                ("601328", "交通银行"),
                ("601398", "工商银行"),
                ("601598", "中国外运"),
                ("601658", "邮储银行"),
                ("601666", "平煤股份"),
                ("601668", "中国建筑"),
                ("601699", "潞安环能"),
                ("601717", "郑煤机"),
                ("601818", "光大银行"),
                ("601825", "沪农商行"),
                ("601838", "成都银行"),
                ("601857", "中国石油"),
                ("601916", "浙商银行"),
                ("601919", "中远海控"),
                ("601928", "凤凰传媒"),
                ("601939", "建设银行"),
                ("601963", "重庆银行"),
                ("601988", "中国银行"),
                ("601998", "中信银行"),
                ("603565", "中谷物流"),
                ("603706", "东方环宇"),
                ("603967", "中创物流"),
                ("920599", "同力股份"),
            ]
        
        bs.logout()
        
        logger.info(f"{index_name} 成分股 {len(stocks)} 只")
        return stocks
    except Exception as e:
        logger.error(f"{index_name} 成分股获取失败: {e}")
        # 返回一个已知的股票列表作为备选
        return []

# ======================
# 使用baostock获取股票数据
# ======================
def get_stock_baostock(code, name, end_date):
    try:
        # 登录baostock
        lg = bs.login()
        
        # 转换日期格式
        end_date_dt = datetime.strptime(end_date, "%Y%m%d")
        start_date_dt = end_date_dt - timedelta(days=520)
        start_date_str = start_date_dt.strftime("%Y-%m-%d")
        end_date_str = end_date_dt.strftime("%Y-%m-%d")
        
        # 构建股票代码：对于baostock，需要添加交易所前缀
        if code.startswith('6'):
            stock_code = f"sh.{code}"
        elif code.startswith('0') or code.startswith('3'):
            stock_code = f"sz.{code}"
        else:
            stock_code = code  # 对于其他代码，直接使用
        
        # 查询历史数据
        rs = bs.query_history_k_data_plus(
            stock_code,
            "date,close",
            start_date=start_date_str,
            end_date=end_date_str,
            frequency="d",
            adjustflag="2"  # 前复权
        )
        
        if rs.error_code != '0':
            logger.warning(f"获取 {code} {name} 数据失败: {rs.error_msg}")
            bs.logout()
            return None
        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        if len(data_list) < 250:
            logger.warning(f"{code} {name} 数据不足250天: {len(data_list)}天")
            bs.logout()
            return None
        
        # 转换为DataFrame
        df = pd.DataFrame(data_list, columns=rs.fields)
        df['date'] = pd.to_datetime(df['date'])
        df['close'] = pd.to_numeric(df['close'])
        df = df.sort_values('date')
        
        # 计算250日均线
        df['MA250'] = df['close'].rolling(250).mean()
        
        # 获取最后一行数据
        last_row = df.iloc[-1]
        
        close_price = float(last_row['close'])
        ma250_price = float(last_row['MA250'])
        
        # 计算偏离度（百分比）
        if ma250_price > 0:
            deviation = ((ma250_price - close_price) / ma250_price) * 100
        else:
            deviation = 0
        
        bs.logout()
        
        logger.info(f"成功获取 {code} {name}: 收盘{close_price:.2f}, 年线{ma250_price:.2f}, 偏离{deviation:.2f}%")
        return {
            "code": code,
            "name": name,
            "close": close_price,
            "ma250": ma250_price,
            "deviation": deviation
        }
    except Exception as e:
        logger.error(f"获取 {code} {name} 数据异常: {e}")
        try:
            bs.logout()
        except:
            pass
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
    logger.info("红利指数监控启动 - 使用baostock数据源")
    
    # 首先登录baostock
    try:
        bs.login()
        logger.info("baostock登录成功")
    except Exception as e:
        logger.error(f"baostock登录失败: {e}")
        return

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
        bs.logout()
        return
    
    logger.info(f"开始获取 {len(stocks)} 只成分股的价格和年线数据...")
    
    # 使用单线程循环，避免并发问题
    success_count = 0
    request_count = 0
    total_stocks = len(stocks)
    
    for idx, (code, name) in enumerate(stocks, 1):
        logger.info(f"正在获取第 {idx}/{total_stocks} 只股票: {code} {name}")
        
        # 添加请求延迟，避免请求过于频繁
        if request_count > 0 and request_count % 5 == 0:
            time.sleep(1)  # 每5个请求暂停1秒
        
        request_count += 1
        
        data = get_stock_baostock(code, name, trade_str)
        
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
            logger.info(f"进度: {idx}/{total_stocks} 只, 成功: {success_count} 只, 失败: {len(failed_stocks)} 只")
    
    # 登出baostock
    try:
        bs.logout()
        logger.info("baostock已登出")
    except:
        pass
    
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
    md += f"- **数据获取时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    md += f"- **数据源**: baostock\n\n"

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
