import akshare as ak
import pandas as pd
import requests
import os
import time
import concurrent.futures
from datetime import datetime, timedelta
import logging

# ======================
# 日志设置
# ======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('monitor.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY", "SCT309374TnEp94s4lbzCybeom1FIbUCVH")

# ======================
# Server酱推送
# ======================
def send_wechat(title, content):
    """发送微信通知"""
    if not SERVER_CHAN_KEY:
        logger.error("未配置 SERVER_CHAN_KEY")
        return False
    
    try:
        url = f"https://sctapi.ftqq.com/{SERVER_CHAN_KEY}.send"
        data = {
            "title": title[:32],
            "desp": content,
            "desp_type": "markdown",
            "channel": "wechat"
        }
        
        logger.info(f"发送微信通知: {title}")
        response = requests.post(url, data=data, timeout=15)
        
        if response.status_code == 200:
            result = response.json()
            if result.get("code") == 0:
                logger.info(f"✅ 微信通知发送成功！")
                return True
            else:
                logger.error(f"❌ Server酱返回错误: {result.get('message')}")
                return False
        else:
            logger.error(f"❌ HTTP请求失败: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"❌ 发送通知失败: {e}")
        return False

# ======================
# 获取指数成分股
# ======================
def get_all_index_stocks(index_code, index_name):
    """获取指数的所有成分股"""
    logger.info(f"获取 {index_name} 的所有成分股...")
    
    stocks = []
    
    try:
        # 方法1: 使用通用接口
        df = ak.index_stock_cons(symbol=index_code)
        if not df.empty:
            if '品种代码' in df.columns:
                for _, row in df.iterrows():
                    code = str(row['品种代码']).strip()
                    name = str(row.get('品种名称', '')).strip()
                    if code and len(code) >= 6:
                        stocks.append((code, name))
            else:
                # 尝试前两列
                for _, row in df.iterrows():
                    if len(row) >= 2:
                        code = str(row.iloc[0]).strip()
                        name = str(row.iloc[1]).strip()
                        if code and len(code) >= 6:
                            stocks.append((code, name))
            
            logger.info(f"{index_name} 获取到 {len(stocks)} 只成分股")
    except Exception as e:
        logger.error(f"获取 {index_name} 成分股失败: {e}")
        return []
    
    # 去重
    unique_stocks = []
    seen_codes = set()
    for code, name in stocks:
        if code and code not in seen_codes:
            seen_codes.add(code)
            unique_stocks.append((code, name))
    
    return unique_stocks

# ======================
# 获取股票技术指标（使用akshare的技术指标接口）
# ======================
def get_stock_technical_data(stock_code, stock_name):
    """获取股票技术指标，包括MA250"""
    try:
        # 处理股票代码格式
        if stock_code.startswith('6'):
            symbol = stock_code
        elif stock_code.startswith('0') or stock_code.startswith('3'):
            symbol = stock_code
        else:
            symbol = stock_code
        
        # 方法1: 使用技术指标接口获取MA系列
        try:
            # 获取股票的历史K线数据，包含技术指标
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", 
                                   start_date="20220101", adjust="qfq")
            
            if df is None or df.empty or len(df) < 250:
                logger.debug(f"{stock_code}: 数据不足")
                return None
            
            # 获取最新收盘价
            latest_close = float(df.iloc[-1]['收盘'])
            
            # 使用akshare的技术指标计算MA250
            try:
                # 尝试使用stock_zh_a_technician函数
                tech_df = ak.stock_zh_a_technician(symbol=symbol, period="daily", start_date="20220101")
                if tech_df is not None and not tech_df.empty and 'ma250' in tech_df.columns:
                    ma250_value = float(tech_df.iloc[-1]['ma250'])
                else:
                    # 如果技术指标接口没有MA250，使用历史数据计算
                    ma250_value = float(df['收盘'].rolling(window=250).mean().iloc[-1])
            except:
                # 如果技术指标接口失败，使用历史数据计算
                ma250_value = float(df['收盘'].rolling(window=250).mean().iloc[-1])
            
            if pd.isna(ma250_value) or ma250_value <= 0:
                return None
            
            result = {
                'code': stock_code,
                'name': stock_name,
                'close': latest_close,
                'ma250': ma250_value,
                'date': str(df.iloc[-1]['日期']),
                'data_points': len(df)
            }
            
            logger.debug(f"{stock_code}: 收盘价={latest_close:.2f}, MA250={ma250_value:.2f}")
            return result
            
        except Exception as e:
            logger.debug(f"技术指标接口失败 {stock_code}: {e}")
            return None
            
    except Exception as e:
        logger.debug(f"获取 {stock_code} 技术指标失败: {e}")
        return None

# ======================
# 检查股票是否符合条件
# ======================
def check_stock_condition(stock_data):
    """检查股票是否低于MA250 6%以内"""
    if not stock_data:
        return None
    
    close_price = stock_data['close']
    ma250_price = stock_data['ma250']
    
    if ma250_price <= 0:
        return None
    
    # 计算偏离度（股价相对于MA250的百分比）
    deviation = (close_price - ma250_price) / ma250_price
    
    # 判断条件：股价低于MA250 6%以内（包括刚好等于）
    if -0.06 <= deviation <= 0:
        stock_data['deviation'] = deviation
        stock_data['deviation_percent'] = deviation * 100
        return stock_data
    
    return None

# ======================
# 批量处理股票
# ======================
def process_stocks_batch(stocks_list, index_name, max_workers=5):
    """批量处理股票数据"""
    logger.info(f"开始处理 {index_name} 的 {len(stocks_list)} 只股票...")
    
    hits = []
    processed_count = 0
    
    # 使用线程池并发处理
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_stock = {}
        for stock_code, stock_name in stocks_list:
            future = executor.submit(get_stock_technical_data, stock_code, stock_name)
            future_to_stock[future] = (stock_code, stock_name)
        
        # 处理结果
        for future in concurrent.futures.as_completed(future_to_stock):
            stock_code, stock_name = future_to_stock[future]
            processed_count += 1
            
            try:
                stock_data = future.result(timeout=15)
                
                if stock_data:
                    # 检查是否符合条件
                    result = check_stock_condition(stock_data)
                    if result:
                        result['index'] = index_name
                        hits.append(result)
                        deviation = result['deviation_percent']
                        logger.info(f"✅ {stock_code} {stock_name}: 低于MA250 {abs(deviation):.2f}%")
                else:
                    logger.debug(f"{stock_code}: 获取技术指标失败")
                    
            except concurrent.futures.TimeoutError:
                logger.warning(f"{stock_code}: 请求超时")
            except Exception as e:
                logger.warning(f"{stock_code}: 处理异常 - {e}")
            
            # 进度显示
            if processed_count % 20 == 0:
                logger.info(f"  已处理 {processed_count}/{len(stocks_list)} 只，发现 {len(hits)} 只符合条件")
            
            # 控制请求频率
            time.sleep(0.2)
    
    logger.info(f"✅ {index_name} 处理完成: 处理{processed_count}只，符合条件{len(hits)}只")
    return hits

# ======================
# 生成通知内容
# ======================
def generate_notification_content(all_hits, total_stocks_checked, analysis_time):
    """生成微信通知内容"""
    if not all_hits:
        return None, None
    
    # 按偏离度排序（从低于MA250最多的开始）
    all_hits.sort(key=lambda x: x['deviation_percent'])
    
    title = f"📉 红利指数MA250提醒 ({len(all_hits)}只)"
    
    content = f"## 红利指数MA250提醒\n\n"
    content += f"**分析时间**: {analysis_time}\n"
    content += f"**监控指数**: 中证红利、上证红利、深证红利\n"
    content += f"**提醒条件**: 股价低于MA250 6%以内\n"
    content += f"**检查总数**: {total_stocks_checked}只\n"
    content += f"**提醒数量**: {len(all_hits)}只股票\n\n"
    
    # 按指数分组
    index_groups = {}
    for hit in all_hits:
        idx = hit['index']
        if idx not in index_groups:
            index_groups[idx] = []
        index_groups[idx].append(hit)
    
    # 显示所有符合条件的股票
    for idx_name, stocks in index_groups.items():
        # 按偏离度排序
        stocks.sort(key=lambda x: x['deviation_percent'])
        
        content += f"### 📊 {idx_name} ({len(stocks)}只)\n\n"
        
        for stock in stocks:
            deviation = stock['deviation_percent']
            below_percent = abs(deviation)
            
            # 根据偏离度设置状态
            if below_percent > 5:
                status = "🔴 显著低于"
            elif below_percent > 3:
                status = "🟠 明显低于"
            else:
                status = "🟡 略低于"
            
            content += f"{status} **{stock['name']}** ({stock['code']})\n"
            content += f"当前价: ¥{stock['close']:.2f} | MA250: ¥{stock['ma250']:.2f} | 低于: {below_percent:.2f}%\n\n"
    
    # 统计信息
    if len(all_hits) > 1:
        below_percents = [abs(h['deviation_percent']) for h in all_hits]
        
        content += f"### 📈 统计摘要\n\n"
        content += f"- **平均低于MA250**: {sum(below_percents)/len(below_percents):.2f}%\n"
        content += f"- **最大低于MA250**: {max(below_percents):.2f}%\n"
        content += f"- **最小低于MA250**: {min(below_percents):.2f}%\n"
        content += f"- **提醒比例**: {len(all_hits)/max(total_stocks_checked,1)*100:.1f}%\n\n"
    
    content += "---\n"
    content += "💡 **技术指标说明**:\n"
    content += "- MA250: 250日移动平均线（年线）\n"
    content += "- 数据来源: akshare技术指标接口\n"
    content += "- 提醒阈值: 股价低于MA250 6%以内\n\n"
    content += f"⏰ **更新时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    
    return title, content

# ======================
# 主程序
# ======================
def main():
    """主函数"""
    logger.info("=" * 70)
    logger.info("🚀 红利指数监控程序启动")
    logger.info(f"📅 启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)
    
    # 指数配置
    index_config = {
        "中证红利": "000922",
        "上证红利": "000015", 
        "深证红利": "399324"
    }
    
    all_hits = []
    total_stocks_checked = 0
    analysis_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # 先发送启动通知
    send_wechat(
        "🚀 红利指数监控启动",
        f"## 红利指数监控系统启动\n\n"
        f"**启动时间**: {analysis_time}\n\n"
        f"📊 **监控配置**:\n"
        f"- 监控指数: 中证红利、上证红利、深证红利\n"
        f"- 技术指标: MA250（使用akshare技术指标接口）\n"
        f"- 提醒条件: 股价低于MA250 6%以内\n"
        f"- 检查范围: 所有成分股\n\n"
        f"✅ 开始检查所有成分股...\n\n"
        f"---\n"
        f"*系统运行中，请稍后查看结果*"
    )
    
    # 遍历所有指数
    for index_name, index_code in index_config.items():
        logger.info(f"\n📈 开始处理: {index_name}")
        
        # 获取所有成分股
        stocks_list = get_all_index_stocks(index_code, index_name)
        
        if not stocks_list:
            logger.warning(f"⚠️ {index_name} 无法获取成分股，跳过")
            continue
        
        logger.info(f"📊 {index_name} 共有 {len(stocks_list)} 只成分股")
        total_stocks_checked += len(stocks_list)
        
        # 处理所有股票
        hits = process_stocks_batch(stocks_list, index_name)
        all_hits.extend(hits)
        
        # 每个指数处理完后休息一下
        time.sleep(2)
    
    # 全局统计
    logger.info("\n" + "=" * 70)
    logger.info(f"📊 全局统计结果:")
    logger.info(f"   检查股票总数: {total_stocks_checked}")
    logger.info(f"   符合条件数量: {len(all_hits)}")
    
    if len(all_hits) > 0:
        below_percents = [abs(h['deviation_percent']) for h in all_hits]
        logger.info(f"   平均低于MA250: {sum(below_percents)/len(below_percents):.2f}%")
        logger.info(f"   提醒比例: {len(all_hits)/max(total_stocks_checked,1)*100:.1f}%")
    
    logger.info("=" * 70)
    
    # 发送详细通知
    if all_hits:
        title, content = generate_notification_content(all_hits, total_stocks_checked, analysis_time)
        if title and content:
            success = send_wechat(title, content)
            
            if success:
                logger.info(f"✅ 详细通知已发送，共发现 {len(all_hits)} 只符合条件的股票")
    else:
        logger.info("无符合条件的股票")
        
        # 发送无提醒通知
        send_wechat(
            "📊 红利指数监控报告",
            f"## 全量监控报告\n\n"
            f"**完成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"📈 **检查统计**:\n"
            f"- 监控指数: 中证红利、上证红利、深证红利\n"
            f"- 检查股票: {total_stocks_checked}只\n"
            f"- 符合条件: 0只\n\n"
            f"💡 **市场情况**:\n"
            f"当前没有股票价格低于MA250 6%以内\n\n"
            f"**技术指标**:\n"
            f"- 使用指标: MA250（akshare技术指标接口）\n"
            f"- 提醒阈值: 低于MA250 6%以内\n\n"
            f"---\n"
            f"✅ 系统运行正常，将持续监控"
        )
        
        logger.info("✅ 无提醒报告已发送")
    
    logger.info("🎉 程序运行完成")

if __name__ == "__main__":
    main()
