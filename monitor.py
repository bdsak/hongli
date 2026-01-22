import akshare as ak
import pandas as pd
import requests
import os
import time
import json
import concurrent.futures
from datetime import datetime, timedelta
import logging

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('monitor_full.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY", "SCT309374TnEp94s4lbzCybeom1FIbUCVH")

# ======================
# 缓存管理（减少重复请求）
# ======================
class DataCache:
    def __init__(self, cache_dir='cache'):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
    
    def get_cache_key(self, stock_code):
        return f"{stock_code}.json"
    
    def get(self, stock_code):
        """获取缓存数据"""
        cache_file = os.path.join(self.cache_dir, self.get_cache_key(stock_code))
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 检查缓存是否过期（1小时）
                    cache_time = datetime.fromisoformat(data['cache_time'])
                    if (datetime.now() - cache_time).total_seconds() < 3600:
                        return data['data']
            except:
                pass
        return None
    
    def set(self, stock_code, data):
        """设置缓存数据"""
        cache_file = os.path.join(self.cache_dir, self.get_cache_key(stock_code))
        cache_data = {
            'cache_time': datetime.now().isoformat(),
            'data': data
        }
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
        except:
            pass

# ======================
# Server酱推送
# ======================
def send_wechat(title, content):
    if not SERVER_CHAN_KEY:
        logger.error("未配置 Server 酱 Key")
        return False
    
    try:
        url = f"https://sctapi.ftqq.com/{SERVER_CHAN_KEY}.send"
        data = {
            "title": title[:32],
            "desp": content,
            "channel": "wechat",
            "desp_type": "markdown"
        }
        
        logger.info(f"发送微信通知: {title}")
        response = requests.post(url, data=data, timeout=15)
        
        if response.status_code == 200:
            result = response.json()
            if result.get("code") == 0:
                logger.info(f"✅ 微信通知发送成功！PushID: {result.get('data', {}).get('pushid', 'N/A')}")
                return True
            else:
                logger.error(f"❌ Server酱错误: {result.get('message')}")
                return False
        else:
            logger.error(f"❌ HTTP错误: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"❌ 发送失败: {e}")
        return False

# ======================
# 获取指数所有成分股
# ======================
def get_all_index_stocks(index_code, index_name):
    """获取指数的所有成分股"""
    logger.info(f"获取 {index_name} 的所有成分股...")
    
    all_stocks = []
    
    try:
        # 方法1: 使用akshare的通用接口
        df = ak.index_stock_cons(index_code)
        if not df.empty:
            if '品种代码' in df.columns:
                for _, row in df.iterrows():
                    code = str(row['品种代码'])
                    name = row.get('品种名称', '') if '品种名称' in df.columns else ''
                    all_stocks.append((code, name))
            else:
                # 尝试其他列名
                for _, row in df.iterrows():
                    code = str(row.iloc[0])
                    name = row.iloc[1] if len(row) > 1 else ''
                    all_stocks.append((code, name))
            
            logger.info(f"方法1获取到 {len(all_stocks)} 只成分股")
            return all_stocks
    except Exception as e:
        logger.warning(f"方法1失败: {e}")
    
    try:
        # 方法2: 使用新浪接口
        df = ak.index_stock_cons_sina(symbol=index_code)
        if not df.empty:
            if 'code' in df.columns:
                for _, row in df.iterrows():
                    code = str(row['code'])
                    name = row.get('name', '') if 'name' in df.columns else ''
                    all_stocks.append((code, name))
            logger.info(f"方法2获取到 {len(all_stocks)} 只成分股")
            return all_stocks
    except Exception as e:
        logger.warning(f"方法2失败: {e}")
    
    try:
        # 方法3: 使用中证指数公司接口（针对中证红利）
        if index_code == "000922":
            df = ak.index_stock_cons_csindex(symbol="000922")
            if not df.empty:
                for _, row in df.iterrows():
                    code = str(row['成分券代码'])
                    name = row.get('成分券名称', '')
                    all_stocks.append((code, name))
                logger.info(f"方法3获取到 {len(all_stocks)} 只成分股")
                return all_stocks
    except Exception as e:
        logger.warning(f"方法3失败: {e}")
    
    logger.warning(f"无法获取 {index_name} 的成分股")
    return []

# ======================
# 获取股票数据（带缓存）
# ======================
def get_stock_data_with_cache(stock_code, stock_name, cache):
    """获取股票数据，带缓存功能"""
    # 检查缓存
    cached_data = cache.get(stock_code)
    if cached_data:
        logger.debug(f"使用缓存数据: {stock_code}")
        return cached_data
    
    try:
        # 处理股票代码
        if stock_code.startswith('6'):
            symbol = stock_code + '.SH'
        elif stock_code.startswith('0') or stock_code.startswith('3'):
            symbol = stock_code + '.SZ'
        else:
            symbol = stock_code
        
        # 获取一年数据（确保有250交易日）
        end_date = datetime.now()
        start_date = end_date - timedelta(days=400)
        
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date.strftime('%Y%m%d'),
            end_date=end_date.strftime('%Y%m%d'),
            adjust="qfq"
        )
        
        if df.empty:
            logger.debug(f"{stock_code} {stock_name}: 无数据")
            return None
        
        if len(df) < 250:
            logger.debug(f"{stock_code} {stock_name}: 数据不足250天 ({len(df)}天)")
            return None
        
        # 计算技术指标
        df['MA250'] = df['收盘'].rolling(window=250, min_periods=1).mean()
        
        # 准备返回数据
        latest = df.iloc[-1]
        result = {
            'code': stock_code,
            'name': stock_name,
            'close': float(latest['收盘']),
            'ma250': float(latest['MA250']),
            'date': latest['日期'].strftime('%Y-%m-%d') if hasattr(latest['日期'], 'strftime') else str(latest['日期']),
            'data_points': len(df)
        }
        
        # 缓存数据
        cache.set(stock_code, result)
        
        return result
        
    except Exception as e:
        logger.debug(f"获取 {stock_code} 数据失败: {e}")
        return None

# ======================
# 检查股票是否符合条件
# ======================
def check_stock_condition(stock_data, threshold=0.06):
    """检查股票是否符合条件"""
    if not stock_data:
        return None
    
    close_price = stock_data['close']
    ma250_price = stock_data['ma250']
    
    if ma250_price <= 0:
        return None
    
    # 计算偏离度（股价低于年线的百分比）
    deviation = (ma250_price - close_price) / ma250_price
    
    # 判断条件：股价低于年线且在threshold以内
    if 0 < deviation <= threshold:
        result = stock_data.copy()
        result['deviation'] = deviation
        result['deviation_percent'] = deviation * 100
        return result
    
    return None

# ======================
# 批量处理股票（使用线程池）
# ======================
def process_stocks_batch(stocks_list, index_name, threshold=0.06, max_workers=10):
    """批量处理股票"""
    cache = DataCache()
    hits = []
    
    logger.info(f"开始批量检查 {index_name} 的 {len(stocks_list)} 只股票...")
    
    # 使用线程池提高效率
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        
        for stock_code, stock_name in stocks_list:
            future = executor.submit(
                get_stock_data_with_cache,
                stock_code, stock_name, cache
            )
            futures.append((future, stock_code, stock_name))
        
        # 处理结果
        for i, (future, stock_code, stock_name) in enumerate(futures):
            try:
                stock_data = future.result(timeout=10)
                if stock_data:
                    # 检查条件
                    result = check_stock_condition(stock_data, threshold)
                    if result:
                        result['index'] = index_name
                        hits.append(result)
                        logger.info(f"✅ {stock_code} {stock_name}: 符合条件 (偏离{result['deviation_percent']:.2f}%)")
                
                # 进度显示
                if (i + 1) % 20 == 0:
                    logger.info(f"  已处理 {i+1}/{len(stocks_list)} 只股票，发现 {len(hits)} 只符合条件")
                    
            except concurrent.futures.TimeoutError:
                logger.warning(f"{stock_code}: 请求超时")
            except Exception as e:
                logger.warning(f"{stock_code}: 处理异常 - {e}")
            
            # 控制请求频率（避免被封）
            if (i + 1) % 50 == 0:
                time.sleep(1)
    
    logger.info(f"{index_name} 检查完成: 处理{len(stocks_list)}只，发现{len(hits)}只符合条件")
    return hits

# ======================
# 生成通知内容
# ======================
def generate_notification_content(hits, total_checked):
    """生成微信通知内容"""
    if not hits:
        return None, None
    
    title = f"📉 红利指数年线预警 ({len(hits)}只)"
    
    content = f"## 红利指数年线预警\n\n"
    content += f"**分析时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    content += f"**监控指数**: 中证红利、上证红利、深证红利\n"
    content += f"**预警条件**: 股价低于年线6%以内\n"
    content += f"**检查总数**: {total_checked}只\n"
    content += f"**发现数量**: {len(hits)}只股票\n\n"
    
    # 按指数分组
    index_groups = {}
    for hit in hits:
        idx = hit['index']
        if idx not in index_groups:
            index_groups[idx] = []
        index_groups[idx].append(hit)
    
    # 按偏离度排序
    for idx in index_groups:
        index_groups[idx].sort(key=lambda x: x['deviation_percent'], reverse=True)
    
    # 生成详细列表
    for idx_name, stocks in index_groups.items():
        content += f"### 📊 {idx_name} ({len(stocks)}只)\n\n"
        
        # 表格头部
        content += "| 股票代码 | 股票名称 | 收盘价 | 年线价 | 偏离度 |\n"
        content += "|:---:|:---:|:---:|:---:|:---:|\n"
        
        for stock in stocks[:20]:  # 最多显示20只
            deviation = stock['deviation_percent']
            
            # 根据偏离度添加表情
            if deviation > 5:
                emoji = "🔴"
            elif deviation > 3:
                emoji = "🟡"
            else:
                emoji = "🟢"
            
            content += f"| {stock['code']} | {stock['name']} | ¥{stock['close']:.2f} | ¥{stock['ma250']:.2f} | {emoji} {deviation:.2f}% |\n"
        
        if len(stocks) > 20:
            content += f"| ... | 还有{len(stocks)-20}只 | ... | ... | ... |\n"
        
        content += "\n"
    
    # 统计信息
    if len(hits) > 1:
        deviations = [h['deviation_percent'] for h in hits]
        avg_deviation = sum(deviations) / len(deviations)
        
        content += "### 📈 统计摘要\n\n"
        content += f"- **平均偏离度**: {avg_deviation:.2f}%\n"
        content += f"- **最大偏离度**: {max(deviations):.2f}%\n"
        content += f"- **最小偏离度**: {min(deviations):.2f}%\n"
        content += f"- **触发比例**: {len(hits)/max(total_checked,1)*100:.1f}%\n\n"
    
    content += "---\n"
    content += "💡 **投资提示**:\n"
    content += "- 股价接近年线可能是技术性买入机会\n"
    content += "- 但需结合基本面、行业趋势等多方面分析\n"
    content += "- 投资有风险，决策需谨慎\n\n"
    content += f"⏰ **推送时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    
    return title, content

# ======================
# 主程序
# ======================
def main():
    """主函数"""
    logger.info("=" * 70)
    logger.info("🚀 全量版红利指数监控程序启动")
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
    
    # 遍历所有指数
    for index_name, index_code in index_config.items():
        logger.info(f"\n{'='*60}")
        logger.info(f"📈 开始处理: {index_name}")
        
        # 获取所有成分股
        stocks_list = get_all_index_stocks(index_code, index_name)
        
        if not stocks_list:
            logger.warning(f"⚠️ {index_name} 无法获取成分股，跳过")
            continue
        
        logger.info(f"📊 {index_name} 共有 {len(stocks_list)} 只成分股")
        
        # 处理所有股票
        hits = process_stocks_batch(
            stocks_list, 
            index_name, 
            threshold=0.06,  # 6%阈值
            max_workers=5    # 并发数，避免请求过快
        )
        
        all_hits.extend(hits)
        total_stocks_checked += len(stocks_list)
        
        # 每个指数处理完后休息一下
        time.sleep(2)
    
    # 保存结果
    os.makedirs('data', exist_ok=True)
    results_data = {
        "analysis_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "total_stocks_checked": total_stocks_checked,
        "total_hits": len(all_hits),
        "hits": all_hits,
        "hit_rate": f"{len(all_hits)/max(total_stocks_checked,1)*100:.1f}%"
    }
    
    with open('data/full_analysis_results.json', 'w', encoding='utf-8') as f:
        json.dump(results_data, f, ensure_ascii=False, indent=2)
    
    logger.info("\n" + "=" * 70)
    logger.info(f"📊 全局统计结果:")
    logger.info(f"   检查股票总数: {total_stocks_checked}")
    logger.info(f"   符合条件数量: {len(all_hits)}")
    logger.info(f"   触发比例: {results_data['hit_rate']}")
    logger.info("=" * 70)
    
    # 发送通知
    if all_hits:
        title, content = generate_notification_content(all_hits, total_stocks_checked)
        if title and content:
            send_wechat(title, content)
            
            # 额外发送一个汇总通知
            summary_content = f"## 📊 红利指数监控汇总\n\n"
            summary_content += f"**完成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            summary_content += f"✅ **全量检查完成**\n\n"
            summary_content += f"**检查统计**:\n"
            summary_content += f"- 📈 监控指数: 3个（中证/上证/深证红利）\n"
            summary_content += f"- 📊 检查股票: {total_stocks_checked}只\n"
            summary_content += f"- 🔔 触发提醒: {len(all_hits)}只\n"
            summary_content += f"- 📉 触发比例: {results_data['hit_rate']}\n\n"
            
            if len(all_hits) > 0:
                # 显示偏离度最大的5只
                top_hits = sorted(all_hits, key=lambda x: x['deviation_percent'], reverse=True)[:5]
                summary_content += f"**偏离度最大的5只股票**:\n"
                for hit in top_hits:
                    summary_content += f"- {hit['code']} {hit['name']}: {hit['deviation_percent']:.2f}%\n"
            
            summary_content += "\n---\n"
            summary_content += "💡 详细列表请查看上一条消息\n"
            summary_content += f"⏰ 推送时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            send_wechat("📊 监控汇总报告", summary_content)
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
            f"当前没有股票价格低于年线6%以内\n\n"
            f"**监控配置**:\n"
            f"- 提醒阈值: 低于年线6%以内\n"
            f"- 检查范围: 全部成分股\n"
            f"- 数据时间: {datetime.now().strftime('%Y-%m-%d')}\n\n"
            f"---\n"
            f"✅ 系统运行正常，将持续监控"
        )
    
    logger.info("🎉 程序运行完成")

# ======================
# 简单模式（快速测试）
# ======================
def simple_mode():
    """简单模式，只检查少量股票用于测试"""
    logger.info("运行简单测试模式...")
    
    # 测试少量股票
    test_stocks = [
        ("600016", "民生银行", "上证红利"),
        ("000858", "五粮液", "深证红利"),
        ("601318", "中国平安", "中证红利"),
        ("600036", "招商银行", "上证红利"),
        ("000333", "美的集团", "深证红利"),
    ]
    
    cache = DataCache()
    hits = []
    
    for stock_code, stock_name, index_name in test_stocks:
        logger.info(f"检查: {stock_code} {stock_name}")
        
        stock_data = get_stock_data_with_cache(stock_code, stock_name, cache)
        if stock_data:
            result = check_stock_condition(stock_data, 0.06)
            if result:
                result['index'] = index_name
                hits.append(result)
                logger.info(f"✅ 符合条件: 偏离{result['deviation_percent']:.2f}%")
        
        time.sleep(1)
    
    if hits:
        title = f"🧪 测试结果 ({len(hits)}只)"
        content = f"## 测试模式结果\n\n"
        content += f"**测试时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        for hit in hits:
            content += f"- {hit['code']} {hit['name']} ({hit['index']})\n"
            content += f"  价格: {hit['close']:.2f}, 年线: {hit['ma250']:.2f}, 偏离: {hit['deviation_percent']:.2f}%\n\n"
        
        send_wechat(title, content)
        logger.info(f"发送测试结果，发现 {len(hits)} 只")
    else:
        logger.info("测试模式未发现符合条件的股票")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        simple_mode()
    else:
        main()
