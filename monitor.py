import akshare as ak
import pandas as pd
import requests
import os
import time
from datetime import datetime, timedelta
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SERVER_CHAN_KEY = os.getenv("SERVER_CHAN_KEY")

# ======================
# Server 酱推送
# ======================
def send_wechat(title, content):
    if not SERVER_CHAN_KEY:
        logger.error("未配置 Server 酱 Key")
        return False
    
    try:
        url = f"https://sctapi.ftqq.com/{SERVER_CHAN_KEY}.send"
        data = {
            "title": title,
            "desp": content,
            "channel": "wechat",
            "desp_type": "markdown"
        }
        
        logger.info(f"发送微信通知: {title}")
        response = requests.post(url, data=data, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            if result.get("code") == 0:
                logger.info("微信通知发送成功！")
                return True
            else:
                logger.error(f"Server酱返回错误: {result.get('message')}")
                return False
        else:
            logger.error(f"HTTP请求失败: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"发送微信通知失败: {e}")
        return False

# ======================
# 获取指数成分股
# ======================
def get_index_stocks(index_code):
    """获取指数成分股"""
    try:
        # 方法1: 使用新浪接口
        logger.info(f"获取指数成分股: {index_code}")
        df = ak.index_stock_cons_sina(symbol=index_code)
        
        if not df.empty:
            # 提取股票代码和名称
            if 'code' in df.columns:
                codes = df['code'].astype(str).tolist()
            elif '成分券代码' in df.columns:
                codes = df['成分券代码'].astype(str).tolist()
            else:
                # 尝试第一列
                codes = df.iloc[:, 0].astype(str).tolist()
            
            # 提取股票名称
            if 'name' in df.columns:
                names = df['name'].tolist()
            elif '成分券名称' in df.columns:
                names = df['成分券名称'].tolist()
            else:
                names = [""] * len(codes)
            
            logger.info(f"成功获取 {index_code} 成分股: {len(codes)} 只")
            return codes, names
            
    except Exception as e:
        logger.warning(f"方法1失败: {e}")
    
    try:
        # 方法2: 使用东方财富接口
        df = ak.index_stock_cons(index_code)
        if not df.empty and '品种代码' in df.columns:
            codes = df['品种代码'].astype(str).tolist()
            names = df['品种名称'].tolist() if '品种名称' in df.columns else [""] * len(codes)
            logger.info(f"方法2成功获取 {index_code} 成分股: {len(codes)} 只")
            return codes, names
    except Exception as e:
        logger.warning(f"方法2失败: {e}")
    
    logger.error(f"无法获取指数 {index_code} 的成分股")
    return [], []

# ======================
# 获取股票数据
# ======================
def get_stock_data(stock_code):
    """获取股票历史数据"""
    try:
        # 处理股票代码格式
        if stock_code.startswith('6'):
            symbol = stock_code + '.SH'
        elif stock_code.startswith('0') or stock_code.startswith('3'):
            symbol = stock_code + '.SZ'
        else:
            symbol = stock_code
        
        # 计算日期（一年前到现在）
        end_date = datetime.now()
        start_date = end_date - timedelta(days=400)  # 多取一些确保有250日
        
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date.strftime('%Y%m%d'),
            end_date=end_date.strftime('%Y%m%d'),
            adjust="qfq"
        )
        
        if df.empty:
            logger.debug(f"股票 {stock_code} 无数据")
            return None
        
        if len(df) < 250:
            logger.debug(f"股票 {stock_code} 数据不足250天")
            return None
        
        return df
        
    except Exception as e:
        logger.debug(f"获取股票 {stock_code} 数据失败: {e}")
        return None

# ======================
# 判断是否接近年线
# ======================
def check_stock(stock_code, stock_name, index_name):
    """检查股票是否接近年线"""
    try:
        df = get_stock_data(stock_code)
        if df is None:
            return None
        
        # 计算250日移动平均线
        df['MA250'] = df['收盘'].rolling(window=250, min_periods=1).mean()
        
        # 获取最新数据
        latest = df.iloc[-1]
        close_price = latest['收盘']
        ma250_price = latest['MA250']
        
        if pd.isna(ma250_price) or ma250_price <= 0:
            return None
        
        # 计算偏离度（低于年线为正数）
        deviation = (ma250_price - close_price) / ma250_price
        
        # 判断是否低于年线且在6%以内
        if 0 < deviation <= 0.06:
            result = {
                "code": stock_code,
                "name": stock_name,
                "index": index_name,
                "close": round(close_price, 2),
                "ma250": round(ma250_price, 2),
                "deviation": round(deviation, 4),
                "deviation_percent": round(deviation * 100, 2),
                "date": latest['日期'].strftime('%Y-%m-%d') if hasattr(latest['日期'], 'strftime') else str(latest['日期'])
            }
            logger.info(f"✅ {stock_code} {stock_name} 触发提醒: 偏离年线 {result['deviation_percent']}%")
            return result
        
    except Exception as e:
        logger.warning(f"检查股票 {stock_code} 时出错: {e}")
    
    return None

# ======================
# 主逻辑
# ======================
def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("红利指数监控程序启动")
    logger.info(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    # 指数配置（使用正确的指数代码）
    index_config = {
        "中证红利": "000922",  # 中证红利指数代码
        "上证红利": "000015",  # 上证红利指数代码
        "深证红利": "399324"   # 深证红利指数代码
    }
    
    hits = []
    
    for index_name, index_code in index_config.items():
        logger.info(f"开始处理 {index_name} 指数...")
        
        # 获取成分股
        codes, names = get_index_stocks(index_code)
        
        if not codes:
            logger.warning(f"{index_name} 无成分股数据")
            continue
        
        logger.info(f"{index_name}: 共有 {len(codes)} 只成分股，开始检查...")
        
        # 限制检查数量以避免请求过多
        check_limit = min(20, len(codes))  # 每次最多检查20只
        checked_count = 0
        
        for code, name in zip(codes[:check_limit], names[:check_limit]):
            # 添加市场前缀以便后续处理
            full_code = code
            
            # 检查股票
            result = check_stock(full_code, name, index_name)
            if result:
                hits.append(result)
            
            checked_count += 1
            
            # 控制请求频率
            time.sleep(0.5)
            
            # 每检查5只打印一次进度
            if checked_count % 5 == 0:
                logger.info(f"  已检查 {checked_count}/{check_limit} 只股票...")
    
    logger.info("=" * 60)
    logger.info(f"检查完成，共发现 {len(hits)} 只符合条件的股票")
    
    # 发送通知
    if hits:
        # 构建通知内容
        title = f"📉 红利指数年线预警 ({len(hits)}只)"
        
        content = f"## 红利指数年线预警\n\n"
        content += f"**分析时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        content += f"**监控指数**: 中证红利、上证红利、深证红利\n"
        content += f"**预警条件**: 股价低于年线6%以内\n"
        content += f"**发现数量**: {len(hits)}只股票\n\n"
        
        # 按指数分组
        index_groups = {}
        for hit in hits:
            idx = hit['index']
            if idx not in index_groups:
                index_groups[idx] = []
            index_groups[idx].append(hit)
        
        # 生成Markdown表格
        content += "### 📊 详细列表\n\n"
        
        for idx_name, stocks in index_groups.items():
            content += f"#### {idx_name} ({len(stocks)}只)\n\n"
            content += "| 股票代码 | 股票名称 | 收盘价 | 年线价 | 偏离度 |\n"
            content += "|:---:|:---:|:---:|:---:|:---:|\n"
            
            for stock in stocks:
                # 根据偏离度添加颜色/表情
                deviation = stock['deviation_percent']
                if deviation > 5:
                    emoji = "🔴"
                elif deviation > 3:
                    emoji = "🟡"
                else:
                    emoji = "🟢"
                
                content += f"| {stock['code']} | {stock['name']} | {stock['close']:.2f} | {stock['ma250']:.2f} | {emoji} {deviation}% |\n"
            
            content += "\n"
        
        # 添加统计信息
        if len(hits) > 1:
            deviations = [h['deviation_percent'] for h in hits]
            avg_deviation = sum(deviations) / len(deviations)
            
            content += "### 📈 统计摘要\n\n"
            content += f"- **平均偏离度**: {avg_deviation:.2f}%\n"
            content += f"- **最大偏离度**: {max(deviations):.2f}%\n"
            content += f"- **最小偏离度**: {min(deviations):.2f}%\n\n"
        
        content += "---\n"
        content += "💡 **提示**: 股价接近年线可能是技术性机会，请结合基本面分析\n\n"
        content += f"*推送时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"
        
        # 发送通知
        success = send_wechat(title, content)
        
        if success:
            logger.info("微信通知发送成功")
        else:
            logger.error("微信通知发送失败")
    else:
        logger.info("无符合条件的股票")
        
        # 可选：发送无提醒的通知
        # send_wechat(
        #     title="✅ 红利指数监控报告",
        #     content=f"**监控时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        #             f"✅ 今日无符合提醒条件的股票\n\n"
        #             f"监控指数: 中证红利、上证红利、深证红利\n"
        #             f"提醒阈值: 低于年线6%以内\n\n"
        #             f"*系统运行正常，将持续监控*"
        # )
    
    logger.info("=" * 60)
    logger.info("程序运行完成")
    logger.info("=" * 60)

# ======================
# 测试函数
# ======================
def test_serverchan():
    """测试Server酱配置"""
    logger.info("测试Server酱配置...")
    
    if not SERVER_CHAN_KEY:
        logger.error("未配置SERVER_CHAN_KEY环境变量")
        print("\n请设置环境变量:")
        print("export SERVER_CHAN_KEY=SCT309374TnEp94s4lbzCybeom1FIbUCVH")
        return False
    
    test_content = f"## Server酱配置测试\n\n"
    test_content += f"**测试时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    test_content += f"✅ Server酱配置成功！\n\n"
    test_content += f"**配置信息**:\n"
    test_content += f"- SendKey: `{SERVER_CHAN_KEY[:8]}...`\n"
    test_content += f"- 监控系统: 红利指数年线预警\n"
    test_content += f"- 监控指数: 中证/上证/深证红利\n"
    test_content += f"- 提醒阈值: 低于年线6%以内\n\n"
    test_content += f"---\n*这是一条测试消息，系统将在收盘后自动运行*"
    
    return send_wechat("✅ Server酱配置测试", test_content)

if __name__ == "__main__":
    # 检查环境变量
    if not SERVER_CHAN_KEY:
        SERVER_CHAN_KEY = "SCT309374TnEp94s4lbzCybeom1FIbUCVH"
        logger.info(f"使用内置SendKey: {SERVER_CHAN_KEY[:8]}...")
    
    # 可以选择运行测试或主程序
    if os.getenv("RUN_TEST", "false").lower() == "true":
        test_serverchan()
    else:
        main()
