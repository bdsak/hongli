#!/usr/bin/env python3
"""
中证红利指数股票价格跟踪器
自动检测股票价格是否跌破250日年线，并发送微信提醒
"""

import pandas as pd
import numpy as np
import akshare as ak
import yfinance as yf
from datetime import datetime, timedelta
import time
import logging
from typing import List, Dict, Optional
from config import *
from wechat_notifier import WeChatNotifier

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DividendStockTracker:
    def __init__(self, data_source="akshare"):
        self.data_source = data_source
        self.notifier = WeChatNotifier()
        
    def get_stock_list(self) -> List[str]:
        """获取中证红利指数成分股列表"""
        try:
            if self.data_source == "akshare":
                # 使用akshare获取中证红利指数成分股
                stock_df = ak.index_stock_cons_csindex(symbol="000922")
                if not stock_df.empty:
                    return stock_df['成分券代码'].tolist()
        except Exception as e:
            logger.warning(f"获取指数成分股失败，使用预设列表: {str(e)}")
        
        # 如果失败，返回预设列表
        return STOCK_LIST
    
    def get_stock_data_akshare(self, stock_code: str, days: int = 300) -> Optional[pd.DataFrame]:
        """使用akshare获取股票数据"""
        try:
            # 添加交易所后缀
            if stock_code.startswith('6'):
                code_with_suffix = f"sh{stock_code}"
            elif stock_code.startswith('0') or stock_code.startswith('3'):
                code_with_suffix = f"sz{stock_code}"
            else:
                code_with_suffix = stock_code
            
            # 获取日线数据
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")
            
            stock_df = ak.stock_zh_a_hist(
                symbol=code_with_suffix,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            
            if stock_df.empty:
                return None
                
            # 重命名列
            stock_df = stock_df.rename(columns={
                '日期': 'date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume'
            })
            
            stock_df['date'] = pd.to_datetime(stock_df['date'])
            stock_df.set_index('date', inplace=True)
            
            return stock_df
            
        except Exception as e:
            logger.error(f"获取股票{stock_code}数据失败(akshare): {str(e)}")
            return None
    
    def get_stock_data_yfinance(self, stock_code: str, days: int = 300) -> Optional[pd.DataFrame]:
        """使用yfinance获取股票数据"""
        try:
            # 添加交易所后缀
            if stock_code.startswith('6'):
                symbol = f"{stock_code}.SS"
            elif stock_code.startswith('0') or stock_code.startswith('3'):
                symbol = f"{stock_code}.SZ"
            else:
                symbol = f"{stock_code}"
            
            # 下载数据
            ticker = yf.Ticker(symbol)
            stock_df = ticker.history(period=f"{days}d")
            
            if stock_df.empty:
                return None
                
            return stock_df
            
        except Exception as e:
            logger.error(f"获取股票{stock_code}数据失败(yfinance): {str(e)}")
            return None
    
    def get_stock_data(self, stock_code: str, days: int = 300) -> Optional[pd.DataFrame]:
        """获取股票数据"""
        if self.data_source == "akshare":
            return self.get_stock_data_akshare(stock_code, days)
        elif self.data_source == "yfinance":
            return self.get_stock_data_yfinance(stock_code, days)
        else:
            logger.error(f"不支持的数据源: {self.data_source}")
            return None
    
    def calculate_ma250(self, stock_df: pd.DataFrame) -> Dict:
        """计算250日移动平均线"""
        try:
            if len(stock_df) < 250:
                return None
                
            # 计算250日移动平均
            stock_df['MA250'] = stock_df['close'].rolling(window=250).mean()
            
            # 获取最新数据
            latest_data = stock_df.iloc[-1]
            prev_data = stock_df.iloc[-2] if len(stock_df) > 1 else latest_data
            
            current_price = latest_data['close']
            ma250 = latest_data['MA250']
            
            # 计算偏离度
            deviation = (current_price - ma250) / ma250 if ma250 != 0 else 0
            
            return {
                'current_price': current_price,
                'ma250': ma250,
                'deviation': deviation,
                'prev_price': prev_data['close'],
                'prev_ma250': prev_data.get('MA250', ma250)
            }
            
        except Exception as e:
            logger.error(f"计算MA250失败: {str(e)}")
            return None
    
    def check_stock_below_ma250(self, stock_code: str) -> Optional[Dict]:
        """检查股票是否跌破250日年线"""
        try:
            # 获取股票名称
            stock_name = self.get_stock_name(stock_code)
            
            # 获取数据
            stock_df = self.get_stock_data(stock_code, days=400)
            if stock_df is None or stock_df.empty:
                logger.warning(f"无法获取股票{stock_code}数据")
                return None
            
            # 计算MA250
            ma_data = self.calculate_ma250(stock_df)
            if ma_data is None:
                return None
            
            # 检查是否跌破年线
            is_below = ma_data['current_price'] < ma_data['ma250']
            
            result = {
                'code': stock_code,
                'name': stock_name,
                'current_price': ma_data['current_price'],
                'ma250': ma_data['ma250'],
                'deviation': ma_data['deviation'],
                'is_below_ma250': is_below,
                'prev_price': ma_data['prev_price'],
                'prev_ma250': ma_data['prev_ma250']
            }
            
            return result
            
        except Exception as e:
            logger.error(f"检查股票{stock_code}失败: {str(e)}")
            return None
    
    def get_stock_name(self, stock_code: str) -> str:
        """获取股票名称"""
        try:
            # 简单映射，实际应用中应该从API获取
            stock_names = {
                "601288": "农业银行",
                "601398": "工商银行",
                "600028": "中国石化",
                "600019": "宝钢股份",
                "601988": "中国银行",
                "601857": "中国石油",
                "600104": "上汽集团",
                "601328": "交通银行",
                "600016": "民生银行",
                "601818": "光大银行",
            }
            return stock_names.get(stock_code, stock_code)
        except:
            return stock_code
    
    def run_daily_check(self):
        """运行每日检查"""
        logger.info("开始执行中证红利股票年线检查...")
        
        # 获取股票列表
        stock_list = self.get_stock_list()
        logger.info(f"共获取到{len(stock_list)}只股票")
        
        below_ma_stocks = []
        
        # 检查每只股票
        for i, stock_code in enumerate(stock_list[:20]):  # 限制检查数量，避免API限制
            try:
                logger.info(f"检查股票 {i+1}/{len(stock_list)}: {stock_code}")
                
                result = self.check_stock_below_ma250(stock_code)
                if result and result['is_below_ma250']:
                    below_ma_stocks.append(result)
                    logger.info(f"  跌破年线: {result['name']} "
                              f"(当前: {result['current_price']:.2f}, "
                              f"年线: {result['ma250']:.2f})")
                
                # 避免请求过于频繁
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"处理股票{stock_code}时出错: {str(e)}")
                continue
        
        # 发送通知
        if below_ma_stocks:
            logger.info(f"发现{len(below_ma_stocks)}只股票跌破年线，发送通知...")
            title, content = self.notifier.format_stock_alert(below_ma_stocks)
            self.notifier.send_message(title, content)
        else:
            logger.info("没有发现跌破年线的股票")
            
        return below_ma_stocks

def main():
    """主函数"""
    tracker = DividendStockTracker(data_source=DATA_SOURCE)
    
    try:
        below_stocks = tracker.run_daily_check()
        
        # 输出结果摘要
        print("\n" + "="*60)
        print("中证红利股票年线检查完成")
        print(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"跌破年线的股票数量: {len(below_stocks)}")
        
        if below_stocks:
            print("\n跌破年线的股票列表:")
            for stock in below_stocks:
                print(f"{stock['name']}({stock['code']}): "
                      f"当前价¥{stock['current_price']:.2f}, "
                      f"年线¥{stock['ma250']:.2f}, "
                      f"偏离度{stock['deviation']:.2%}")
        
    except Exception as e:
        logger.error(f"执行主程序时出错: {str(e)}")
        
        # 发送错误通知
        notifier = WeChatNotifier()
        notifier.send_message(
            "❌ 股票跟踪系统运行出错",
            f"中证红利股票跟踪系统执行时出现错误:\n\n{str(e)}"
        )

if __name__ == "__main__":
    main()
