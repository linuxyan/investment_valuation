# 数据处理模块
import pandas as pd
import numpy as np
import json
import logging
import os,sys
from datetime import datetime
from config import OUTPUT_JSON_DIR, STOCKS_DATA_FILE
from database import StockDatabase

class StockDataProcessor:
    def __init__(self):
        self.db = StockDatabase()
        
    def calculate_valuation_metrics(self, symbol):
        """计算股票估值指标"""
        # 获取股票数据
        stock_data = self.db.get_stock_data_by_symbol(symbol, limit=2500)  # 获取约10年数据
        stock_profit_forecast = self.db.get_latest_profit_forecast(symbol)
        symbols_df = pd.read_csv(STOCKS_DATA_FILE)
        symbols_std_pe_args = symbols_df[symbols_df['股票代码']==symbol]['市盈率标准差倍数'].iloc[0]
        if not stock_data or len(stock_data) < 1000:  # 至少需要4年数据
            logging.warning(f"股票 {symbol} 数据不足，跳过计算")
            return None

        # 转换为DataFrame
        df = pd.DataFrame(stock_data, columns=[
            'symbol', 'timestamp', 'close', 'pe', 'market_capital', 'shares_outstanding'
        ])
        
        # 确保PE数据有效
        df = df[df['pe'].notna() & (df['pe'] > 0)]

        if len(df) < 1000:
            logging.warning(f"股票 {symbol} - {len(df)} 有效PE数据不足,跳过计算")
            return None

        # 获取最新数据
        latest_data = df.iloc[0]
        current_pe = latest_data['pe']
        current_close = latest_data['close']
        current_market_cap = latest_data['market_capital']
        
        # 计算最近五年平均市盈率
        five_year_data = df.head(1250)  # 最近5年数据
        avg_pe_5y = five_year_data['pe'].mean()
        std_pe_5y = five_year_data['pe'].std()
        
        # 计算市盈率90%分位
        pe_percentile_90 = df['pe'].quantile(0.9)
        
        # 计算合理市盈率
        reasonable_pe = ((avg_pe_5y - std_pe_5y * symbols_std_pe_args) + avg_pe_5y) / 2
        
        # 根据合理市盈率,计算合理市盈率和当前市盈率的百分比值
        pe_valuation = current_pe / reasonable_pe if reasonable_pe > 0 else 0
        
        # 根据预测利润,计算三年后的市值,三年后的市盈率为当前合理市盈率的8折,和当前市值的百分比值
        predicted_net_profit = stock_profit_forecast[2]  # 单位：元
        net_profit_valuation = current_market_cap / (reasonable_pe * 0.8 * predicted_net_profit)
        
        # 使用市盈率计算买点
        if reasonable_pe >= 20:
            pe_buy_point = current_close / pe_valuation * 0.5
        else:
            pe_buy_point = current_close / pe_valuation * 0.6
        
        # 使用净利润估值计算买点
        if reasonable_pe >= 20:
            profit_buy_point = (current_close / net_profit_valuation) * 0.5 
        else:
            profit_buy_point = (current_close / net_profit_valuation) * 0.6

        result = {
            'symbol': symbol,
            'timestamp': int(latest_data['timestamp']),
            'current_close': current_close,
            'current_pe': current_pe,
            'avg_pe_5y': round(avg_pe_5y, 2),
            'std_pe_5y': round(std_pe_5y, 2),
            'pe_percentile_90': round(pe_percentile_90, 2),
            'reasonable_pe': round(reasonable_pe, 2),
            'pe_valuation': round(pe_valuation, 2),
            'net_profit_valuation': round(net_profit_valuation, 2),
            'pe_buy_point': round(pe_buy_point, 2),
            'profit_buy_point': round(profit_buy_point, 2),
            'predicted_net_profit': predicted_net_profit,
            'profit_date': stock_profit_forecast[3],
            'calculation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        logging.info(f"完成 {symbol} 的估值计算")
        return result
        
    def process_all_stocks(self):
        """处理所有股票数据"""
        symbols_df = pd.read_csv('data/stocks_data.csv')
        symbols = symbols_df['股票代码'].tolist()
        
        results = []
        successful_count = 0
        
        logging.info(f"开始处理 {len(symbols)} 个股票的估值计算")
        
        for symbol in symbols:
            result = self.calculate_valuation_metrics(symbol)
            if result:
                # 保存到数据库
                self.db.save_valuation_result(result)
                    
                results.append(result)
                successful_count += 1

        logging.info(f"估值计算完成，成功处理 {successful_count}/{len(symbols)} 个股票")
        return results

    def save_to_json(self):
        """保存结果为JSON文件,从数据库估值表查询数据"""
        os.makedirs(OUTPUT_JSON_DIR, exist_ok=True)
        
        # 从数据库查询所有估值数据
        valuation_data = self.db.get_all_valuation_data()
        
        if not valuation_data:
            logging.warning("数据库中没有估值数据")
            return
        
        # 读取股票代码顺序
        symbols_df = pd.read_csv(STOCKS_DATA_FILE)
        symbol_order = symbols_df['股票代码'].tolist()
        
        # 按股票代码分组，并按照CSV文件中的顺序排序
        grouped_results = {}
        for data in valuation_data:
            symbol = data['symbol']
            if symbol not in grouped_results:
                grouped_results[symbol] = []
            grouped_results[symbol].append(data)
        
        # 按照CSV文件中的股票代码顺序保存数据
        for symbol in symbol_order:
            if symbol in grouped_results:
                data_list = grouped_results[symbol]
                # 按时间戳从前往后排序
                data_list.sort(key=lambda x: x['timestamp'])
                
                # 转换时间戳为日期格式，预测利润单位转换为亿元
                for data in data_list:
                    # 转换时间戳为日期格式
                    data['date'] = self.db.timestamp_to_datetime(data['timestamp'])
                    # 转换预测利润单位为亿元（如果存在）
                    if data['predicted_net_profit']:
                        data['predicted_net_profit_billion'] = round(data['predicted_net_profit'] / 100000000, 2)
                
                filename = os.path.join(OUTPUT_JSON_DIR, f"{symbol}_valuation.json")
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(data_list, f, ensure_ascii=False, indent=2)
        
        # 保存所有股票的最新估值数据，按照CSV文件中的顺序
        all_stocks_latest = []
        for symbol in symbol_order:
            if symbol in grouped_results:
                data_list = grouped_results[symbol]
                # 获取每个股票的最新数据（时间戳最大的）
                latest_data = max(data_list, key=lambda x: x['timestamp'])
                # 转换时间戳为日期格式
                latest_data['date'] = self.db.timestamp_to_datetime(latest_data['timestamp'])
                # 转换预测利润单位为亿元（如果存在）
                if latest_data['predicted_net_profit']:
                    latest_data['predicted_net_profit_billion'] = round(latest_data['predicted_net_profit'] / 100000000, 2)
                all_stocks_latest.append(latest_data)
        
        summary_file = os.path.join(OUTPUT_JSON_DIR, "all_stocks_valuation.json")
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(all_stocks_latest, f, ensure_ascii=False, indent=2)
        
        logging.info(f"估值结果已保存到 {OUTPUT_JSON_DIR} 目录")
        logging.info(f"共处理 {len(grouped_results)} 个股票的估值数据，按照CSV文件顺序保存")