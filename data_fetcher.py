# 数据获取模块
import requests
import pandas as pd
import sys,time
import logging
from datetime import datetime, timedelta
from io import StringIO
from config import XUEQIU_API_URL, API_PARAMS, HEADERS, TEN_YEARS_TRADING_DAYS, STOCKS_DATA_FILE
from database import StockDatabase

class StockDataFetcher:
    def __init__(self):
        self.db = StockDatabase()
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        
    def get_stock_symbols(self):
        """从CSV文件获取股票代码列表"""
        try:
            df = pd.read_csv(STOCKS_DATA_FILE)
            symbols = df['股票代码'].tolist()
            logging.info(f"成功读取 {len(symbols)} 个股票代码")
            return symbols
        except Exception as e:
            logging.error(f"读取股票代码文件失败: {e}")
            return []
            
    def fetch_stock_data(self, symbol, count=None):
        """从雪球API获取股票数据"""
        if count is None:
            # 检查数据库中是否已有该股票数据
            if self.db.check_symbol_exists(symbol):
                count = -10  # 获取最新数据
            else:
                count = TEN_YEARS_TRADING_DAYS  # 获取10年数据

        params = API_PARAMS.copy()
        symbol_code = str(symbol).replace("hk", "").replace("HK", "")
        params.update({
            'symbol': symbol_code,
            'count': count
        })
        
        try:
            response = self.session.get(XUEQIU_API_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            if data.get('error_code') == 0:
                return self._parse_api_data(symbol, data)
            else:
                logging.error(f"API返回错误: {data}")
                return None
                
        except requests.RequestException as e:
            logging.error(f"请求API失败: {e}")
            return None
        except Exception as e:
            logging.error(f"处理API数据失败: {e}")
            return None

    def _stock_profit_forecast(self, symbol):
        """获取股票业绩预测数据"""
        if symbol.lower().startswith('hk'):
            try:
                url = "https://www.etnet.com.hk/www/sc/stocks/realtime/quote_profit.php"
                headers = HEADERS.copy()
                headers['Referer'] = "https://www.etnet.com.hk"
                params = {
                    "code": str(symbol).lower().replace("hk", "")
                }
                response = requests.get(url, params=params, headers=headers)
                temp_df = pd.read_html(StringIO(response.text), header=0)[3]
                last_year_data = temp_df.loc[temp_df.index.max()]
                last_year = int(last_year_data['财政年度'])
                try:
                    last_year_profit_forecast = last_year_data['纯利/(亏损)  (百万元人民币)'] * 1000000  # 转换为元人民币
                except Exception as e:
                    last_year_profit_forecast = last_year_data['纯利/(亏损)  (百万港元)'] * 1000000  # 转换为元
                return symbol, last_year, last_year_profit_forecast
            except Exception as e:
                logging.error(f"获取{symbol}业绩预测数据失败: {e}")
                return None, None, None
        else:
            try:
                symbol_code = str(symbol).lower().replace("sz", "").replace("sh", "")
                url = f"https://basic.10jqka.com.cn/new/{symbol_code}/worth.html"
                headers = HEADERS.copy()
                headers['Referer'] = f"https://basic.10jqka.com.cn/{symbol_code}"
                response = requests.get(url, headers=headers)
                response.encoding = "gbk"
                temp_df = pd.read_html(StringIO(response.text))[1]
                last_year_data = temp_df.loc[temp_df.index.max()]
                last_year = int(last_year_data['年度'])
                last_year_profit_forecast = round((last_year_data['最小值'] + last_year_data['均值']) / 2 * 100000000, 2)  # 取最小值和均值的平均值，并转换为元人民币
                return symbol, last_year, last_year_profit_forecast
            except Exception as e:
                logging.error(f"获取{symbol}业绩预测数据失败: {e}")
                return None, None, None


    def _parse_api_data(self, symbol, data):
        """解析API返回的数据"""
        items = data.get('data', {}).get('item', [])
        parsed_data = []
        
        for item in items[4:]:   # 跳过前4条数据，因为前4条数据是无效数据
            if len(item) == 14:  # 确保数据格式正确
                timestamp = item[0]  # 时间戳
                close = item[5]     # 收盘价
                pe = item[-2]  # 市盈率
                market_capital = item[-1]   # 总市值
                
                if close and close > 0:  # 确保有效数据
                    parsed_data.append({
                        'symbol': symbol,
                        'timestamp': timestamp,
                        'close': close,
                        'pe': pe,
                        'market_capital': market_capital
                    })

        logging.info(f"解析到 {len(parsed_data)} 条 {symbol}的数据 - 最新 { self.db.timestamp_to_datetime(parsed_data[-1]['timestamp']) }")
        return parsed_data

    def save_to_database(self, stock_data):
        """保存数据到数据库"""
        for data in stock_data:
            self.db.insert_stock_data(
                data['symbol'],
                data['timestamp'],
                data['close'],
                data['pe'],
                data['market_capital']
            )
            
    def fetch_all_stocks_data(self, delay=1):
        """获取所有股票数据"""
        symbols = self.get_stock_symbols()
        total_symbols = len(symbols)
        
        logging.info(f"开始获取 {total_symbols} 个股票的数据")
        
        for i, symbol in enumerate(symbols, 1):
            logging.info(f"正在处理第 {i}/{total_symbols} 个股票: {symbol}")
            
            stock_data = self.fetch_stock_data(symbol)
            if stock_data:
                self.save_to_database(stock_data)
            else:
                logging.warning(f"获取 {symbol} 的数据失败")
                sys.exit(1)
                
            # 添加延迟避免请求过快
            if i < total_symbols:
                time.sleep(delay)
                
        logging.info("所有股票数据处理完成")

    def fetch_all_profit_forecasts(self, delay=1):
        """获取所有股票的业绩预测数据"""
        symbols = self.get_stock_symbols()
        total_symbols = len(symbols)
        
        logging.info(f"开始获取 {total_symbols} 个股票的业绩预测数据")
        
        for i, symbol in enumerate(symbols, 1):
            logging.info(f"正在处理第 {i}/{total_symbols} 个股票: {symbol}")

            # 获取业绩预测数据
            result = self._stock_profit_forecast(symbol)
            # 保存到数据库
            if result and result[0] and result[1] and result[2]:
                logging.info(f"成功获取 {symbol} 的业绩预测数据: {result[1]}年预测净利润 {(result[2]/100000000):.2f}亿元")
                forecast_date = datetime.now().strftime('%Y-%m-%d')
                self.db.save_profit_forecast(result[0], result[1], result[2], forecast_date)
            else:
                logging.warning(f"获取 {symbol} 的业绩预测数据失败")
                sys.exit(1)
                
            # 添加延迟避免请求过快
            if i < total_symbols:
                time.sleep(delay)
                
        logging.info("所有股票业绩预测数据处理完成")