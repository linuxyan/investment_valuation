# 数据库模块
import sqlite3
import logging
from datetime import datetime
import pytz
from config import DB_PATH
import os,sys

class StockDatabase:
    def __init__(self):
        self.db_path = DB_PATH
        self._create_tables()
        
    def _create_tables(self):
        """创建数据库表"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 创建股票基础数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_basic_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                close REAL NOT NULL,
                pe REAL,
                market_capital REAL,
                shares_outstanding REAL,
                created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, timestamp)
            )
        ''')
        
        # 创建股票估值结果表（支持历史记录）
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_valuation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                current_close REAL NOT NULL,
                current_pe REAL NOT NULL,
                avg_pe_5y REAL NOT NULL,
                std_pe_5y REAL NOT NULL,
                pe_percentile_90 REAL NOT NULL,
                reasonable_pe REAL NOT NULL,
                pe_valuation REAL NOT NULL,
                net_profit_valuation REAL NOT NULL,
                pe_buy_point REAL NOT NULL,
                profit_buy_point REAL NOT NULL,
                predicted_net_profit REAL,
                profit_date TEXT,
                calculation_date TEXT NOT NULL,
                UNIQUE(symbol, timestamp, profit_date)
            )
        ''')
        
        # 创建股票业绩预测表（支持历史记录）
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_profit_forecast (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                forecast_year INTEGER NOT NULL,
                forecast_net_profit REAL NOT NULL,
                forecast_date TEXT NOT NULL,  -- 预测数据获取日期
                created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, forecast_year, forecast_date)
            )
        ''')
        
        conn.commit()
        conn.close()
        logging.info("数据库表创建完成")
        
    def insert_stock_data(self, symbol, timestamp, close, pe, market_capital):
        """插入股票基础数据"""

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            shares_outstanding = market_capital / close if close and close > 0 else 0
            cursor.execute('''
                INSERT OR REPLACE INTO stock_basic_data 
                (symbol, timestamp, close, pe, market_capital, shares_outstanding)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (symbol, timestamp, close, pe, market_capital, shares_outstanding))
            
            conn.commit()
            # logging.info(f"成功插入数据: {symbol} - {self.timestamp_to_datetime(timestamp)} - {timestamp}")
        except Exception as e:
            logging.error(f"插入数据失败: {e} {symbol} - {timestamp} - {close} - {pe} - {market_capital}")
            conn.rollback()
        finally:
            conn.close()
            
    def save_valuation_result(self, valuation_data):
        """保存估值结果（支持历史记录）"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO stock_valuation 
                (symbol, timestamp, current_close, current_pe, avg_pe_5y, std_pe_5y, 
                pe_percentile_90, reasonable_pe, pe_valuation, net_profit_valuation,
                pe_buy_point, profit_buy_point, predicted_net_profit, profit_date, calculation_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
                valuation_data['symbol'],
                valuation_data['timestamp'],
                valuation_data['current_close'],
                valuation_data['current_pe'],
                valuation_data['avg_pe_5y'],
                valuation_data['std_pe_5y'],
                valuation_data['pe_percentile_90'],
                valuation_data['reasonable_pe'],
                valuation_data['pe_valuation'],
                valuation_data['net_profit_valuation'],
                valuation_data['pe_buy_point'],
                valuation_data['profit_buy_point'],
                valuation_data.get('predicted_net_profit'),
                valuation_data.get('profit_date'),
                valuation_data['calculation_date']
            ))
            
            conn.commit()
            logging.info(f"成功保存估值结果: {valuation_data['symbol']} - {valuation_data['calculation_date']}")
        except Exception as e:
            logging.error(f"保存估值结果失败: {e}")
            conn.rollback()
            sys.exit()
        finally:
            conn.close()

            
    def get_stock_data_by_symbol(self, symbol, limit=None):
        """根据股票代码获取数据"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = '''
            SELECT symbol, timestamp, close, pe, market_capital, shares_outstanding
            FROM stock_basic_data 
            WHERE symbol = ? 
            ORDER BY timestamp DESC
        '''
        
        if limit:
            query += f' LIMIT {limit}'
            
        cursor.execute(query, (symbol,))
        results = cursor.fetchall()
        conn.close()
        
        return results
        
    def check_symbol_exists(self, symbol):
        """检查股票代码是否存在"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM stock_basic_data WHERE symbol = ?', (symbol,))
        count = cursor.fetchone()[0]
        conn.close()
        
        return count > 0

    def save_profit_forecast(self, symbol, forecast_year, forecast_net_profit, forecast_date):
        """保存股票业绩预测数据（插入或更新）"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # 使用INSERT OR REPLACE语句，自动处理插入或更新
            cursor.execute('''
                INSERT OR REPLACE INTO stock_profit_forecast 
                (symbol, forecast_year, forecast_net_profit, forecast_date)
                VALUES (?, ?, ?, ?)
            ''', (symbol, forecast_year, forecast_net_profit, forecast_date))
            
            conn.commit()
            logging.info(f"成功保存业绩预测数据: {symbol} - {forecast_year} - {forecast_date}")
                
        except Exception as e:
            logging.error(f"保存业绩预测数据失败: {e}")
            conn.rollback()
        finally:
            conn.close()
            
    def get_profit_forecast_by_symbol(self, symbol, forecast_date=None):
        """根据股票代码获取业绩预测数据，可指定预测日期"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if forecast_date:
            cursor.execute('''
                SELECT symbol, forecast_year, forecast_net_profit, forecast_date, created_time
                FROM stock_profit_forecast 
                WHERE symbol = ? AND forecast_date = ?
                ORDER BY forecast_year DESC
            ''', (symbol, forecast_date))
        else:
            cursor.execute('''
                SELECT symbol, forecast_year, forecast_net_profit, forecast_date, created_time
                FROM stock_profit_forecast 
                WHERE symbol = ? 
                ORDER BY forecast_date DESC, forecast_year DESC
            ''', (symbol,))
        
        results = cursor.fetchall()
        conn.close()
        return results
        

            
    def get_latest_profit_forecast(self, symbol):
        """获取股票最新的业绩预测数据"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT symbol, forecast_year, forecast_net_profit, forecast_date, created_time
            FROM stock_profit_forecast 
            WHERE symbol = ? 
            ORDER BY forecast_date DESC, forecast_year DESC
            LIMIT 1
        ''', (symbol,))
        
        result = cursor.fetchone()
        conn.close()
        return result

    def get_all_valuation_data(self):
        """从数据库估值表查询所有数据"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT symbol, timestamp, current_close, current_pe, avg_pe_5y, std_pe_5y,
                   pe_percentile_90, reasonable_pe, pe_valuation, net_profit_valuation,
                   pe_buy_point, profit_buy_point, predicted_net_profit, profit_date, calculation_date
            FROM stock_valuation 
            ORDER BY symbol, timestamp DESC
        ''')
        
        results = cursor.fetchall()
        conn.close()
        
        # 转换为字典格式
        valuation_data = []
        for row in results:
            data = {
                'symbol': row[0],
                'timestamp': row[1],
                'current_close': row[2],
                'current_pe': row[3],
                'avg_pe_5y': row[4],
                'std_pe_5y': row[5],
                'pe_percentile_90': row[6],
                'reasonable_pe': row[7],
                'pe_valuation': row[8],
                'net_profit_valuation': row[9],
                'pe_buy_point': row[10],
                'profit_buy_point': row[11],
                'predicted_net_profit': row[12],
                'profit_date': row[13],
                'calculation_date': row[14]
            }
            valuation_data.append(data)
        
        return valuation_data

    def timestamp_to_datetime(self, timestamp_ms):
        """将毫秒级时间戳转换为北京时间格式"""
        # 转换为秒（浮点数）
        timestamp_sec = timestamp_ms / 1000.0
        
        # 创建UTC时间对象
        utc_time = datetime.utcfromtimestamp(timestamp_sec)
        
        # 设置时区为UTC
        utc_time = utc_time.replace(tzinfo=pytz.utc)
        
        # 转换为北京时间
        beijing_tz = pytz.timezone('Asia/Shanghai')
        beijing_time = utc_time.astimezone(beijing_tz)
        
        # 格式化输出
        return beijing_time.strftime("%Y-%m-%d")
