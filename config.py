# 配置文件
import os
from datetime import datetime, timedelta
import pytz

# 时区配置
beijing_tz = pytz.timezone('Asia/Shanghai')

# 数据库配置
DB_PATH = os.path.join('data', 'db', 'stock_valuation.db')

# 获取当前时间（带时区）
now = datetime.now(beijing_tz)
# 计算明天同一时间
tomorrow = now + timedelta(days=1)
# 转换为毫秒级时间戳
timestamp_ms = int(tomorrow.timestamp() * 1000)

# API配置
XUEQIU_API_URL = "https://stock.xueqiu.com/v5/stock/chart/kline.json"
API_PARAMS = {
    'symbol': 'SH000001',
    'begin': timestamp_ms,
    'period': 'day',
    'type': 'before',
    'count': -10,
    'indicator': 'kline,pe,market_capital'
}

# 请求头配置
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': 'https://xueqiu.com/',
    'Cookie': 'u=1986409130; xqat=583cb4adf8795f8460bd283150f78522e2f6e03c;'  # 需要用户自行配置
}

# 时间配置
TRADING_DAYS_PER_YEAR = -250
FIVE_YEARS_TRADING_DAYS = 5 * TRADING_DAYS_PER_YEAR
TEN_YEARS_TRADING_DAYS = 10 * TRADING_DAYS_PER_YEAR

# 文件路径配置
STOCKS_DATA_FILE = os.path.join('data', 'stocks_data.csv')
POSITION_DATA_FILE = os.path.join('data', 'position.csv')
TRANSFER_DATA_FILE = os.path.join('data', 'transfer.csv')
OUTPUT_JSON_DIR = os.path.join('docs', 'data')
