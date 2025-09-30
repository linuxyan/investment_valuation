import csv
import json
import requests
import logging
from datetime import datetime
from config import POSITION_DATA_FILE, TRANSFER_DATA_FILE, OUTPUT_JSON_DIR

# 根据股票代码判断市场前缀
def get_market_prefix(stock_code):
    # 如果是港股（代码以 "hk" 开头）
    if stock_code.lower().startswith("hk"):
        return "hk"
    # A 股代码：以 6 开头为上海，其它为深圳
    return 'sh' if stock_code.startswith("6") else 'sz'

# 调用腾讯股票接口获取实时数据
def fetch_stock_data(stock_code):
    # 现金持仓（代码 000000），定为当前价为1，涨跌幅为0%
    if stock_code == "000000":
        return 1.0, 0.0

    market = get_market_prefix(stock_code)
    clean_code = stock_code
    if market == "hk":
        clean_code = stock_code[2:]  # 去除 "hk" 前缀
    url = f"https://qt.gtimg.cn/q={market}{clean_code}"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.text
            # 腾讯接口返回示例：
            # v_sh600519="贵州茅台~600519~SH~1929.00~1930.00~...~...~...~...~涨跌幅数据~...";
            start = data.find('="')
            end = data.rfind('"')
            if start != -1 and end != -1:
                content = data[start+2:end]
                parts = content.split('~')
                # 假设 parts[3] 为当前价，parts[32] 为今日涨跌幅（若存在）
                current_price = float(parts[3])
                if len(parts) > 32:
                    try:
                        today_change = float(parts[32])
                    except:
                        today_change = 0.0
                else:
                    today_change = 0.0
                return current_price, today_change
    except Exception as e:
        logging.error(f"Error fetching data for {stock_code}: {e}")
    return None, None

def fetch_hk_exchange_rate():
    """
    从 API 接口获取实时的港币兑人民币汇率
    """
    url = "https://api.exchangerate-api.com/v4/latest/HKD"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            rate = data.get("rates", {}).get("CNY")
            return rate
    except Exception as e:
        logging.error("Error fetching HKD to CNY exchange rate:", e)
    return 0.85

def process_positions():
    positions = []
    portfolio_total_value = 0.0
    hk_rate = fetch_hk_exchange_rate()  # 从API接口中获取实时的港币兑人民币汇率

    # 读取 position.csv 中的持仓数据
    with open(POSITION_DATA_FILE, newline='', encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            stock_code = row["股票代码"]
            cost_price = float(row["成本价"])
            quantity = float(row["持仓数量"])

            # 调用接口获取实时行情数据
            current_price, today_change = fetch_stock_data(stock_code)
            if current_price is None:
                continue

            # 计算初步数据（未转换）
            market_value = current_price * quantity
            profit_loss = (current_price - cost_price) * quantity
            profit_loss_rate = ((current_price - cost_price) / cost_price * 100) if cost_price != 0 else 0.0
            
            # 如果是港股，将市值和盈亏数据从港币转换为人民币
            if stock_code.lower().startswith("hk"):
                market_value = market_value * hk_rate
                profit_loss = profit_loss * hk_rate

            portfolio_total_value += market_value
            portfolio_total_value = round(portfolio_total_value, 2)
            # 更新数据字典供前端展示
            row.update({
                "当前价": current_price,
                "今日涨跌幅": today_change,
                "持仓市值": round(market_value, 2),
                "持仓盈亏": round(profit_loss, 2),
                "持仓盈亏率": round(profit_loss_rate, 2),
            })
            positions.append(row)

    # 新增：读取 transfer.csv，累计投入成本（转账记录）
    investment_cost = 0.0
    try:
        with open(TRANSFER_DATA_FILE, newline='', encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if not row or len(row) < 2:
                    continue
                date_str = row[0].strip()
                amount_str = row[1].strip()
                transfer_date = datetime.strptime(date_str, "%Y-%m-%d")
                # 只累加当前日期之前或当天的转账记录
                if transfer_date <= datetime.now():
                    investment_cost += float(amount_str)
    except Exception as e:
        logging.error("Error reading transfer.csv:", e)

    result = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "positions": positions,
        "portfolio_value": portfolio_total_value,
        "investment_cost": investment_cost
    }

    # 保存当前持仓数据到 current_position.json
    with open(f"{OUTPUT_JSON_DIR}/current_position.json", "w", encoding="utf-8") as jsonfile:
        json.dump(result, jsonfile, ensure_ascii=False, indent=4)

    # 更新每日市值趋势数据到 market_trend.json
    const_today = datetime.now().strftime("%Y-%m-%d")
    try:
        with open(f"{OUTPUT_JSON_DIR}/market_trend.json", "r", encoding="utf-8") as trend_file:
            trend_data = json.load(trend_file)
        # 按日期排序
        trend_data.sort(key=lambda record: datetime.strptime(record.get("date"), "%Y-%m-%d"))
    except Exception as e:
        trend_data = []

    # 检查是否已存在当天记录，如果存在则更新，否则追加
    updated = False
    for record in trend_data:
        if record.get("date") == const_today:
            record["portfolio_value"] = portfolio_total_value
            record["investment_cost"] = investment_cost
            updated = True
            break
    if not updated:
        trend_data.append({
            "date": const_today,
            "portfolio_value": portfolio_total_value,
            "investment_cost": investment_cost
        })

    # 新增：根据 transfer.csv 文件重新计算所有日期的累计投入成本
    transfers = []
    try:
        with open(TRANSFER_DATA_FILE, newline='', encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if not row or len(row) < 2:
                    continue
                date_str = row[0].strip()
                amount_str = row[1].strip()
                transfer_date = datetime.strptime(date_str, "%Y-%m-%d")
                transfers.append((transfer_date, float(amount_str)))
    except Exception as e:
        logging.error("Error reading transfer.csv to recalc:", e)

    transfers.sort(key=lambda x: x[0])
    for record in trend_data:
        recalc_date = datetime.strptime(record["date"], "%Y-%m-%d")
        cumulative = sum(amt for (t_date, amt) in transfers if t_date <= recalc_date)
        record["investment_cost"] = cumulative

    # 排序后写入文件
    trend_data.sort(key=lambda record: datetime.strptime(record.get("date"), "%Y-%m-%d"))
    with open(f"{OUTPUT_JSON_DIR}/market_trend.json", "w", encoding="utf-8") as trend_file:
        json.dump(trend_data, trend_file, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    process_positions() 