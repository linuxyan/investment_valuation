#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
投资组合数据生成器
用于读取持仓数据并生成README.md文档
"""

import json
import os
from datetime import datetime


def read_position_data(file_path):
    """读取持仓数据文件"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        print(f"错误：文件 {file_path} 不存在")
        return None
    except json.JSONDecodeError:
        print(f"错误：文件 {file_path} 格式不正确")
        return None


def calculate_portfolio_stats(data):
    """计算投资组合统计信息"""
    if not data:
        return None
    
    positions = data.get('positions', [])
    total_value = data.get('portfolio_value', 0)
    total_cost = data.get('investment_cost', 0)
    total_profit = total_value - total_cost
    total_profit_rate = (total_profit / total_cost * 100) if total_cost > 0 else 0
    
    # 计算各持仓占比
    for position in positions:
        position_value = position.get('持仓市值', 0)
        position['持仓占比'] = (position_value / total_value * 100) if total_value > 0 else 0
    
    return {
        'total_value': total_value,
        'total_cost': total_cost,
        'total_profit': total_profit,
        'total_profit_rate': total_profit_rate,
        'positions': positions
    }


def format_number(num):
    """格式化数字显示"""
    if num >= 100000000:
        return f"{num/100000000:.2f}亿"
    elif num >= 10000:
        return f"{num/10000:.2f}万"
    else:
        return f"{num:.2f}"


def generate_markdown(stats, date):
    """生成Markdown格式的README内容"""
    
    # 标题和基本信息
    markdown = f"""# 投资组合概览

**数据日期**: {date}

## 组合总体情况

| 指标 | 数值 |
|------|------|
| 总市值 | {format_number(stats['total_value'])} |
| 总投资成本 | {format_number(stats['total_cost'])} |
| 总盈亏 | {format_number(stats['total_profit'])} |
| 总盈亏率 | {stats['total_profit_rate']:.2f}% |

## 持仓明细

| 股票代码 | 股票名称 | 持仓数量 | 当前价 | 今日涨跌幅 | 持仓市值 | 持仓占比 | 持仓盈亏 | 盈亏率 |
|----------|----------|----------|--------|------------|----------|----------|----------|--------|
"""
    
    # 添加持仓数据行
    for position in stats['positions']:
        stock_code = position.get('股票代码', '')
        stock_name = position.get('股票名称', '')
        quantity = position.get('持仓数量', 0)
        current_price = position.get('当前价', 0)
        daily_change = position.get('今日涨跌幅', 0)
        market_value = position.get('持仓市值', 0)
        holding_ratio = position.get('持仓占比', 0)
        profit = position.get('持仓盈亏', 0)
        profit_rate = position.get('持仓盈亏率', 0)
        
        # 确定涨跌幅颜色
        change_color = "🟢" if daily_change >= 0 else "🔴"
        profit_color = "🟢" if profit >= 0 else "🔴"
        
        markdown += f"| {stock_code} | {stock_name} | {quantity} | {current_price:.2f} | {change_color} {daily_change:.2f}% | {format_number(market_value)} | {holding_ratio:.2f}% | {profit_color} {format_number(profit)} | {profit_rate:.2f}% |\n"
    
    # 添加总结
    markdown += f"""
## 总结

- **组合总市值**: {format_number(stats['total_value'])}
- **总投资成本**: {format_number(stats['total_cost'])}
- **累计盈亏**: {format_number(stats['total_profit'])} ({stats['total_profit_rate']:.2f}%)
- **持仓数量**: {len(stats['positions'])} 只股票

*最后更新: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
    
    return markdown


def main():
    """主函数"""
    # 数据文件路径
    data_file = "frontend/data/current_position.json"
    
    # 读取数据
    data = read_position_data(data_file)
    if not data:
        return
    
    # 计算统计信息
    stats = calculate_portfolio_stats(data)
    if not stats:
        return
    
    # 获取数据日期
    date = data.get('date', '未知日期')
    
    # 生成Markdown内容
    markdown_content = generate_markdown(stats, date)
    
    # 写入README.md文件
    try:
        with open("README.md", "w", encoding="utf-8") as f:
            f.write(markdown_content)
        print("✅ README.md 文件生成成功！")
        print(f"📊 数据日期: {date}")
        print(f"💰 总市值: {format_number(stats['total_value'])}")
        print(f"📈 总盈亏: {format_number(stats['total_profit'])} ({stats['total_profit_rate']:.2f}%)")
    except Exception as e:
        print(f"❌ 文件写入失败: {e}")


if __name__ == "__main__":
    main()