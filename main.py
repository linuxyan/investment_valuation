# 主程序入口
import logging
import argparse
from data_fetcher import StockDataFetcher
from data_processor import StockDataProcessor
from database import StockDatabase

def setup_logging():
    """配置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )

def main():
    """主函数"""
    setup_logging()
    
    parser = argparse.ArgumentParser(description='股票数据分析与估值系统')
    parser.add_argument('--mode', choices=['basic_data','profit_data', 'process', 'all'], default='all',
                       help='运行模式: basic_data-仅获取基础数据,profit_data-仅获取业绩预测数据, process-仅处理数据, all-全部执行')
    parser.add_argument('--delay', type=float, default=1.0,
                       help='API请求间隔时间(秒)')
    
    args = parser.parse_args()
    
    logging.info("开始运行股票数据分析与估值系统")
    
    try:
        # 初始化数据库
        db = StockDatabase()
        logging.info("数据库初始化完成")
        
        if args.mode in ['basic_data', 'all']:
            # 获取数据
            logging.info("开始获取股票数据")
            fetcher = StockDataFetcher()
            fetcher.fetch_all_stocks_data(delay=args.delay)
            logging.info("股票数据获取完成")

        if args.mode in ['profit_data', 'all']:
            # 获取业绩预测数据
            logging.info("开始获取股票业绩预测数据")
            fetcher = StockDataFetcher()
            fetcher.fetch_all_profit_forecasts(delay=args.delay)
            logging.info("股票业绩预测数据获取完成")
            
        if args.mode in ['process', 'all']:
            # 处理数据
            logging.info("开始处理股票数据")
            processor = StockDataProcessor()
            results = processor.process_all_stocks()
            processor.save_to_json()
            logging.info(f"股票数据处理完成，共处理 {len(results)} 个股票")
            
        logging.info("股票数据分析与估值系统运行完成")
        
    except Exception as e:
        logging.error(f"系统运行失败: {e}")
        raise

if __name__ == "__main__":
    main()