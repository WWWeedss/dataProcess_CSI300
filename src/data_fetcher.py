import akshare as ak
from database_handler import DatabaseHandler

class StockDataFetcher:
    def __init__(self):
        """
        初始化StockDataFetcher类，连接数据库
        """
        self.db_handler = DatabaseHandler()

    def fetch_csi300_data(self, start_date, end_date):
        """
        获取沪深300的股票数据
        """
        print(f"Fetching CSI 300 data from akshare for the period from {start_date} to {end_date}")
        csi300_data = ak.stock_zh_index_daily_em(symbol="sh000300", start_date=start_date, end_date=end_date)
        return csi300_data

    def save_data_to_db(self, csi300_data):
        """
        将获取到的股票数据存储到数据库中, 并关闭数据库连接
        """
        self.db_handler.create_table()
        self.db_handler.insert_data(csi300_data)
        self.db_handler.close_connection()

def main():
    fetcher = StockDataFetcher()
    csi300_data = fetcher.fetch_csi300_data("20230101", "20231231")  # 获取2023年数据
    fetcher.save_data_to_db(csi300_data)  # 保存数据到数据库

if __name__ == "__main__":
    main()
