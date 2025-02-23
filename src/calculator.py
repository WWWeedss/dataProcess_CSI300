import pandas as pd
import matplotlib.pyplot as plt
from database_handler import DatabaseHandler

class DataProcessor:
    def __init__(self, data):
        self.data = data

    def calculate_monthly_high_low(self):
        """
        计算每月的最高股价、最低股价以及对应日期
        """
        self.data['year_month'] = self.data['date'].dt.to_period('M')

        # 每月最高价和最低价对应的日期
        highest_dates = self.data.loc[
            self.data.groupby('year_month')['high'].idxmax(), ['year_month', 'high', 'date']
        ]
        lowest_dates = self.data.loc[
            self.data.groupby('year_month')['low'].idxmin(), ['year_month', 'low', 'date']
        ]

        return highest_dates, lowest_dates

class Plotter:
    def __init__(self, highest_dates, lowest_dates):
        self.highest_dates = highest_dates
        self.lowest_dates = lowest_dates

    def plot(self):
        """
        绘制每月股价波动折线图，并标注最高和最低股价的日期
        """
        plt.figure(figsize=(10, 6))

        # 绘制每月的最高和最低股价
        plt.plot(self.highest_dates['year_month'].astype(str), self.highest_dates['high'], label='Monthly High', marker='o')
        plt.plot(self.lowest_dates['year_month'].astype(str), self.lowest_dates['low'], label='Monthly Low', marker='x')

        # 标注每个月的最高股价和最低股价，并且显示对应的日期
        for _, row in self.highest_dates.iterrows():
            plt.text(row['year_month'].strftime('%Y-%m'), row['high'], f'{row["high"]:.2f}\n({row["date"].strftime("%Y-%m-%d")})',
                     ha='center', va='bottom', fontsize=8)
        for _, row in self.lowest_dates.iterrows():
            plt.text(row['year_month'].strftime('%Y-%m'), row['low'], f'{row["low"]:.2f}\n({row["date"].strftime("%Y-%m-%d")})',
                     ha='center', va='top', fontsize=8)

        # 图表美化
        plt.xticks(rotation=45)
        plt.xlabel('Month')
        plt.ylabel('Price')
        plt.title('Monthly High and Low Prices of CSI 300 in 2023')
        plt.legend()
        plt.tight_layout()
        plt.grid(True)

        # 显示图表
        plt.show()


def main():
    db_handler = DatabaseHandler()

    data = db_handler.query_data('2023-01-01', '2023-12-31')
    db_handler.close_connection()

    if data.empty:
        print("No data found in the database for the specified period.")
        return

    # 处理数据
    data_processor = DataProcessor(data)
    highest_dates, lowest_dates = data_processor.calculate_monthly_high_low()

    # 合并数据表
    merged_data = pd.merge(highest_dates[['year_month', 'high', 'date']], lowest_dates[['year_month', 'low', 'date']],
                           on='year_month', how='left', suffixes=('_high', '_low'))

    # 重新命名列
    merged_data = merged_data[['year_month', 'high', 'date_high', 'low', 'date_low']]
    merged_data.columns = ['year_month', 'monthly_high', 'highest_date', 'monthly_low', 'lowest_date']

    # 输出最终合并的数据表
    print(merged_data)

    # 绘制图形
    plotter = Plotter(highest_dates, lowest_dates)
    plotter.plot()

if __name__ == "__main__":
    main()
