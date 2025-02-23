import psycopg2
import configparser
import pandas as pd
import os

# 获取当前文件所在目录的绝对路径
base_dir = os.path.dirname(os.path.abspath(__file__))

# 拼接config.ini的绝对路径
config_path = os.path.join(base_dir, '..', 'config.ini')

class DatabaseHandler:
    def __init__(self, config_file = config_path):
        """
                初始化数据库配置，创建数据库连接
        """
        self.database_config = self.load_config(config_file)
        self.conn = None
        self.cursor = None
        self.create_database_connection()

    def load_config(self, config_file):
        """
        从配置文件加载数据库配置
        """
        config = configparser.ConfigParser()
        config.read(config_file)
        database_config = {
            'dbname': config.get('database', 'dbname'),
            'user': config.get('database', 'user'),
            'password': config.get('database', 'password'),
            'host': config.get('database', 'host'),
            'port': config.get('database', 'port')
        }
        return database_config

    def create_database_connection(self):
        """
        创建数据库连接
        """
        try:
            self.conn = psycopg2.connect(
                dbname=self.database_config['dbname'],
                user=self.database_config['user'],
                password=self.database_config['password'],
                host=self.database_config['host'],
                port=self.database_config['port']
            )
            self.cursor = self.conn.cursor()
            print("Database connection established successfully.")
        except Exception as e:
            print(f"Error while connecting to database: {e}")

    def create_table(self):
        """
        创建表格，如果表格不存在的话
        """
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS csi300data (
            date DATE PRIMARY KEY,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT
        );
        """)
        self.conn.commit()
        print("Table created successfully or already exists.")

    def insert_data(self, csi300_data):
        """
        插入数据到数据库
        """
        for _, row in csi300_data.iterrows():
            self.cursor.execute("""
            INSERT INTO csi300data (date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING;
            """, (row['date'], row['open'], row['high'], row['low'], row['close'], row['volume']))
        self.conn.commit()
        print("Data inserted successfully.")

    def query_data(self, start_date, end_date):
        """
        根据时间范围查询数据
        """
        query = """
        SELECT date, open, high, low, close FROM csi300data
        WHERE date >= %s AND date <= %s
        ORDER BY date ASC;
        """
        self.cursor.execute(query, (start_date, end_date))
        rows = self.cursor.fetchall()
        df = pd.DataFrame(rows, columns=['date', 'open', 'high', 'low', 'close'])
        df['date'] = pd.to_datetime(df['date'])
        return df

    def close_connection(self):
        """
        关闭数据库连接
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("Database connection closed.")


