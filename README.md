# dataProcess_CSI300
## 运行
```bash
#获取数据并插入数据库
python data_fetcher.py
```
```bash
python calculator.py
```
## 配置相关
### 当前数据库配置
postgreSQL
```bash
user: postgres
password: 12345678
database: csi300
host: localhost
port: 5432
```
如有变化请在config.ini中修改

### 依赖库

```shell
pip install akshare psycopg2 matplotlib pandas
```

## 注意事项
1. 由于使用了东方财富的数据源，所以可能需要关闭vpn才能正常获取数据