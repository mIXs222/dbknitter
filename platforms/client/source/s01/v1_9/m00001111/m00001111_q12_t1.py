import os 
from pymongo import MongoClient
import pandas as pd
import pymysql

mongo_client = MongoClient('mongodb://localhost:27017/')
mysql_client = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

db = mongo_client['tpch']
orders = pd.DataFrame(list(db.orders.find()))
lineitem = pd.DataFrame(list(db.lineitem.find()))

mysql_cursor = mysql_client.cursor()
mysql_cursor.execute("SELECT * FROM orders")
ordersMysql = pd.DataFrame(mysql_cursor.fetchall())
mysql_cursor.execute("SELECT * FROM lineitem")
lineitemMysql = pd.DataFrame(mysql_cursor.fetchall())

data = pd.concat([orders, ordersMysql])
data = pd.concat([lineitem, lineitemMysql])

filtered_data = data[
    (data.L_SHIPMODE.isin(['MAIL', 'SHIP'])) &
    (data.L_COMMITDATE < data.L_RECEIPTDATE) &
    (data.L_SHIPDATE < data.L_COMMITDATE) &
    (data.L_RECEIPTDATE >= '1994-01-01') &
    (data.L_RECEIPTDATE < '1995-01-01')
]

grouped_data = filtered_data.groupby('L_SHIPMODE').apply(lambda x: pd.Series({
    'HIGH_LINE_COUNT': ((x.O_ORDERPRIORITY == '1-URGENT') | (x.O_ORDERPRIORITY == '2-HIGH')).sum(),
    'LOW_LINE_COUNT': ((x.O_ORDERPRIORITY != '1-URGENT') & (x.O_ORDERPRIORITY != '2-HIGH')).sum()
})).reset_index()

grouped_data.to_csv('query_output.csv', index=False)
