import mysql.connector
from pymongo import MongoClient
import pandas as pd

# Connect to MySQL Server
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="my_password"
)

mysql_cursor = mydb.cursor()

# Connect to MongoDB Server
client = MongoClient('mongodb://localhost:27017/')
db = client['tpch']

# Perform SQL Query on MySQL
mysql_cursor.execute("""
SELECT
    L_SHIPMODE,
    SUM(CASE
            WHEN O_ORDERPRIORITY = '1-URGENT'
            OR O_ORDERPRIORITY = '2-HIGH'
            THEN 1
            ELSE 0
    END) AS HIGH_LINE_COUNT,
    SUM(CASE
            WHEN O_ORDERPRIORITY <> '1-URGENT'
            AND O_ORDERPRIORITY <> '2-HIGH'
            THEN 1
            ELSE 0
    END) AS LOW_LINE_COUNT
FROM
    orders,
    lineitem
WHERE
    O_ORDERKEY = L_ORDERKEY
    AND L_SHIPMODE IN ('MAIL', 'SHIP')
    AND L_COMMITDATE < L_RECEIPTDATE
    AND L_SHIPDATE < L_COMMITDATE
    AND L_RECEIPTDATE >= '1994-01-01'
    AND L_RECEIPTDATE < '1995-01-01'
GROUP BY
    L_SHIPMODE
ORDER BY
    L_SHIPMODE
""")
mysql_data = mysql_cursor.fetchall()

# Perform SQL Query on MongoDB
mongodb_orders = pd.DataFrame(list(db.orders.find()))
mongodb_lineitem = pd.DataFrame(list(db.lineitem.find()))

mongodb_data = pd.merge(mongodb_orders, mongodb_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
mongodb_data['HIGH_LINE_COUNT'] = mongodb_data.apply(lambda row: 1 if row.O_ORDERPRIORITY in ['1-URGENT', '2-HIGH'] else 0, axis=1)
mongodb_data['LOW_LINE_COUNT'] = mongodb_data.apply(lambda row: 1 if row.O_ORDERPRIORITY not in ['1-URGENT', '2-HIGH'] else 0, axis=1)
mongodb_data = mongodb_data[(mongodb_data['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) & (mongodb_data['L_COMMITDATE'] < mongodb_data['L_RECEIPTDATE']) & (mongodb_data['L_SHIPDATE'] < mongodb_data['L_COMMITDATE']) & (mongodb_data['L_RECEIPTDATE'] >= '1994-01-01') & (mongodb_data['L_RECEIPTDATE'] < '1995-01-01')]
mongodb_data = mongodb_data.groupby('L_SHIPMODE').sum().reset_index()

# Combine MySQL and MongoDB Data
combined_data = pd.concat([pd.DataFrame(mysql_data), mongodb_data])

# Save to CSV
combined_data.to_csv('query_output.csv', index=False)
