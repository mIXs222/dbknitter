import pandas as pd
import mysql.connector
from pymongo import MongoClient
from pandas.io.json import json_normalize

# Connect to MySQL
mysql_conn = mysql.connector.connect(user='root', password='my-secret-pw',
                              host='mysql', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Fetch ORDERS data from MySQL
mysql_cursor.execute("SELECT * FROM ORDERS")
orders_data = mysql_cursor.fetchall()
orders_df = pd.DataFrame(orders_data, columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 
                                               'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 
                                               'O_COMMENT'])

# Connect to MongoDB
mongo_client = MongoClient('mongodb://mongodb:27017/')
mongo_db = mongo_client['tpch']
lineitem = mongo_db.lineitem

# Fetch LINEITEM data from MongoDB
lineitem_data = list(lineitem.find())
lineitem_df = pd.json_normalize(lineitem_data)

# Merge dataframes on O_ORDERKEY = L_ORDERKEY
merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Apply the conditions mentioned in the WHERE clause

filtered_df = merged_df[(merged_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) & 
                        (merged_df['L_COMMITDATE'] < merged_df['L_RECEIPTDATE']) & 
                        (merged_df['L_SHIPDATE'] < merged_df['L_COMMITDATE']) &
                        (merged_df['L_RECEIPTDATE'] >= pd.to_datetime('1994-01-01')) & 
                        (merged_df['L_RECEIPTDATE'] < pd.to_datetime('1995-01-01'))]

result = filtered_df.groupby('L_SHIPMODE').apply(lambda group: pd.Series({
    'HIGH_LINE_COUNT': sum((group['O_ORDERPRIORITY'] == '1-URGENT') | (group['O_ORDERPRIORITY'] == '2-HIGH')),
    'LOW_LINE_COUNT': sum((group['O_ORDERPRIORITY'] != '1-URGENT') & (group['O_ORDERPRIORITY'] != '2-HIGH'))
})).reset_index()

# Save result to csv file
result.to_csv('query_output.csv')
