# execute_query.py
import pymysql
import pymongo
import pandas as pd
from decimal import Decimal

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Fetch data from MySQL
mysql_query = """
SELECT C_CUSTKEY, C_MKTSEGMENT
FROM customer
WHERE C_MKTSEGMENT = 'BUILDING'
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    customer_data = cursor.fetchall()

mysql_conn.close()

# Create a DataFrame from customer data
df_customer = pd.DataFrame(customer_data, columns=['C_CUSTKEY', 'C_MKTSEGMENT'])

# Fetch data from MongoDB
orders_collection = mongodb['orders']
lineitem_collection = mongodb['lineitem']

orders_data = list(orders_collection.find({'O_ORDERDATE': {'$lt': '1995-03-15'}}))
lineitem_data = list(lineitem_collection.find({'L_SHIPDATE': {'$gt': '1995-03-15'}}))

# Create DataFrames from MongoDB data
df_orders = pd.DataFrame(orders_data)
df_lineitem = pd.DataFrame(lineitem_data)

# Data processing and query execution
df_merged = (df_customer
             .merge(df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
             .merge(df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY'))

# Calculating the revenue
df_merged['REVENUE'] = df_merged.apply(
    lambda row: (Decimal(row['L_EXTENDEDPRICE']) * (1 - Decimal(row['L_DISCOUNT']))),
    axis=1
)

# Grouping and sorting
df_result = (df_merged.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'], as_index=False)
             .agg({'REVENUE': 'sum'})
             .sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True]))

# Selecting columns for output
df_output = df_result[['L_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]

# Writing to CSV
df_output.to_csv('query_output.csv', index=False)
