import pymysql
import pymongo
import pandas as pd
import direct_redis
import csv
from datetime import datetime

# Connect to MySQL and retrieve customers in 'BUILDING' market segment
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT C_CUSTKEY, C_NAME FROM customer WHERE C_MKTSEGMENT = 'BUILDING'
    """)
    building_customers = cursor.fetchall()
mysql_conn.close()

df_customers = pd.DataFrame(building_customers, columns=['C_CUSTKEY', 'C_NAME'])

# Connect to MongoDB and retrieve line items shipped after March 15, 1995
client = pymongo.MongoClient('mongodb', 27017)
mongodb = client['tpch']
lineitem_coll = mongodb['lineitem']
lineitems = lineitem_coll.find({
    "L_SHIPDATE": {"$gt": datetime(1995, 3, 15)}
})
df_lineitems = pd.DataFrame(list(lineitems))

# Connect to Redis and retrieve orders placed before March 15, 1995
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
orders_str = redis_conn.get('orders')
df_orders = pd.read_json(orders_str)
df_orders = df_orders[df_orders.O_ORDERDATE < datetime(1995, 3, 15).date()]

# Merge the dataframes
df_merged = df_customers.merge(df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df_merged = df_merged.merge(df_lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
df_merged['REVENUE'] = df_merged['L_EXTENDEDPRICE'] * (1 - df_merged['L_DISCOUNT'])

result_df = df_merged.groupby(
    ['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])['REVENUE'].sum().reset_index()

# Sorting the result as specified
result_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)

# Write the results to a CSV file
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
