import pandas as pd
import pymysql
from pymongo import MongoClient
import direct_redis

# MySQL connection and query
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_query = """
    SELECT O_ORDERKEY, O_TOTALPRICE as REVENUE, O_ORDERDATE, O_SHIPPRIORITY
    FROM orders
    WHERE O_ORDERDATE < '1995-03-05'
    ORDER BY REVENUE DESC
"""
orders_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# MongoDB connection and query for customer data
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
customer_df = pd.DataFrame(list(mongo_db.customer.find({ "C_MKTSEGMENT": "BUILDING" }, { 'C_CUSTKEY': 1 })))

# Sorting customer keys to filter orders
customer_keys = customer_df['C_CUSTKEY'].tolist()
orders_df = orders_df[orders_df['O_CUSTKEY'].isin(customer_keys)]

# Redis connection and dataframe extraction
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
lineitem_df = r.get('lineitem')

# Filter and calculate lineitem data
lineitem_df['SHIPPING_REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] > '1995-03-15')]
lineitem_rev = lineitem_df.groupby('L_ORDERKEY')['SHIPPING_REVENUE'].sum().reset_index()

# Merge dataframes
final_df = orders_df.merge(lineitem_rev, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')
final_df = final_df.sort_values('REVENUE', ascending=False)
final_df = final_df[['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]

# Writing the dataframe to CSV
final_df.to_csv('query_output.csv', index=False)
