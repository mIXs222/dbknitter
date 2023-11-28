import pymysql
import pymongo
import pandas as pd
import direct_redis
from datetime import datetime

# MySQL connection - retrieve lineitem data
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')

# Retrieve lineitem data within the specified date range and apply discount
query_lineitem = """
SELECT
    L_ORDERKEY,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue
FROM lineitem
WHERE L_SHIPDATE > '1995-03-15'
GROUP BY L_ORDERKEY;
"""

lineitem_df = pd.read_sql(query_lineitem, mysql_conn)
mysql_conn.close()

# MongoDB connection - retrieve customer data with market segment 'BUILDING'
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
customer_col = mongo_db["customer"]

# Using aggregation to filter 'BUILDING' customers and their orders
pipeline = [
    {"$match": {"C_MKTSEGMENT": "BUILDING"}},
    {"$project": {"C_CUSTKEY": 1}}
]

cust_df = pd.DataFrame(list(customer_col.aggregate(pipeline)))
mongo_client.close()

# Redis connection - retrieve orders data with desired order date
redis_conn = direct_redis.DirectRedis(port=6379, host='redis')

orders = redis_conn.get("orders")
orders_df = pd.DataFrame.from_records(orders)

# Filter orders with order date before '1995-03-15'
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
filtered_orders_df = orders_df[orders_df['O_ORDERDATE'] < datetime(1995, 3, 15)]

# Merging the data frames for final output
merged_df = (
    filtered_orders_df
    .merge(cust_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
)

# Grouping by order key, order date, and shipping priority
grouped_df = merged_df.groupby(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])

# Summing up revenue for each group
final_df = grouped_df['revenue'].sum().reset_index()

# Sorting by revenue in descending order and order date in ascending order
final_df.sort_values(by=['revenue', 'O_ORDERDATE'], ascending=[False, True], inplace=True)

# Output to CSV
final_df.to_csv('query_output.csv', index=False)
