# Python code for the query
import pymysql
import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# Connecting to MySQL for 'lineitem' table
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Fetch lineitem data
lineitem_query = """
SELECT L_ORDERKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE
FROM lineitem
WHERE L_SHIPDATE > '1995-03-15'
"""
lineitem_df = pd.read_sql(lineitem_query, mysql_conn)
mysql_conn.close()

# Connecting to MongoDB for 'orders' table
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Fetch orders data
orders_pipeline = [
    {"$match": {
        "O_ORDERDATE": {"$lt": datetime(1995, 3, 15)}
    }},
    {"$project": {
        "O_ORDERKEY": 1,
        "O_CUSTKEY": 1,
        "O_ORDERDATE": 1,
        "O_SHIPPRIORITY": 1
    }}
]
orders_cursor = mongodb_db.orders.aggregate(orders_pipeline)
orders_df = pd.DataFrame(list(orders_cursor))
mongodb_client.close()

# Connecting to Redis for 'customer' table
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch customer data
customer_json = redis_conn.get('customer')
customer_df = pd.read_json(customer_json)

# Filtering customers based on 'BUILDING' market segment
building_customers_df = customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']

# Combining the dataframes
combined_df = orders_df.merge(building_customers_df, how='inner', left_on="O_CUSTKEY", right_on="C_CUSTKEY")
combined_df = combined_df.merge(lineitem_df, how='inner', left_on="O_ORDERKEY", right_on="L_ORDERKEY")

# Calculate revenue
combined_df['REVENUE'] = combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])

# Group by order key, order date and shipping priority
grouped_df = combined_df.groupby(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])['REVENUE'].sum().reset_index()

# Sort the results
sorted_df = grouped_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Write to CSV
sorted_df.to_csv('query_output.csv', index=False)
