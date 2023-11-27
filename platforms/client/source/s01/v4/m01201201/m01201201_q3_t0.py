import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MySQL
mysql_query = """
SELECT
    O_ORDERKEY,
    O_CUSTKEY,
    O_ORDERDATE,
    O_SHIPPRIORITY
FROM
    orders
WHERE
    O_ORDERDATE < '1995-03-15'
"""
mysql_cursor.execute(mysql_query)
orders_data = mysql_cursor.fetchall()
orders_df = pd.DataFrame(list(orders_data), columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
mysql_conn.close()

# Fetch data from MongoDB
lineitem_data = list(mongo_db.lineitem.find(
    {"L_SHIPDATE": {"$gt": "1995-03-15"}},
    {"_id": 0, "L_ORDERKEY": 1, "L_EXTENDEDPRICE": 1, "L_DISCOUNT": 1}
))
lineitem_df = pd.DataFrame(lineitem_data)

# Calculate revenue
lineitem_df['REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

# Fetch data from Redis
customers_df = pd.read_msgpack(redis_client.get('customer'))
# Filter for 'BUILDING' segment
customers_df = customers_df[customers_df['C_MKTSEGMENT'] == 'BUILDING']

# Merge the dataframes to perform the JOIN operation
merged_df = orders_df.merge(customers_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = merged_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Perform GROUP BY and ORDER BY operations
result_df = merged_df.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']).agg({'REVENUE': 'sum'}).reset_index()
result_df = result_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Write the query's output to the file
result_df.to_csv('query_output.csv', index=False)
