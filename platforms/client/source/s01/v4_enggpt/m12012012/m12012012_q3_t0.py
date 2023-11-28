# python_code.py

import pymysql
import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Query to select customers in the 'BUILDING' market segment
mysql_query = """
SELECT C_CUSTKEY, C_NAME
FROM customer
WHERE C_MKTSEGMENT = 'BUILDING'
"""

with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    customers = cursor.fetchall()

# Convert to DataFrame
customer_df = pd.DataFrame(customers, columns=['C_CUSTKEY', 'C_NAME'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# Query to select orders placed before 1995-03-15
orders = list(mongo_db.orders.find(
    {
        'O_ORDERDATE': {'$lt': datetime(1995, 3, 15)}
    },
    {
        '_id': 0, 'O_ORDERKEY': 1, 'O_CUSTKEY': 1, 'O_ORDERDATE': 1, 'O_SHIPPRIORITY': 1
    }
))

# Convert to DataFrame
orders_df = pd.DataFrame(orders)

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve 'lineitem' table from Redis
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Filter line items with ship date after 1995-03-15
lineitem_df = lineitem_df[lineitem_df.L_SHIPDATE > datetime(1995, 3, 15).date()]

# Perform necessary calculations and create a revenue column
lineitem_df['REVENUE'] = lineitem_df.L_EXTENDEDPRICE * (1 - lineitem_df.L_DISCOUNT)

# Merge DataFrames to assemble necessary information
merged_df = (lineitem_df
             .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
             .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY'))

# Group by the specified columns and sum the revenue
grouped_df = (merged_df.groupby(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
              .agg({'REVENUE': 'sum'})
              .reset_index())

# Sort the grouped results as specified
sorted_df = grouped_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Write output to a CSV file
sorted_df.to_csv('query_output.csv', index=False)

# Close all the connections
mysql_conn.close()
mongo_client.close()
redis_client.close()
