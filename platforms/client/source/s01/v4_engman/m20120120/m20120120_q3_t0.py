import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# MySQL Queries
mysql_query = """
SELECT
    lineitem.L_ORDERKEY,
    SUM(lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT)) AS REVENUE
FROM
    lineitem
WHERE
    lineitem.L_SHIPDATE > '1995-03-15'
GROUP BY
    lineitem.L_ORDERKEY
"""

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute(mysql_query)
lineitem_results = mysql_cursor.fetchall()
mysql_cursor.close()
mysql_conn.close()

lineitem_df = pd.DataFrame(lineitem_results, columns=['O_ORDERKEY', 'REVENUE'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
customer_collection = mongo_db['customer']
customer_df = pd.DataFrame(list(customer_collection.find({'C_MKTSEGMENT': 'BUILDING'})))
mongo_client.close()

# DirectRedis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)
orders_df = redis_conn.get('orders')
redis_conn.close()

# Convert json string to DataFrame
orders_df = pd.read_json(orders_df, orient='records')

# Apply datetime conversion to the ORDERDATE column
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])

# Filter orders before 1995-03-05
orders_df = orders_df[orders_df['O_ORDERDATE'] < datetime(1995, 3, 5)]

# Merge dataframes
result_df = orders_df.merge(customer_df, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
result_df = result_df.merge(lineitem_df, how='inner', on='O_ORDERKEY')

# Select and sort the final result
final_result = result_df[['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]
final_result.sort_values(by=['REVENUE'], ascending=False, inplace=True)

# Write result to CSV
final_result.to_csv('query_output.csv', index=False)
