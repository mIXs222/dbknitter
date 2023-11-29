import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)
mysql_cursor = mysql_conn.cursor()
mysql_query = """
SELECT L_ORDERKEY,
       SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
       L_SHIPDATE
FROM lineitem
WHERE L_SHIPDATE > '1995-03-15'
GROUP BY L_ORDERKEY, L_SHIPDATE
HAVING SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) > 0
"""
mysql_cursor.execute(mysql_query)
lineitem_results = mysql_cursor.fetchall()
lineitem_df = pd.DataFrame(lineitem_results, columns=['O_ORDERKEY', 'REVENUE', 'L_SHIPDATE'])
mysql_cursor.close()
mysql_conn.close()

# MongoDB Connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
orders_col = mongodb_db['orders']
orders_query = {
    'O_ORDERDATE': {'$lt': '1995-03-05'},
    'O_ORDERSTATUS': {'$ne': 'BUILDING'}
}
orders_projection = {
    'O_ORDERKEY': 1,
    'O_ORDERDATE': 1,
    'O_SHIPPRIORITY': 1,
    '_id': 0
}
orders_cursor = orders_col.find(orders_query, projection=orders_projection)
orders_df = pd.DataFrame(list(orders_cursor))
mongodb_client.close()

# Redis connection and retrieval
redis_conn = DirectRedis(host='redis', port=6379, db=0)
customer_data = redis_conn.get('customer')
customer_df = pd.read_json(customer_data)

# Joining data
combined_df = pd.merge(
    orders_df,
    lineitem_df,
    left_on='O_ORDERKEY',
    right_on='O_ORDERKEY'
)
combined_df = combined_df[combined_df['L_SHIPDATE'] > '1995-03-15']
final_df = combined_df.merge(
    customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING'],
    left_on='O_ORDERKEY',
    right_on='C_CUSTKEY'
)

# Selecting columns and sorting
output_df = final_df[['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]
output_df = output_df.sort_values(by='REVENUE', ascending=False)

# Writing to CSV
output_df.to_csv('query_output.csv', index=False)
