# query_execution.py
import pandas as pd
import pymongo
import pymysql
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# MySQL query for the lineitem data
mysql_query = """
SELECT
    L_ORDERKEY,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
FROM
    lineitem
WHERE
    L_SHIPDATE >= '1993-10-01' AND L_SHIPDATE <= '1993-12-31'
    AND L_RETURNFLAG = 'R'
GROUP BY
    L_ORDERKEY
"""
lineitem_df = pd.read_sql(mysql_query, mysql_conn)

# MongoDB query for orders and nation data
orders_coll = mongodb['orders']
orders_query = {
    'O_ORDERDATE': {'$gte': '1993-10-01', '$lte': '1993-12-31'}
}
orders_df = pd.DataFrame(list(orders_coll.find(orders_query)))

nation_coll = mongodb['nation']
nation_df = pd.DataFrame(list(nation_coll.find()))

# Redis query for customer data
customer_keys = redis_conn.keys('customer*')
customer_list = [redis_conn.get(key) for key in customer_keys]
customer_df = pd.concat([pd.read_json(c) for c in customer_list])

# Merging dataframes
result_df = (
    customer_df.merge(orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    .merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
)

# Selecting relevant columns and calculating revenue
result_df = result_df[[
    'C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT'
]].groupby([
    'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT'
]).sum().reset_index()

# Sort the results
result_df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], 
                      ascending=[True, True, True, False], inplace=True)

# Write output to CSV file
result_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
