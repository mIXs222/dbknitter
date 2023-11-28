import pymysql
import pymongo
import pandas as pd
from redis import Redis
from direct_redis import DirectRedis

# MySQL connection and query
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_query = """
SELECT 
    o.O_CUSTKEY, 
    SUM(l_extendedprice * (1 - l_discount)) as REVENUE
FROM 
    orders o
WHERE 
    o.O_ORDERDATE >= '1993-10-01' AND o.O_ORDERDATE <= '1993-12-31'
GROUP BY 
    o.O_CUSTKEY
"""
mysql_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# MongoDB connection and query
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client.tpch
mongo_customers = list(mongo_db.customer.find(
    {},
    {'_id': 0, 'C_CUSTKEY': 1, 'C_NAME': 1, 'C_ACCTBAL': 1, 'C_ADDRESS': 1, 'C_PHONE': 1, 'C_COMMENT': 1}
))
mongo_df = pd.DataFrame(mongo_customers)

# Redis connection and query
redis_conn = DirectRedis(host='redis', port=6379, db=0)
lineitems = redis_conn.get('lineitem')
redis_df = pd.read_json(lineitems, orient='records')
redis_df = redis_df[redis_df['L_RETURNFLAG'] == 'R']

# Data merging and aggregation
merged_df = pd.merge(
    mysql_df,
    mongo_df,
    how='inner',
    left_on='O_CUSTKEY',
    right_on='C_CUSTKEY'
)
merged_df = pd.merge(
    merged_df,
    redis_df,
    how='inner',
    left_on='O_CUSTKEY',
    right_on='L_ORDERKEY'
)
aggregated_df = merged_df.groupby([
    'C_CUSTKEY', 
    'C_NAME',
    'C_ACCTBAL',
    'C_ADDRESS',
    'C_PHONE',
    'C_COMMENT'
]).agg({'REVENUE': 'sum'}).reset_index()

# Sorting results
aggregated_df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False], inplace=True)

# Write results to CSV
aggregated_df.to_csv('query_output.csv', index=False)
