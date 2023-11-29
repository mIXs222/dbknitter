# Python code to execute the query

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query execution
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_query = """
SELECT s.S_NATIONKEY, o.O_ORDERDATE, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS total_revenue
FROM supplier s
JOIN orders o ON o.O_CUSTKEY = s.S_SUPPKEY
JOIN lineitem l ON l.L_ORDERKEY = o.O_ORDERKEY
WHERE s.S_NATIONKEY = (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'INDIA')
AND o.O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
AND l.L_PARTKEY = (SELECT P_PARTKEY FROM part WHERE P_TYPE = 'SMALL PLATED COPPER')
GROUP BY s.S_NATIONKEY, o.O_ORDERDATE;
"""
mysql_data = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# MongoDB connection and query execution
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongodb_client['tpch']

# Fetch ASIAN region key
asia_region = mongodb['region'].find_one({'R_NAME': 'ASIA'})
asia_region_key = asia_region['R_REGIONKEY'] if asia_region else None

# Fetch INDIA nation key
india_nation = mongodb['nation'].find_one({'N_NAME': 'INDIA'})
india_nation_key = india_nation['N_NATIONKEY'] if india_nation else None

# Fetch SMALL PLATED COPPER part keys
small_plated_copper_parts_cursor = mongodb['part'].find({'P_TYPE': 'SMALL PLATED COPPER'}, {'P_PARTKEY': 1})
small_plated_copper_part_keys = [doc['P_PARTKEY'] for doc in small_plated_copper_parts_cursor]

mongodb_client.close()

# Redis connection and query execution
redis_conn = DirectRedis(host='redis', port=6379, db=0)

orders_df = pd.DataFrame(eval(redis_conn.get('orders')))
lineitem_df = pd.DataFrame(eval(redis_conn.get('lineitem')))

# Combine the data with filters
combined_df = orders_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
filtered_df = combined_df[
    combined_df['O_ORDERDATE'].dt.year.isin([1995, 1996]) &
    combined_df['L_PARTKEY'].isin(small_plated_copper_part_keys) &
    (combined_df['O_CUSTKEY'] == india_nation_key)
].copy()

filtered_df['YEAR'] = filtered_df['O_ORDERDATE'].dt.year
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Calculating market share
market_share = filtered_df.groupby('YEAR')['REVENUE'].sum() / mysql_data['total_revenue'].sum()

# Writing to CSV
output_df = market_share.reset_index()
output_df.columns = ['ORDER_YEAR', 'MARKET_SHARE']
output_df.to_csv('query_output.csv', index=False)
