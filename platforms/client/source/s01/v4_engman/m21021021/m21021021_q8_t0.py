import pymysql
import pymongo
import pandas as pd
import direct_redis

# MySQL connection and data retrieval
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_query = """
SELECT
    o.O_ORDERDATE,
    l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) AS revenue,
    s.S_NATIONKEY
FROM
    orders o
JOIN
    lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
JOIN
    supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
JOIN
    part p ON l.L_PARTKEY = p.P_PARTKEY
WHERE
    p.P_TYPE = 'SMALL PLATED COPPER'
"""
mysql_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# MongoDB connection and data retrieval
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]
region_col = mongodb["region"]
region_df = pd.DataFrame(list(region_col.find({}, {"_id": 0})))


# Redis connection and data retrieval
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.read_json(redis_conn.get('nation'))
supplier_df = pd.read_json(redis_conn.get('supplier'))

# Processing data
nation_df = nation_df.merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
supplier_df = supplier_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
india_suppliers = supplier_df[supplier_df['N_NAME'] == 'INDIA']['S_SUPPKEY'].tolist()

# Filtering data for Indian suppliers in Asia
india_data = mysql_df[mysql_df['S_NATIONKEY'].isin(india_suppliers)]
india_data['O_ORDERYEAR'] = pd.to_datetime(india_data['O_ORDERDATE']).dt.year
india_data = india_data[(india_data['O_ORDERYEAR'] == 1995) | (india_data['O_ORDERYEAR'] == 1996)]

# Calculate market share
market_share = (
    india_data.groupby('O_ORDERYEAR')['revenue']
    .sum()
    .div(mysql_df.groupby('O_ORDERYEAR')['revenue'].sum())
    .reset_index()
)
market_share.columns = ['order_year', 'market_share']

# Write to CSV
market_share.to_csv('query_output.csv', index=False)
