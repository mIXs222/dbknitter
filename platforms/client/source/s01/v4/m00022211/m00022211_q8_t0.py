# query.py

import pymysql
import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# MySQL Connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB Connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_tpch_db = mongo_client["tpch"]

# Redis Connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MySQL
mysql_query = """
SELECT 
    n.N_NAME AS nation, 
    n.N_NATIONKEY, 
    r.R_REGIONKEY,
    r.R_NAME AS region, 
    p.P_PARTKEY, 
    p.P_TYPE
FROM 
    nation n
JOIN 
    region r ON n.N_REGIONKEY = r.R_REGIONKEY AND r.R_NAME = 'ASIA'
JOIN 
    part p ON p.P_TYPE = 'SMALL PLATED COPPER'
"""
mysql_cursor.execute(mysql_query)
nation_region_part = pd.DataFrame(mysql_cursor.fetchall(), columns=['nation', 'N_NATIONKEY', 'R_REGIONKEY', 'region', 'P_PARTKEY', 'P_TYPE'])

# Retrieve data from MongoDB
orders_query = {
    "O_ORDERDATE": {"$gte": datetime(1995, 1, 1), "$lte": datetime(1996, 12, 31)}
}
orders = mongo_tpch_db.orders.find(orders_query, {'_id': 0, 'O_ORDERKEY': 1, 'O_CUSTKEY': 1, 'O_ORDERDATE': 1})

lineitem = mongo_tpch_db.lineitem.find({}, {'_id': 0, 'L_PARTKEY': 1, 'L_ORDERKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1, 'L_SUPPKEY': 1})

orders_df = pd.DataFrame(orders).assign(O_YEAR=lambda x: x['O_ORDERDATE'].dt.year)
lineitem_df = pd.DataFrame(lineitem)

# Retrieve data from Redis
supplier_df = pd.read_json(redis_client.get('supplier'))
customer_df = pd.read_json(redis_client.get('customer'))

# Close MySQL Connection
mysql_conn.close()

# Merge Datasets
merged_df = (
    orders_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(nation_region_part, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
    .merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
)

# Calculate Volume
merged_df['VOLUME'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Calculate Market Share
result_df = (merged_df
            .assign(INDIA_VOLUME=lambda x: x.apply(lambda y: y['VOLUME'] if y['nation'] == 'INDIA' else 0, axis=1))
            .groupby('O_YEAR')
            .agg({'INDIA_VOLUME': 'sum', 'VOLUME': 'sum'})
            .reset_index()
            .assign(MKT_SHARE=lambda x: x['INDIA_VOLUME'] / x['VOLUME']))

# Save to CSV
result_df.to_csv('query_output.csv', index=False)
