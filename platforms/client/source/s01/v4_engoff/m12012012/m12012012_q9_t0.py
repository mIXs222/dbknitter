# Import necessary libraries
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and querying
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_query = """
SELECT P_PARTKEY, P_TYPE FROM part
"""
parts_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# Filter parts with the specified dimension in their names
specified_dim = 'SPECIFIED_DIM'  # Replace SPECIFIED_DIM with the actual dimension string provided
parts_df = parts_df[parts_df['P_TYPE'].str.contains(specified_dim)]

# MongoDB connection and querying
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
suppliers_df = pd.DataFrame(list(mongo_db.supplier.find({}, {'_id': 0})))
nations_df = pd.DataFrame(list(mongo_db.nation.find({}, {'_id': 0})))

# Redis connection and querying
redis_conn = DirectRedis(host='redis', port=6379, db=0)
partsupp_df = pd.read_json(redis_conn.get('partsupp'))
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Compute profit
lineitem_df = lineitem_df.merge(parts_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
lineitem_df = lineitem_df.merge(partsupp_df, on=['L_PARTKEY', 'L_SUPPKEY'])
lineitem_df = lineitem_df.merge(suppliers_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
lineitem_df = lineitem_df.merge(nations_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Calculate the profit
lineitem_df['YEAR'] = pd.to_datetime(lineitem_df['L_SHIPDATE']).dt.year
lineitem_df['PROFIT'] = (lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])) - (lineitem_df['PS_SUPPLYCOST'] * lineitem_df['L_QUANTITY'])

# Perform the aggregation
profit_by_nation_year = lineitem_df.groupby(['N_NAME', 'YEAR']).agg({'PROFIT': 'sum'}).reset_index()

# Sort results as required
profit_by_nation_year.sort_values(by=['N_NAME', 'YEAR'], ascending=[True, False], inplace=True)

# Write to CSV
profit_by_nation_year.to_csv('query_output.csv', index=False)

mongo_client.close()
