# profits_query.py
import csv
import datetime
import pymysql
import pymongo
import pandas as pd

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Querying MySQL database for relevant tables
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM nation")
    nations = cursor.fetchall()
    cursor.execute("SELECT * FROM supplier")
    suppliers = cursor.fetchall()

# MongoDB connection setup
mongo_client = pymongo.MongoClient(
    host='mongodb',
    port=27017
)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# Querying MongoDB for part table
parts = list(part_collection.find({}))

mysql_conn.close()

# Redis connection and query setup
from direct_redis import DirectRedis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Reading data from Redis
partsupp_df = pd.read_json(redis_conn.get('partsupp'))
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Calculate profit for each line
merged_df = pd.merge(lineitem_df, partsupp_df, how='inner', on=['L_PARTKEY', 'L_SUPPKEY'])
merged_df['profit'] = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])) - (merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY'])

# Filtering parts with a specified dimension in their names (example: 'dim')
# Assuming 'specified_line' is a user-given input for filtering parts
specified_line = 'dim'
filtered_parts = [part for part in parts if specified_line in part['P_NAME']]

# Filter merged_df for relevant parts only
filtered_df = merged_df[merged_df['L_PARTKEY'].isin([part['P_PARTKEY'] for part in filtered_parts])]

# Aggregate profit by nation and year
profits = (
    filtered_df
    .groupby(['L_SUPPKEY', filtered_df['L_SHIPDATE'].dt.year])
    .agg({'profit': 'sum'})
    .reset_index()
)
profits = pd.merge(profits, nations, left_on='L_SUPPKEY', right_on='N_NATIONKEY')

# Final output: sorting and selecting relevant columns
final_output = profits.sort_values(['N_NAME', 'L_SHIPDATE'], ascending=[True, False])[['N_NAME', 'L_SHIPDATE', 'profit']]

# Writing results to CSV
final_output.to_csv('query_output.csv', index=False)
