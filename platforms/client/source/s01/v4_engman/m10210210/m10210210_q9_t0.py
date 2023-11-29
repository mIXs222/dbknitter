import pymysql
import pymongo
import pandas as pd
from sqlalchemy import create_engine
import direct_redis
import csv

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)
mysql_cursor = mysql_connection.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient(
    host='mongodb',
    port=27017,
)
mongodb_db = mongodb_client['tpch']

# Connect to Redis (simulate direct_redis.DirectRedis functionality)
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Pandas DataFrame from Redis
part_df = pd.read_msgpack(redis_connection.get('part'))

# Get partsupp and lineitem tables from MySQL
mysql_cursor.execute("SELECT * FROM partsupp")
partsupp_df = pd.DataFrame(mysql_cursor.fetchall(), columns=['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'])

mysql_cursor.execute("SELECT * FROM lineitem")
lineitem_df = pd.DataFrame(mysql_cursor.fetchall(), columns=['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'])

# Get supplier and nation tables from MongoDB
supplier_df = pd.DataFrame(list(mongodb_db.supplier.find({}, {'_id': 0})))
nation_df = pd.DataFrame(list(mongodb_db.nation.find({}, {'_id': 0})))

# Merge dataframes to perform the query equivalent
merged_df = (lineitem_df
             .merge(partsupp_df, how='inner', left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
             .merge(supplier_df, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
             .merge(nation_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
             .merge(part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY'))

# Filter data by the pattern in part names
# (for example, we can specify a type here if we have such criteria)
filtered_df = merged_df[filtered_df['P_NAME'].str.contains('specified_pattern')]

# Calculate profit
filtered_df['YEAR'] = filtered_df['L_SHIPDATE'].map(lambda x: x.year)
filtered_df['PROFIT'] = (filtered_df['L_EXTENDEDPRICE'] * (1-filtered_df['L_DISCOUNT'])) - (filtered_df['PS_SUPPLYCOST'] * filtered_df['L_QUANTITY'])

# Aggregate profit by nation and year
result_df = (filtered_df
             .groupby(['N_NAME', 'YEAR'])['PROFIT']
             .sum()
             .reset_index())

# Sort the results according to the specifications
result_df = result_df.sort_values(by=['N_NAME', 'YEAR'], ascending=[True, False])

# Write result to a CSV file
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Clean up connections
mysql_cursor.close()
mysql_connection.close()
mongodb_client.close()
