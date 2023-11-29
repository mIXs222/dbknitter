import pandas as pd
import pymysql
import pymongo
import direct_redis
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    db='tpch', user='root', passwd='my-secret-pw', host='mysql')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MySQL
mysql_cursor.execute("SELECT * FROM orders")
orders_df = pd.DataFrame(mysql_cursor.fetchall(), columns=[
                         'O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])

mysql_cursor.execute("SELECT * FROM lineitem")
lineitem_df = pd.DataFrame(mysql_cursor.fetchall(), columns=[
                           'L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'])

# Fetch data from MongoDB
supplier_df = pd.DataFrame(list(mongo_db.supplier.find()), columns=[
                           'S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

partsupp_df = pd.DataFrame(list(mongo_db.partsupp.find()), columns=[
                           'PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'])

# Fetch data from Redis
nation_df = pd.read_msgpack(redis_conn.get('nation'))
part_df = pd.read_msgpack(redis_conn.get('part'))

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()

# Join and process the data
joined_df = (
    lineitem_df.merge(orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(supplier_df, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(partsupp_df, how='inner', left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
    .merge(nation_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    .merge(part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
)

# Filter rows where part name contains the specified dim
dim = 'SPECIFIED_DIM' # This should be set to the actual dim required
joined_df = joined_df[joined_df['P_NAME'].str.contains(dim)]

# Calculate profit
joined_df['YEAR'] = pd.to_datetime(joined_df['O_ORDERDATE']).dt.year
joined_df['PROFIT'] = (
    (joined_df['L_EXTENDEDPRICE'] * (1 - joined_df['L_DISCOUNT'])) - (joined_df['PS_SUPPLYCOST'] * joined_df['L_QUANTITY'])
)

# Aggregate results
results = (
    joined_df.groupby(['N_NAME', 'YEAR'])
    .agg({'PROFIT': 'sum'})
    .reset_index()
    .sort_values(['N_NAME', 'YEAR'], ascending=[True, False])
)

# Write to CSV
results.to_csv('query_output.csv', index=False)
