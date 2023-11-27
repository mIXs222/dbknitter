import pymysql
import pymongo
import redis
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Perform queries and join data

# Fetch necessary tables from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM nation")
    nation_df = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

    cursor.execute("SELECT * FROM supplier")
    supplier_df = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

    cursor.execute("SELECT O_ORDERKEY, YEAR(O_ORDERDATE) as O_YEAR, O_ORDERDATE FROM orders")
    orders_df = pd.DataFrame(cursor.fetchall(), columns=['O_ORDERKEY', 'O_YEAR', 'O_ORDERDATE'])

mysql_conn.close()

# Fetch necessary collections from MongoDB
part_cur = mongodb.part.find()
parts_df = pd.DataFrame(list(part_cur))

# Fetch necessary tables from Redis
partsupp_df = pd.read_json(redis_conn.get('partsupp'))
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Query and computation
# Define a specified dim (dimension) as an example since the original query does not provide one
specified_dim = 'DIM'

# Filter parts that contain the specified dim
parts_filtered_df = parts_df[parts_df['P_NAME'].str.contains(specified_dim)]

# Join tables to have all data in one dataframe
joined_df = (
    lineitem_df
    .merge(parts_filtered_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
    .merge(partsupp_df, on=['L_PARTKEY', 'L_SUPPKEY'])
    .merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
)

# Calculate profit
joined_df['PROFIT'] = (joined_df['L_EXTENDEDPRICE'] * (1 - joined_df['L_DISCOUNT'])) - (joined_df['PS_SUPPLYCOST'] * joined_df['L_QUANTITY'])

# Group by nation and year, and sum profit
result_df = (
    joined_df.groupby(['N_NAME', 'O_YEAR'])
    .agg({'PROFIT': 'sum'})
    .reset_index()
    .sort_values(by=['N_NAME', 'O_YEAR'], ascending=[True, False])
)

# Write result to CSV file
result_df.to_csv('query_output.csv', index=False)
