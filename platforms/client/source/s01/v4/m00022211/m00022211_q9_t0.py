import pymysql
import pymongo
import pandas as pd
import direct_redis
from sqlalchemy import create_engine

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor  # Default cursor returns tuples
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_database = mongodb_client['tpch']

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Function to read data from MySQL
def read_mysql(sql_query, connection):
    return pd.read_sql_query(sql_query, connection)

# Function to read data from MongoDB
def read_mongo(collection, query=None):
    if query is None:
        query = {}
    return pd.DataFrame(list(mongodb_database[collection].find(query)))

# Function to read dataframe from Redis
def read_redis(key):
    df_json = redis_connection.get(key).decode('utf-8')
    return pd.read_json(df_json, orient='table')

# Read data from MySQL
parts_query = "SELECT * FROM part WHERE P_NAME LIKE '%dim%'"
parts_df = read_mysql(parts_query, mysql_connection)

nation_query = "SELECT * FROM nation"
nation_df = read_mysql(nation_query, mysql_connection)

# Read data from MongoDB
orders_df = read_mongo('orders')
lineitem_df = read_mongo('lineitem')

# Read data from Redis - assuming dataframes are stored in JSON
supplier_df = read_redis('supplier')
partsupp_df = read_redis('partsupp')

# Perform joins as needed
merged_df = (
    parts_df
    .merge(supplier_df, left_on='P_PARTKEY', right_on='S_SUPPKEY', how='inner')
    .merge(lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY', how='inner')
    .merge(partsupp_df, left_on=['P_PARTKEY', 'S_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'], how='inner')
    .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY', how='inner')
    .merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY', how='inner')
)

# Perform the necessary transformations
merged_df['O_YEAR'] = merged_df['O_ORDERDATE'].dt.year
merged_df['AMOUNT'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT']) - merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY']

# Group, aggregate and sort as required by the query
result_df = (
    merged_df.groupby(['N_NAME', 'O_YEAR'])
    .agg(SUM_PROFIT=('AMOUNT', 'sum'))
    .reset_index()
    .rename(columns={'N_NAME': 'NATION'})
    .sort_values(by=['NATION', 'O_YEAR'], ascending=[True, False])
)

# Save query results to CSV
result_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_connection.close()
mongodb_client.close()
