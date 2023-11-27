import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Function to execute MySQL query and return result as DataFrame
def fetch_mysql(query):
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
    try:
        df = pd.read_sql_query(query, connection)
    finally:
        connection.close()
    return df

# Function to execute MongoDB query and return result as DataFrame
def fetch_mongodb(collection_name, query):
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    collection = db[collection_name]
    documents = collection.find(query)
    client.close()
    return pd.DataFrame(list(documents))

# Function to get data from Redis and return result as DataFrame
def fetch_redis_df(key):
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    df_json = redis_client.get(key)
    redis_client.close()
    return pd.read_json(df_json, orient='records')

# Fetching data from MySQL
mysql_query = """
SELECT
    L_ORDERKEY, 
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
FROM
    lineitem
WHERE
    L_RETURNFLAG = 'R'
GROUP BY
    L_ORDERKEY;
"""
lineitem_df = fetch_mysql(mysql_query)

# Fetching data from MongoDB
orders_df = fetch_mongodb('orders', {
    'O_ORDERDATE': {
        '$gte': datetime(1993, 10, 1),
        '$lt': datetime(1994, 1, 1)
    }
})
nation_df = fetch_mongodb('nation', {})

# Fetching data from Redis
customer_df = fetch_redis_df('customer')

# Merging DataFrames (MySQL and MongoDB)
merged_df = pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = pd.merge(merged_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = pd.merge(merged_df, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Selecting and renaming columns
final_df = merged_df[[
    'C_CUSTKEY',
    'C_NAME',
    'REVENUE',
    'C_ACCTBAL',
    'N_NAME',
    'C_ADDRESS',
    'C_PHONE',
    'C_COMMENT'
]].rename(columns={
    'N_NAME': 'NATION_NAME'
})

# Sorting as specified in the SQL query
final_df.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, False], inplace=True)

# Writing to CSV
final_df.to_csv('query_output.csv', index=False)
