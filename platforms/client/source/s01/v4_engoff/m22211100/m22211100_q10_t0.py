# import necessary libraries
import pymysql
import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# Define connection parameters for MySQL
mysql_conn_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch',
}

# Define query for MySQL
mysql_query = """
SELECT 
    O_CUSTKEY, 
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue_loss
FROM 
    orders
JOIN 
    lineitem ON O_ORDERKEY = L_ORDERKEY
WHERE 
    L_RETURNFLAG = 'R' 
    AND O_ORDERDATE >= '1993-10-01' 
    AND O_ORDERDATE < '1994-01-01'
GROUP BY 
    O_CUSTKEY;
"""

# Execute MySQL query
my_conn = pymysql.connect(**mysql_conn_params)
mysql_df = pd.read_sql(mysql_query, my_conn)
my_conn.close()

# Define connection parameters for MongoDB
mongodb_conn_params = {
    'host': 'mongodb',
    'port': 27017,
}

# Connect to MongoDB and get customer data
mongo_client = pymongo.MongoClient(**mongodb_conn_params)
mongo_db = mongo_client['tpch']
mongo_collection = mongo_db['customer']
customers_cursor = mongo_collection.find(
    {}, 
    {'_id': 0, 'C_CUSTKEY': 1, 'C_NAME': 1, 'C_ADDRESS': 1, 'C_PHONE': 1, 'C_ACCTBAL': 1, 'C_COMMENT': 1}
)
customers_df = pd.DataFrame(list(customers_cursor))
mongo_client.close()

# Merge MySQL and MongoDB results
merged_df = pd.merge(mysql_df, customers_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Define connection parameters for Redis
redis_conn_params = {
    'host': 'redis',
    'port': 6379,
    'db': 0,
}

# Connect to Redis and get nation data
redis_client = direct_redis.DirectRedis(**redis_conn_params)
nation_data = redis_client.get('nation')
nation_df = pd.read_json(nation_data if nation_data is not None else '[]')
redis_client.close()

# Define function to get nation information
def get_nation(nation_key):
    nation = nation_df.loc[nation_df['N_NATIONKEY'] == nation_key]
    if not nation.empty:
        return nation.iloc[0]['N_NAME']
    return None

# Add nation information to merged DataFrame
merged_df['N_NAME'] = merged_df['C_NATIONKEY'].apply(get_nation)

# Finalize the result and sort as per requirement
final_result = merged_df.sort_values(by=['revenue_loss', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, True])

# Select required columns
columns = ['C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT', 'revenue_loss']
final_result = final_result[columns]

# Write the final result to CSV
final_result.to_csv('query_output.csv', index=False)
