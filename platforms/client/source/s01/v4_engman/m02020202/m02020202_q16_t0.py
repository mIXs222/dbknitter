import pymysql
import pandas as pd
import csv

# Function to query mysql using pymysql and return a DataFrame
def query_mysql(sql, connection_params):
    connection = pymysql.connect(**connection_params)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            data = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
    finally:
        connection.close()
    return pd.DataFrame(data, columns=columns)

# Connection information for mysql
mysql_connection_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# SQL query for mysql
mysql_query = """
    SELECT P_BRAND, P_TYPE, P_SIZE, COUNT(DISTINCT PS_SUPPKEY) AS supplier_count
    FROM part 
    JOIN partsupp ON P_PARTKEY = PS_PARTKEY 
    WHERE P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9) 
    AND P_TYPE NOT LIKE 'MEDIUM POLISHED%%' 
    AND P_BRAND <> 'Brand#45' 
    GROUP BY P_BRAND, P_TYPE, P_SIZE
    ORDER BY supplier_count DESC, P_BRAND, P_TYPE, P_SIZE;
"""

# Fetch the data from mysql
parts_supplier_df = query_mysql(mysql_query, mysql_connection_params)

# Function to connect to Redis and get the data
def get_redis_data(redis_params, table_name):
    from direct_redis import DirectRedis
    redis_connection = DirectRedis(**redis_params)
    df_json = redis_connection.get(table_name)
    if df_json:
        return pd.read_json(df_json)
    else:
        # No data found in Redis, return an empty DataFrame with expected columns
        return pd.DataFrame(columns=["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT"])

# Connection information for redis
redis_connection_params = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Get the data from redis
supplier_df = get_redis_data(redis_connection_params, 'supplier')

# Filter out suppliers with complaints
supplier_df_filtered = supplier_df[~supplier_df['S_COMMENT'].str.contains('Customer Complaints')]

# Merge parts_supplier_df with supplier_df_filtered to get the final output
final_df = pd.merge(parts_supplier_df, supplier_df_filtered, left_on='supplier_count', right_on='S_SUPPKEY')
final_df = final_df[['P_BRAND', 'P_TYPE', 'P_SIZE', 'supplier_count']]

# Write the merged data to CSV
final_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
