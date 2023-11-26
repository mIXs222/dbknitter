import pymysql
import pandas as pd
from redis.exceptions import RedisError
from direct_redis import DirectRedis

# Function to get data from MySQL
def get_mysql_data(query, connection_params):
    try:
        conn = pymysql.connect(**connection_params)
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except pymysql.MySQLError as e:
        print(f"Error connecting to MySQL Platform: {e}")
        return pd.DataFrame()

# Function to get data from Redis
def get_redis_data(key, connection_params):
    try:
        client = DirectRedis(**connection_params)
        data = client.get(key)
        if data:
            df = pd.read_json(data, orient='index')
            return df
        else:
            return pd.DataFrame()
    except RedisError as e:
        print(f"Error connecting to Redis Platform: {e}")
        return pd.DataFrame()

# MySQL Connection Information
mysql_conn = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql'
}

# Redis Connection Information
redis_conn = {
    'hostname': 'redis',
    'port': 6379,
    'db': 0
}

# Get data from MySQL
mysql_query = """
SELECT
    L_PARTKEY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_SHIPDATE
FROM
    lineitem
WHERE
    L_SHIPDATE >= '1995-09-01'
    AND L_SHIPDATE < '1995-10-01'
"""

lineitem_df = get_mysql_data(mysql_query, mysql_conn)

# Get data from Redis
part_df = get_redis_data('part', redis_conn)

# Merge and calculate as per the given SQL query
if not lineitem_df.empty and not part_df.empty:
    merged_df = lineitem_df.merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY', how='inner')
    merged_df['TOTAL_PRICE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
    merged_df['PROMO_TOTAL_PRICE'] = merged_df.apply(
        lambda row: row['TOTAL_PRICE'] if row['P_TYPE'].startswith('PROMO') else 0, axis=1
    )

    result = 100.00 * merged_df['PROMO_TOTAL_PRICE'].sum() / merged_df['TOTAL_PRICE'].sum() if merged_df['TOTAL_PRICE'].sum() != 0 else 0
    result_df = pd.DataFrame([{'PROMO_REVENUE': result}])
    
    # Write to CSV
    result_df.to_csv('query_output.csv', index=False)
else:
    print("No data available to perform the calculation.")
