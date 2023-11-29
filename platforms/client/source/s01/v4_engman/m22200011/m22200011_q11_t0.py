import pymysql
import pandas as pd
import direct_redis

# Function to get a dataframe from mysql
def get_mysql_data(sql_query, connection_params):
    conn = pymysql.connect(**connection_params)
    try:
        return pd.read_sql(sql_query, conn)
    finally:
        conn.close()

# Function to get a dataframe from redis
def get_redis_data(key, redis_params):
    dr = direct_redis.DirectRedis(**redis_params)
    df = pd.read_json(dr.get(key))
    return df

# Connection details
mysql_connection_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

redis_params = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Query for mysql
mysql_query = """
SELECT
    PS_PARTKEY,
    SUM(PS_SUPPLYCOST * PS_AVAILQTY) AS value
FROM
    partsupp,
    supplier
WHERE
    supplier.S_SUPPKEY = partsupp.PS_SUPPKEY
GROUP BY
    PS_PARTKEY
HAVING
    value > 0.0001
ORDER BY
    value DESC;
"""

# Get data
partsupp_supplier_data = get_mysql_data(mysql_query, mysql_connection_params)
nation_data = get_redis_data('nation', redis_params)

# Merge data
merged_data = pd.merge(partsupp_supplier_data, nation_data, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter for GERMANY
german_data = merged_data[merged_data['N_NAME'] == 'GERMANY']

# Save to CSV
german_data.to_csv('query_output.csv', index=False)
