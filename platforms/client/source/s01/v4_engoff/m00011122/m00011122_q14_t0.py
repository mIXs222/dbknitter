# promotion_effect_query.py

import pymysql
import pandas as pd
from direct_redis import DirectRedis
import json

# Function to fetch data from MySQL
def fetch_mysql_data(query, connection_params):
    connection = pymysql.connect(host=connection_params['host'],
                                 user=connection_params['user'],
                                 password=connection_params['password'],
                                 database=connection_params['database'])
    try:
        df = pd.read_sql(query, connection)
    finally:
        connection.close()
    return df

# Function to fetch data from Redis
def fetch_redis_data(key, connection_params):
    redis_client = DirectRedis(host=connection_params['host'], port=connection_params['port'], db=connection_params['database'])
    data = redis_client.get(key)
    df = pd.DataFrame(json.loads(data))
    return df

# MySQL connection parameters
mysql_conn_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Redis connection parameters
redis_conn_params = {
    'host': 'redis',
    'port': 6379,
    'database': 0
}

# Query for MySQL
mysql_query = """
SELECT P_PARTKEY 
FROM part 
WHERE P_CONTAINER = 'PROMO'
"""

# Getting promotional part keys from MySQL
promotional_parts_df = fetch_mysql_data(mysql_query, mysql_conn_params)

# Getting lineitem data from Redis
lineitem_df = fetch_redis_data('lineitem', redis_conn_params)

# Converting dates from string to datetime for filtering
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filtering lineitem data based on shipdate and join with promotional parts
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1995-09-01') & 
    (lineitem_df['L_SHIPDATE'] <= '1995-10-01') &
    (lineitem_df['L_PARTKEY'].isin(promotional_parts_df['P_PARTKEY']))
]

# Computing the revenue
filtered_lineitem_df['revenue'] = filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT'])

# Total revenue and total promotional revenue
total_revenue = (lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])).sum()
total_promotional_revenue = filtered_lineitem_df['revenue'].sum()

# Percentage of revenue from promotions
promotion_percentage = (total_promotional_revenue / total_revenue) * 100

# Writing result to CSV
with open('query_output.csv', 'w') as f:
    f.write(f"promotion_percentage,{promotion_percentage}\n")

print(f"Promotion percentage: {promotion_percentage}%")
