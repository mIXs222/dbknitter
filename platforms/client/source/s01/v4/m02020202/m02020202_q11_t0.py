# File: query_code.py

import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Establish connection to the MySQL database
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# Connect to Redis using DirectRedis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query partsupp and nation tables from MySQL
mysql_query = """
SELECT
    PS_PARTKEY,
    PS_SUPPKEY,
    PS_AVAILQTY,
    PS_SUPPLYCOST,
    N_NATIONKEY,
    N_NAME
FROM 
    partsupp, nation
WHERE 
    PS_SUPPLYCOST IS NOT NULL
    AND N_NATIONKEY IS NOT NULL
"""
partsupp_nation_df = pd.read_sql(mysql_query, mysql_connection)

# Get supplier data from Redis as a Pandas DataFrame
supplier_df = pd.read_msgpack(redis_client.get('supplier'))

# Filter for 'GERMANY' in the nation name
filtered_df = pd.merge(partsupp_nation_df,
                       supplier_df,
                       how='inner',
                       left_on=['PS_SUPPKEY', 'N_NATIONKEY'],
                       right_on=['S_SUPPKEY', 'S_NATIONKEY'])

filtered_df = filtered_df.loc[filtered_df['N_NAME'] == 'GERMANY']

# Calculate the VALUE and group by PS_PARTKEY
grouped = filtered_df.groupby('PS_PARTKEY').agg(VALUE=('supply_value', 'sum'))

# Subquery to calculate total supply cost for 'GERMANY'
total_supply_cost = filtered_df.agg(TotalValue=('supply_value', 'sum')).iloc[0]['TotalValue'] * 0.0001000000

# Filter according to the HAVING clause
having_df = grouped[grouped['VALUE'] > total_supply_cost]

# Order by VALUE DESC
ordered_df = having_df.sort_values(by='VALUE', ascending=False)

# Write to CSV file
ordered_df.to_csv('query_output.csv', index=False)

# Close MySQL connection
mysql_connection.close()
