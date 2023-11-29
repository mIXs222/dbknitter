# query.py

import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL database
my_sql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Query nation table in MySQL to get German nation key
with my_sql_conn.cursor() as cursor:
    cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'GERMANY'")
    nation_result = cursor.fetchone()
    german_nation_key = nation_result[0] if nation_result else None

# Close MySQL connection
my_sql_conn.close()

# Connect to Redis database
redis_conn = DirectRedis(host='redis', port=6379)

# Get supplier and partsupp tables from Redis and convert to DataFrames
supplier_df = pd.read_json(redis_conn.get('supplier'))
partsupp_df = pd.read_json(redis_conn.get('partsupp'))

# Close Redis connection
redis_conn.close()

# Filter suppliers in GERMANY and calculate total available parts value
if german_nation_key is not None:
    german_suppliers_df = supplier_df[supplier_df['S_NATIONKEY'] == german_nation_key].copy()
    german_partsupp_df = partsupp_df[partsupp_df['PS_SUPPKEY'].isin(german_suppliers_df['S_SUPPKEY'])]
    german_partsupp_df['PART_VALUE'] = german_partsupp_df['PS_AVAILQTY'] * german_partsupp_df['PS_SUPPLYCOST']
    total_value = german_partsupp_df['PART_VALUE'].sum()

    # Find all parts that represent a significant percentage of the total value
    important_parts_df = german_partsupp_df[german_partsupp_df['PART_VALUE'] > 0.0001 * total_value]

    # Select relevant columns and sort the results
    important_parts_df = important_parts_df[['PS_PARTKEY', 'PART_VALUE']]
    important_parts_df = important_parts_df.sort_values(by='PART_VALUE', ascending=False)

    # Save the results to CSV
    important_parts_df.to_csv('query_output.csv', index=False)
