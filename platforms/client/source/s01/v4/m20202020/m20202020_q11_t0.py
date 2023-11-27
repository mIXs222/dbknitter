# query.py
import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Query data from MySQL
supplier_query = "SELECT * FROM supplier WHERE S_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'GERMANY')"
supplier_df = pd.read_sql(supplier_query, mysql_conn)

mysql_conn.close()

# Connect to Redis and get data
redis_conn = DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.read_json(redis_conn.get('nation').decode('utf-8'))
partsupp_df = pd.read_json(redis_conn.get('partsupp').decode('utf-8'))

# Filter for nation 'GERMANY'
nation_germany_df = nation_df[nation_df['N_NAME'] == 'GERMANY']

# Merge dataframes
merged_df = partsupp_df.merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(nation_germany_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Perform the query logic
grouped = merged_df.groupby('PS_PARTKEY').apply(lambda x: pd.Series({
    'VALUE': (x['PS_SUPPLYCOST'] * x['PS_AVAILQTY']).sum()
}))

threshold = merged_df.apply(lambda x: (x['PS_SUPPLYCOST'] * x['PS_AVAILQTY']).sum() * 0.0001000000, axis=1).sum()

# Apply having clause
result_df = grouped[grouped['VALUE'] > threshold].reset_index().sort_values('VALUE', ascending=False)

# Write the result to CSV
result_df.to_csv('query_output.csv', index=False)
