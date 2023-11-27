import pandas as pd
import direct_redis
import csv

# Connect to redis
client = direct_redis.DirectRedis(host='localhost', port=6379, db=0)

# Read tables into pandas dataframes
nation = pd.DataFrame(client.get('nation'))
part = pd.DataFrame(client.get('part'))
supplier = pd.DataFrame(client.get('supplier'))
partsupp = pd.DataFrame(client.get('partsupp'))
orders = pd.DataFrame(client.get('orders'))
lineitem = pd.DataFrame(client.get('lineitem'))

# Merge dataframes on the necessary keys
merged = pd.merge(lineitem, part, left_on='L_PARTKEY', right_on='P_PARTKEY')
merged = pd.merge(merged, partsupp, left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
merged = pd.merge(merged, supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged = pd.merge(merged, orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged = pd.merge(merged, nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter rows where P_NAME contains 'dim'
merged = merged[merged['P_NAME'].str.contains('dim')]

# Add O_YEAR column
merged['O_YEAR'] = pd.to_datetime(merged['O_ORDERDATE']).dt.year

# Add AMOUNT column
merged['AMOUNT'] = merged['L_EXTENDEDPRICE'] * (1 - merged['L_DISCOUNT']) - merged['PS_SUPPLYCOST'] * merged['L_QUANTITY']

# Execute the final query
result = merged.groupby(['NATION', 'O_YEAR']).agg({'AMOUNT': 'sum'}).reset_index().sort_values(['NATION', 'O_YEAR'], ascending=[True, False])

# Write result to a csv file
result.to_csv('query_output.csv', index=False)
