import pandas as pd
import redis
from dateutil.parser import parse 

# Define table schemas
TABLES = {
    'nation': ['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'],
    'supplier': ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'],
    'customer': ['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'],
    'orders': ['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'],
    'lineitem': ['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT']
}

# Connect to Redis
r = redis.Redis(host='redis', port=6379, db=0)

# Load the data from Redis into Pandas DataFrames
dfs = {table: pd.DataFrame(r.get(table), columns=schema) for table, schema in TABLES.items()}

# Perform the join operations
shipping = dfs['supplier'].merge(dfs['lineitem'], how='inner', left_on='S_SUPPKEY', right_on='L_SUPPKEY') \
    .merge(dfs['orders'], how='inner', left_on='O_ORDERKEY', right_on='O_ORDERKEY') \
    .merge(dfs['customer'], how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY') \
    .merge(dfs['nation'], how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY', suffixes=('', '_supplier')) \
    .merge(dfs['nation'], how='inner', left_on='C_NATIONKEY', right_on='N_NATIONKEY', suffixes=('_supplier', '_customer'))

# Add new columns
shipping['SUPP_NATION'] = shipping['N_NAME_supplier']
shipping['CUST_NATION'] = shipping['N_NAME_customer']
shipping['L_YEAR'] = shipping['L_SHIPDATE'].apply(lambda x: parse(x).year)
shipping['VOLUME'] = shipping['L_EXTENDEDPRICE'] * (1 - shipping['L_DISCOUNT'])

# Filter the data
shipping = shipping[(shipping['SUPP_NATION'].isin(['JAPAN', 'INDIA'])) & (shipping['CUST_NATION'].isin(['JAPAN', 'INDIA'])) &
                    (shipping['L_SHIPDATE'].between('1995-01-01', '1996-12-31'))]

# Group the data
shipment = shipping.groupby(['SUPP_NATION', 'CUST_NATION', 'L_YEAR']).sum().reset_index()

# Sort the data
shipment = shipment.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Write the data to a csv file
shipment.to_csv('query_output.csv', index=False)
