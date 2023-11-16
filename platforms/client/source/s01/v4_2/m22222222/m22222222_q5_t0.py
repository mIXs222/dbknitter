import pandas as pd
import redis
import json
from datetime import datetime
from pandas.io.json import json_normalize

# Connect to redis
r = redis.Redis(host='redis', port=6379, db=0)

# Get tables from redis
tables = ['nation', 'region', 'supplier', 'customer', 'orders', 'lineitem']
df_tables = {}
for table in tables:
    data = json.loads(r.get(table))
    df_tables[table] = json_normalize(data)

# Rename keys to remove prefix
for key, df in df_tables.items():
    df.columns = df.columns.str.replace(f"{key.upper()}_", "")

# Type casting for orders
df_tables['orders']['O_ORDERDATE'] = pd.to_datetime(df_tables['orders']['O_ORDERDATE'])

# Query
result = (
    df_tables['customer']
    .merge(df_tables['orders'], left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    .merge(df_tables['lineitem'], left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(df_tables['supplier'], left_on=['C_NATIONKEY', 'L_SUPPKEY'], right_on=['S_NATIONKEY', 'S_SUPPKEY'])
    .merge(df_tables['nation'], left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    .merge(df_tables['region'], left_on='N_REGIONKEY', right_on='R_REGIONKEY')
    .loc[lambda x: 
        (x['R_NAME'] == 'ASIA') & 
        (x['O_ORDERDATE'] >= datetime.strptime('1990-01-01', '%Y-%m-%d')) & 
        (x['O_ORDERDATE'] < datetime.strptime('1995-01-01', '%Y-%m-%d'))
    ]
    .assign(REVENUE=lambda x: x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']))
    .groupby('N_NAME')['REVENUE']
    .sum()
    .sort_values(ascending=False)
    .reset_index()
)

# Output result
result.to_csv('query_output.csv', index=False)
