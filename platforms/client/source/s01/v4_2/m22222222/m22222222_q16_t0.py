import redis
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import json

REDIS_HOST = 'redis'
REDIS_PORT = 6379

# Connect to Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

# Get data from Redis
partsupp = json.loads(r.get('partsupp'))
part = json.loads(r.get('part'))
supplier = json.loads(r.get('supplier'))

# Convert to dataframes
df_partsupp = pd.DataFrame.from_records(partsupp)
df_part = pd.DataFrame.from_records(part)
df_supplier = pd.DataFrame.from_records(supplier)

# Execute query in Pandas
df = pd.merge(df_part, df_partsupp, how='inner', on='P_PARTKEY')
df = df[(df['P_BRAND'] != 'Brand#45') &
        (~df['P_TYPE'].str.contains('MEDIUM POLISHED')) &
        (df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9])) &
        (~df['PS_SUPPKEY'].isin(df_supplier[df_supplier['S_COMMENT'].str.contains('Customer Complaints')]['S_SUPPKEY'].unique()))]
df = df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])['PS_SUPPKEY'].nunique().reset_index(name='SUPPLIER_CNT')
df = df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write to CSV
df.to_csv('query_output.csv', index=False)
