import pandas as pd
import redis
from io import StringIO

# Connect to Redis
r = redis.Redis(
    host='redis',
    port=6379,
    db=0)

# Load tables into DataFrames
nation = pd.read_json(r.get('nation'))
region = pd.read_json(r.get('region'))
part = pd.read_json(r.get('part'))
supplier = pd.read_json(r.get('supplier'))
partsupp = pd.read_json(r.get('partsupp'))

# Merge DataFrames
df1 = pd.merge(part, partsupp, how='inner', on='P_PARTKEY')
df2 = pd.merge(df1, supplier, how='inner', on='S_SUPPKEY')
df3 = pd.merge(df2, nation, how='inner', on='S_NATIONKEY')
df = pd.merge(df3, region, how='inner', on='R_REGIONKEY')

# Apply filters
df = df[
    (df.P_SIZE == 15) &
    (df.P_TYPE.str.contains('BRASS')) &
    (df.R_NAME == 'EUROPE') &
    (df.PS_SUPPLYCOST == df.PS_SUPPLYCOST.min())
]

# Select columns and sort
df = df[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']]
df = df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Write to CSV
df.to_csv('query_output.csv', index=False)
