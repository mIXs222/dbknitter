import pandas as pd
import redis

# Estabilishing Redis Connection
r = redis.Redis(
    host='redis',
    port=6379, 
    db=0)

# Fetching Redis Data into DataFrames
partsupp_df = pd.DataFrame.from_dict(r.get('partsupp'), orient='index', columns=['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'])
supplier_df = pd.DataFrame.from_dict(r.get('supplier'), orient='index', columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])
nation_df = pd.DataFrame.from_dict(r.get('nation'), orient='index', columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

# Type Conversion
partsupp_df = partsupp_df.astype({'PS_PARTKEY': 'int64', 'PS_SUPPKEY': 'int64', 'PS_AVAILQTY': 'int64', 'PS_SUPPLYCOST': 'float64'})
supplier_df = supplier_df.astype({'S_SUPPKEY': 'int64', 'S_NATIONKEY': 'int64'})
nation_df = nation_df.astype({'N_NATIONKEY': 'int64'})

# Merging DataFrames
df = pd.merge(pd.merge(partsupp_df,supplier_df,on='S_SUPPKEY'),nation_df,on='N_NATIONKEY')
df = df[df['N_NAME'] == 'GERMANY']

# Compute value
df['VALUE'] = df['PS_SUPPLYCOST']*df['PS_AVAILQTY']
threshold_value = df['VALUE'].sum() * 0.0001000000

# Apply conditions and arrange in descending order
output_df = df[df['VALUE'] > threshold_value].sort_values(by='VALUE', ascending=False)

# Group by PartKey and compute total value for each part
grouped_df = output_df.groupby(['PS_PARTKEY'])['VALUE'].sum().reset_index()

# Export result to csv
grouped_df.to_csv('query_output.csv', index = False)
