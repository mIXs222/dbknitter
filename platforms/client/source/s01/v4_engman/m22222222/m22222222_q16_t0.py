# query.py
import pandas as pd
import direct_redis

# Connection information for Redis
hostname = 'redis'
port = 6379
database = 0

# Connecting to Redis
dr = direct_redis.DirectRedis(host=hostname, port=port, db=database)

# Reading Redis DataFrame
part = pd.DataFrame(eval(dr.get('part')))
supplier = pd.DataFrame(eval(dr.get('supplier')))
partsupp = pd.DataFrame(eval(dr.get('partsupp')))

# Query execution
filtered_parts = part[
    (~part['P_BRAND'].eq('Brand#45')) &
    (~part['P_TYPE'].str.contains('MEDIUM POLISHED')) &
    (part['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))
]

filtered_suppliers = supplier[~supplier['S_COMMENT'].str.contains('Customer.*Complaints')]

merged_data = pd.merge(
    filtered_parts,
    partsupp,
    how='inner',
    left_on='P_PARTKEY',
    right_on='PS_PARTKEY'
)

result = pd.merge(
    filtered_suppliers,
    merged_data,
    how='inner',
    left_on='S_SUPPKEY',
    right_on='PS_SUPPKEY'
)

# Group by the required fields
output = result \
    .groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']) \
    .agg({'S_SUPPKEY': pd.Series.nunique}) \
    .rename(columns={'S_SUPPKEY': 'SUPPLIER_COUNT'}) \
    .reset_index() \
    .sort_values(by=['SUPPLIER_COUNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Writing to CSV
output.to_csv('query_output.csv', index=False)
