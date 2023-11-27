import pandas as pd
import direct_redis

# Connection info for Redis
REDIS_DATABASE = 0
REDIS_PORT = 6379
REDIS_HOSTNAME = "redis"

# Connect to Redis
dr = direct_redis.DirectRedis(host=REDIS_HOSTNAME, port=REDIS_PORT, db=REDIS_DATABASE)

# Read tables from Redis as Pandas DataFrames
part_df = pd.DataFrame(eval(dr.get('part')))  # Assuming data is stored in a string that can be evaluated to a list of dicts
supplier_df = pd.DataFrame(eval(dr.get('supplier')))
partsupp_df = pd.DataFrame(eval(dr.get('partsupp')))

# Sizes required
sizes = [49, 14, 23, 45, 19, 3, 36, 9]

# Perform query operation
query_result = (
    part_df[(part_df['P_SIZE'].isin(sizes)) & 
            (part_df['P_BRAND'] != 'Brand#45') & 
            (part_df['P_TYPE'].str.contains('MEDIUM POLISHED') == False)]
    .merge(partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')
    .merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
    .groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])
    .agg({'S_SUPPKEY': pd.Series.nunique})
    .reset_index()
    .rename(columns={'S_SUPPKEY': 'SUPPLIER_COUNT'})
    .sort_values(by=['SUPPLIER_COUNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])
)

# Save the result to a CSV file
query_result.to_csv('query_output.csv', index=False)
