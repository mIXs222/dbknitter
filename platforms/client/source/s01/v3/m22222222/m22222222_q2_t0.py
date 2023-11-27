import pandas as pd
import direct_redis
from functools import reduce

# Open connections
redis_db = direct_redis.DirectRedis(db=0, host='redis', port=6379)

# Read data from Redis database
redis_parts = pd.DataFrame(redis_db.get('part'))
redis_supplier = pd.DataFrame(redis_db.get('supplier'))
redis_partsupp = pd.DataFrame(redis_db.get('partsupp'))
redis_nation = pd.DataFrame(redis_db.get('nation'))
redis_region = pd.DataFrame(redis_db.get('region'))

# Filter data according to the condition in SQL query
filtered_parts = redis_parts[(redis_parts["P_SIZE"] == 15) & (redis_parts["P_TYPE"].str.contains('BRASS'))]
filtered_region = redis_region[redis_region["R_NAME"] == 'EUROPE']

# Join data frames
dfs = [filtered_parts, redis_supplier, redis_partsupp, redis_nation, filtered_region]
df_final = reduce(lambda left,right: pd.merge(left,right,left_on=['P_PARTKEY', 'S_SUPPKEY', 'S_NATIONKEY', 'N_REGIONKEY'], 
                                              right_on=['PS_PARTKEY', 'S_SUPPKEY', 'N_NATIONKEY', 'R_REGIONKEY'],
                                              how='inner'), dfs)

# Select columns and sort values
selected_col_df = df_final[["S_ACCTBAL", "S_NAME", "N_NAME", "P_PARTKEY", "P_MFGR", "S_ADDRESS", 
                            "S_PHONE", "S_COMMENT"]]

sorted_df = selected_col_df.sort_values(by=["S_ACCTBAL", "N_NAME", "S_NAME", "P_PARTKEY"], 
                                        ascending=[False, True, True, True])

# Write the result into csv
sorted_df.to_csv('query_output.csv', index=False)
