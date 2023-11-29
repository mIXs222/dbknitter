# query.py
import pandas as pd
import csv
from direct_redis import DirectRedis

def get_data_from_redis(table_name):
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    data = redis_client.get(table_name)
    df = pd.read_json(data)
    return df

# Loading the data from Redis
nation_df = get_data_from_redis('nation')
region_df = get_data_from_redis('region')
part_df = get_data_from_redis('part')
supplier_df = get_data_from_redis('supplier')
partsupp_df = get_data_from_redis('partsupp')

# Filtering and joining the data
europe_nations = region_df.query("R_NAME == 'EUROPE'") \
    .merge(nation_df, left_on='R_REGIONKEY', right_on='N_REGIONKEY')

brass_parts = part_df.query("P_TYPE == 'BRASS' and P_SIZE == 15")

qualified_parts = brass_parts.merge(partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY') \
    .merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY') \
    .merge(europe_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

qualified_parts['MIN_PS_SUPPLYCOST'] = qualified_parts.groupby('P_PARTKEY')['PS_SUPPLYCOST'].transform('min')
min_cost_parts = qualified_parts.query("PS_SUPPLYCOST == MIN_PS_SUPPLYCOST")

# Sorting and selecting columns
result = min_cost_parts.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True]) \
    [['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']]

# Saving the result to a CSV file
result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
