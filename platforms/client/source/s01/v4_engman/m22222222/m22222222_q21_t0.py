import pandas as pd
from direct_redis import DirectRedis

def get_df_from_redis(redis_client, table_name):
    dataframe_json = redis_client.get(table_name)
    if dataframe_json:
        return pd.read_json(dataframe_json)

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0, decode_responses=True)

# Get dataframes from Redis
df_nation = get_df_from_redis(redis_client, 'nation')
df_supplier = get_df_from_redis(redis_client, 'supplier')
df_orders = get_df_from_redis(redis_client, 'orders')
df_lineitem = get_df_from_redis(redis_client, 'lineitem')

# Join and filter data
nation_target = df_nation[df_nation['N_NAME'] == 'SAUDI ARABIA']
supplier_nation = pd.merge(df_supplier, nation_target, left_on='S_NATIONKEY', right_on='N_NATIONKEY', how='inner')

# Multi-supplier orders
multi_supp_orders = df_lineitem[df_lineitem['L_ORDERKEY'].duplicated(keep=False)]

# Get orders with status 'F' where they were the only supplier who failed to meet the commit date
failed_orders = multi_supp_orders[
    (multi_supp_orders['L_RETURNFLAG'] == 'F') &
    (multi_supp_orders['L_COMMITDATE'] < multi_supp_orders['L_RECEIPTDATE'])
]

# Count the number of waits per supplier
supplier_waits = failed_orders.groupby('L_SUPPKEY').size().reset_index(name='NUMWAIT')

# Merge the results with suppliers
result = pd.merge(supplier_waits, supplier_nation, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Select and sort the required output
output_df = result[['NUMWAIT', 'S_NAME']].sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write to CSV
output_df.to_csv('query_output.csv', index=False)
