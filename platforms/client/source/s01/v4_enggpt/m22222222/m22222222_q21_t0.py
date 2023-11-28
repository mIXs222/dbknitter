import pandas as pd
from direct_redis import DirectRedis

# Connection to Redis database
redis_host = 'redis'
redis_port = 6379
redis_db = 0

redis_con = DirectRedis(host=redis_host, port=redis_port, db=redis_db)

# Reading tables as Pandas DataFrames
try:
    nation_df = pd.read_msgpack(redis_con.get('nation'))
    supplier_df = pd.read_msgpack(redis_con.get('supplier'))
    orders_df = pd.read_msgpack(redis_con.get('orders'))
    lineitem_df = pd.read_msgpack(redis_con.get('lineitem'))
except Exception as e:
    print(f"Error reading data from Redis: {e}")
    raise

# Merge DataFrames to retrieve necessary information
df = supplier_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
df = df.merge(lineitem_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
df = df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Filtering the DataFrame according to the specified selection criteria
df_filtered = df[(df['N_NAME'] == 'SAUDI ARABIA') &
                 (df['O_ORDERSTATUS'] == 'F') &
                 (df['L_RECEIPTDATE'] > df['L_COMMITDATE'])]

# Define the subqueries conditions
condition_exists_other_supplier = df_filtered.groupby('L_ORDERKEY').S_SUPPKEY.transform(lambda x: x.nunique() > 1)
condition_not_exists_receipt_after_commit = ~df_filtered.groupby('L_ORDERKEY').L_RECEIPTDATE.transform(lambda x: (x > df_filtered['L_COMMITDATE']).any())

# Applying subquery conditions to the main DataFrame
df_filtered = df_filtered[condition_exists_other_supplier & condition_not_exists_receipt_after_commit]

# Group by supplier and count waiting line items
result = (df_filtered.groupby('S_NAME')
          .size()
          .reset_index(name='NUMWAIT')
          .sort_values(['NUMWAIT', 'S_NAME'], ascending=[False, True]))

# Write the results to a CSV
result.to_csv('query_output.csv', index=False)
