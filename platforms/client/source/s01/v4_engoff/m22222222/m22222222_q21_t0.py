# suppliers_who_kept_orders_waiting.py
import pandas as pd
from direct_redis import DirectRedis

# Function to get data from Redis and return as a pandas DataFrame
def get_data_from_redis(redis_client, table_name):
    data = redis_client.get(table_name)
    return pd.read_json(data, orient='index')

# Establish a connection to the Redis instance
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from Redis
nation_df = get_data_from_redis(redis_client, 'nation')
supplier_df = get_data_from_redis(redis_client, 'supplier')
orders_df = get_data_from_redis(redis_client, 'orders')
lineitem_df = get_data_from_redis(redis_client, 'lineitem')

# Find the nation key for 'SAUDI ARABIA'
saudi_arabia_key = nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA']['N_NATIONKEY'].iloc[0]

# Find suppliers from 'SAUDI ARABIA'
saudi_suppliers = supplier_df[supplier_df['S_NATIONKEY'] == saudi_arabia_key]

# Identify orders with status 'F' and join with lineitem to find multi-supplier orders
multi_supplier_orders = (
    orders_df[orders_df['O_ORDERSTATUS'] == 'F']
    .merge(lineitem_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')
)

# Mark if supplier is late
multi_supplier_orders['is_late'] = multi_supplier_orders['L_COMMITDATE'] < multi_supplier_orders['L_RECEIPTDATE']

# Find orders with at least two distinct suppliers
multi_supplier_flag = (
    multi_supplier_orders.groupby('L_ORDERKEY')['S_SUPPKEY'].transform('nunique') > 1
)
multi_supplier_orders = multi_supplier_orders[multi_supplier_flag]

# Find orders where only our supplier is late
late_only_supplier = (
    multi_supplier_orders.groupby('L_ORDERKEY')['is_late'].transform(lambda x: (x.sum() == 1) & x.any())
)
multi_supplier_orders = multi_supplier_orders[late_only_supplier]

# Selecting the relevant suppliers
relevant_suppliers = (
    multi_supplier_orders[multi_supplier_orders['S_SUPPKEY'].isin(saudi_suppliers['S_SUPPKEY'])]
    [['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']]
    .drop_duplicates()
)

# Writing the result to CSV
relevant_suppliers.to_csv('query_output.csv', index=False)
