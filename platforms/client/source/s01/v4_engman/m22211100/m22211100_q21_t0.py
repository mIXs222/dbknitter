import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Function to get data from MySQL
def get_mysql_data():
    conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    orders_query = "SELECT * FROM orders"
    lineitem_query = "SELECT * FROM lineitem"
    orders_df = pd.read_sql(orders_query, conn)
    lineitem_df = pd.read_sql(lineitem_query, conn)
    conn.close()
    return orders_df, lineitem_df

# Function to get data from MongoDB
def get_mongodb_data():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    supplier_coll = db['supplier']
    supplier_df = pd.DataFrame(list(supplier_coll.find()))
    client.close()
    return supplier_df

# Function to get data from Redis
def get_redis_data():
    client = DirectRedis(host='redis', port=6379, db=0)
    nation_df = pd.DataFrame(eval(client.get('nation')))
    client.close()
    return nation_df

# Get data from all sources
orders_df, lineitem_df = get_mysql_data()
supplier_df = get_mongodb_data()
nation_df = get_redis_data()

# Processing the data
# Filter SAUDI ARABIA nation and get nation key.
saudi_arabia_key = nation_df.loc[nation_df['N_NAME'] == 'SAUDI ARABIA', 'N_NATIONKEY'].values[0]

# Filter suppliers by nation key
suppliers_saudi = supplier_df[supplier_df['S_NATIONKEY'] == saudi_arabia_key]

# Identify multi-supplier orders by counting distinct suppliers
lineitem_multi_supplier = lineitem_df.groupby('L_ORDERKEY').filter(lambda x: x['L_SUPPKEY'].nunique() > 1)

# Find orders where there was a failure ('F') in commit date
late_orders = lineitem_multi_supplier[(lineitem_multi_supplier['L_RETURNFLAG'] == 'F') & 
                                      (lineitem_multi_supplier['L_COMMITDATE'] < lineitem_multi_supplier['L_RECEIPTDATE'])]

# Find the orders where the supplier was the only one being late
late_order_keys = late_orders['L_ORDERKEY'].unique()
exclusive_late_orders = lineitem_multi_supplier.groupby('L_ORDERKEY').filter(lambda x: set(x.loc[x['L_RETURNFLAG'] == 'F', 'L_SUPPKEY']) <= set(late_orders['L_SUPPKEY']))

# Count of await lineitems
await_counts = exclusive_late_orders.groupby('L_SUPPKEY').size().reset_index(name='NUMWAIT')
await_counts.columns = ['S_SUPPKEY', 'NUMWAIT']

# Merge with supplier names
result_df = await_counts.merge(suppliers_saudi, left_on='S_SUPPKEY', right_on='S_SUPPKEY')
result_df = result_df[['NUMWAIT', 'S_NAME']]
result_df = result_df.sort_values(['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
