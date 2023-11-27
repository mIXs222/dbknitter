import pymongo
import direct_redis
import pandas as pd

# Connection to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
mongodb = client['tpch']

# Fetching data from MongoDB
orders_df = pd.DataFrame(list(mongodb.orders.find(
    {'O_ORDERSTATUS': 'F'},
    {'O_ORDERKEY': 1, 'O_CUSTKEY': 1}
)))
lineitem_df = pd.DataFrame(list(mongodb.lineitem.find(
    {},
    {'L_ORDERKEY': 1, 'L_SUPPKEY': 1, 'L_COMMITDATE': 1, 'L_RECEIPTDATE': 1}
)))

# Connection to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379)
nation = pd.read_json(redis_conn.get('nation'))  # Assuming the data is stored as JSON string
supplier = pd.read_json(redis_conn.get('supplier'))  # Assuming the data is stored as JSON string

# Filter for SAUDI ARABIA in the nation table
nation_saudi = nation[nation['N_NAME'] == 'SAUDI ARABIA']

# Merging tables
suppliers_nation = supplier.merge(nation_saudi, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df = suppliers_nation.merge(lineitem_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
final_df = merged_df.merge(orders_df, on='O_ORDERKEY')

# Performing the query's logic
final_df['LATE'] = final_df['L_COMMITDATE'] < final_df['L_RECEIPTDATE']
late_orders = final_df.groupby('L_ORDERKEY').filter(lambda x: (x['LATE'] == True).any())
late_orders = late_orders.groupby('L_ORDERKEY').filter(lambda x: (x['LATE'] == False).all())

# Selecting the appropriate columns for output
output_df = late_orders[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']]
output_df = output_df.drop_duplicates()

# Writing to CSV
output_df.to_csv('query_output.csv', index=False)
