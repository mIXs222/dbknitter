# Import necessary libraries
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client['tpch']
mongo_nation = mongo_db['nation'].find()
mongo_orders = mongo_db['orders'].find({
    'O_ORDERDATE': {'$gte': '1993-10-01', '$lte': '1993-12-31'}
})
df_nation = pd.DataFrame(list(mongo_nation))
df_orders = pd.DataFrame(list(mongo_orders))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
df_customer = pd.read_json(redis_client.get('customer'))
df_lineitem = pd.read_json(redis_client.get('lineitem'))

# Merge Redis and MongoDB data
df_lineitem = df_lineitem[df_lineitem['L_RETURNFLAG'] == 'R']
df_orders_lineitem = pd.merge(df_orders, df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
df_orders_lineitem['REVENUE'] = df_orders_lineitem['L_EXTENDEDPRICE'] * (1 - df_orders_lineitem['L_DISCOUNT'])
df_cust_orders = pd.merge(df_customer, df_orders_lineitem, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df_final = pd.merge(df_cust_orders, df_nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Select required columns
df_final = df_final[['C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']]

# Group by the required fields
df_final_grouped = df_final.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_PHONE', 'N_NAME', 'C_ADDRESS', 'C_COMMENT'], as_index=False).sum()

# Sort the results
df_final_sorted = df_final_grouped.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False])

# Write results to CSV
df_final_sorted.to_csv('query_output.csv', index=False)
