# execute_query.py
import pymongo
import pandas as pd
import direct_redis

# Setting up the MongoDB connection
mongodb_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb_db = mongodb_client["tpch"]
# Loading collections from MongoDB
nation_col = mongodb_db["nation"]
supplier_col = mongodb_db["supplier"]
orders_col = mongodb_db["orders"]

# Convert MongoDB collections to DataFrames
query_nation = {"N_NAME": "SAUDI ARABIA"}
nation_df = pd.DataFrame(list(nation_col.find(query_nation)))
supplier_df = pd.DataFrame(list(supplier_col.find()))
orders_df = pd.DataFrame(list(orders_col.find()))

# Filter orders with status 'F'
orders_df_f = orders_df[orders_df["O_ORDERSTATUS"] == "F"]

# Setting up the Redis connection
redis_client = direct_redis.DirectRedis(host="redis", port=6379, db=0)
# Load 'lineitem' table from Redis
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Merge DataFrames to get the result
result_df = supplier_df.merge(
    lineitem_df,
    left_on='S_SUPPKEY', 
    right_on='L_SUPPKEY'
).merge(
    orders_df_f,
    left_on='L_ORDERKEY', 
    right_on='O_ORDERKEY'
).merge(
    nation_df,
    left_on='S_NATIONKEY', 
    right_on='N_NATIONKEY'
)

# Find multi-supplier orders
multi_supplier_orders = result_df.groupby('L_ORDERKEY').filter(lambda x: x['S_SUPPKEY'].nunique() > 1)

# Identify failed deliveries
failed_deliveries = multi_supplier_orders[
    (multi_supplier_orders['L_COMMITDATE'] < multi_supplier_orders['L_RECEIPTDATE']) & 
    (multi_supplier_orders['L_RETURNFLAG'] == 'R') # assuming 'F' is a typo and 'R' stands for return
]

# Count the number of waiting line items per supplier
numwait_df = failed_deliveries.groupby('S_NAME').size().reset_index(name='NUMWAIT')

# Sort the result
sorted_df = numwait_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Save the result to a CSV file
sorted_df.to_csv('query_output.csv', index=False)
