import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# Establish connection with MongoDB
client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = client['tpch']

# Load the data from MongoDB
nation = pd.DataFrame(list(mongodb_db.nation.find()))
supplier = pd.DataFrame(list(mongodb_db.supplier.find()))
orders = pd.DataFrame(list(mongodb_db.orders.find({'O_ORDERSTATUS': 'F'})))

# Establish connection with Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
# Load the lineitem table into a DataFrame
lineitem_str = redis_conn.get('lineitem')
lineitem = pd.read_json(lineitem_str)

# Preprocess and filter data
nation = nation[nation['N_NAME'] == 'SAUDI ARABIA']
supplier = supplier[supplier['S_NATIONKEY'].isin(nation['N_NATIONKEY'])]

# Merge dataframes
result = (
    lineitem.merge(supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
)

# Filter based on L_COMMITDATE and L_RECEIPTDATE
result = result[result['L_RECEIPTDATE'] > result['L_COMMITDATE']]

# Aggregate and sort result
final_result = (
    result.groupby('S_NAME')
    .agg(NUMWAIT=('L_ORDERKEY', 'count'))
    .reset_index()
    .sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])
)

# Write result to CSV
final_result.to_csv('query_output.csv', index=False)
