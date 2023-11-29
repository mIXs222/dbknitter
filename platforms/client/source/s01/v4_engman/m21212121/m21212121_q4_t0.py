import pymongo
import pandas as pd
import direct_redis

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
mongo_collection = mongo_db['lineitem']
lineitem_df = pd.DataFrame(list(mongo_collection.find({
    "L_SHIPDATE": {"$gte": "1993-07-01", "$lt": "1993-10-01"},
    "L_COMMITDATE": {"$lt": "L_RECEIPTDATE"}
}, {
    "L_ORDERKEY": 1,
    "_id": 0
})))

# Redis connection
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
orders_df = pd.DataFrame(eval(r.get('orders')))
orders_df = orders_df.astype({"O_ORDERKEY": 'int64'})

# Merging datasets
merged_df = pd.merge(orders_df, lineitem_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Getting the desired count
order_priority_count_df = merged_df.groupby("O_ORDERPRIORITY")["O_ORDERKEY"].nunique().reset_index()
order_priority_count_df.columns = ['O_ORDERPRIORITY', 'ORDER_COUNT']
order_priority_count_df = order_priority_count_df.sort_values('O_ORDERPRIORITY')

# Writing output
order_priority_count_df.to_csv("query_output.csv", index=False)
