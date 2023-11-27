import pymongo
import pandas as pd
from datetime import datetime
import direct_redis
import csv

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
customer_collection = mongo_db["customer"]
lineitem_collection = mongo_db["lineitem"]

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch nation data from Redis
nation_data = redis_client.get('nation')
nation_df = pd.read_json(nation_data)

# Fetch orders data from Redis
orders_data = redis_client.get('orders')
orders_df = pd.read_json(orders_data)

# Query MongoDB for required data
query_customer = list(customer_collection.find({}, {'_id': 0}))
query_lineitem = list(lineitem_collection.find({
    'L_SHIPDATE': {
        '$gte': datetime(1993, 10, 1),
        '$lt': datetime(1994, 1, 1)
    },
    'L_RETURNFLAG': 'R'
}, {'_id': 0}))

# Create DataFrames from MongoDB data
customer_df = pd.DataFrame(query_customer)
lineitem_df = pd.DataFrame(query_lineitem)

# Merge the DataFrames
merged_df = (customer_df
             .merge(orders_df, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
             .merge(lineitem_df, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
             .merge(nation_df, left_on="C_NATIONKEY", right_on="N_NATIONKEY"))

# Calculate lost revenue and filter the required date range
merged_df = merged_df.assign(
    LOST_REVENUE=merged_df.L_EXTENDEDPRICE * (1 - merged_df.L_DISCOUNT)
)

# Aggregate the results
result_df = merged_df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT']) \
    .agg(LOST_REVENUE=pd.NamedAgg(column="LOST_REVENUE", aggfunc="sum")) \
    .reset_index() \
    .sort_values(by=["LOST_REVENUE", "C_CUSTKEY", "C_NAME", "C_ACCTBAL"], ascending=[False, True, True, True])

# Write output to CSV
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
