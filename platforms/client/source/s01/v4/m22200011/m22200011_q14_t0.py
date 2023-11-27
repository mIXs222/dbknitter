import pymongo
import direct_redis
import pandas as pd
from datetime import datetime

# Connecting to mongodb
mongodb_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb_db = mongodb_client["tpch"]
lineitem_collection = mongodb_db.lineitem

# Prepare the query for mongodb
query = {
    "L_SHIPDATE": {
        "$gte": datetime.strptime('1995-09-01', '%Y-%m-%d'),
        "$lt": datetime.strptime('1995-10-01', '%Y-%m-%d')
    }
}
projection = {
    "_id": 0,
    "L_PARTKEY": 1,
    "L_EXTENDEDPRICE": 1,
    "L_DISCOUNT": 1
}
lineitem_df = pd.DataFrame(list(lineitem_collection.find(query, projection)))

# Connecting to redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get 'part' table from Redis and load into a pandas DataFrame
part_bytes = r.get('part')
part_df = pd.read_msgpack(part_bytes)

# Merge the dataframes
merged_df = pd.merge(
    lineitem_df,
    part_df,
    left_on='L_PARTKEY',
    right_on='P_PARTKEY'
)

# Compute the PROMO_REVENUE
promo_revenue = (
    100.00 *
    merged_df[merged_df["P_TYPE"].str.startswith("PROMO")]
    .apply(lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']), axis=1)
    .sum() /
    merged_df
    .apply(lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']), axis=1)
    .sum()
)

# Save the result to a CSV file
result_df = pd.DataFrame([{'PROMO_REVENUE': promo_revenue}])
result_df.to_csv('query_output.csv', index=False)
