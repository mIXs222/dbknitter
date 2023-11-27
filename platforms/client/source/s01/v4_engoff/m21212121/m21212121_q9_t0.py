import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# MongoDB Connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# Get data from MongoDB
supplier_df = pd.DataFrame(list(mongo_db["supplier"].find()))
lineitem_df = pd.DataFrame(list(mongo_db["lineitem"].find()))

# Add "year" column to lineitem_df
lineitem_df["year"] = lineitem_df["L_SHIPDATE"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").year)

# Redis Connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get data from Redis
nation_df = pd.read_json(redis_client.get('nation'), orient="records")
partsupp_df = pd.read_json(redis_client.get('partsupp'), orient="records")

# Join operation
joined_df = lineitem_df \
    .merge(supplier_df, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY') \
    .merge(nation_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY') \
    .merge(partsupp_df, how='inner', left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])

# Filter specific dim from P_NAME in part table
# Since the part table is not present in the provided schema, we can't filter based on part names

# Calculate profit
joined_df['profit'] = (joined_df['L_EXTENDEDPRICE'] * (1 - joined_df['L_DISCOUNT'])) - \
                       (joined_df['PS_SUPPLYCOST'] * joined_df['L_QUANTITY'])

# Group by nation and year to sum profit
result_df = joined_df.groupby(['N_NAME', 'year'])['profit'].sum().reset_index()

# Sort results
result_df = result_df.sort_values(by=['N_NAME', 'year'], ascending=[True, False])

# Output to CSV
result_df.to_csv('query_output.csv', index=False)
