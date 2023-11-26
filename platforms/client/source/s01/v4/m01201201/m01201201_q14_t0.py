import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
lineitem_collection = mongo_db["lineitem"]

# Retrieve lineitem data from MongoDB with condition
lineitem_query = {
    "L_SHIPDATE": {"$gte": datetime(1995, 9, 1), "$lt": datetime(1995, 10, 1)}
}
lineitem_projection = {
    "_id": 0, "L_PARTKEY": 1, "L_EXTENDEDPRICE": 1, "L_DISCOUNT": 1
}
lineitem_data = list(lineitem_collection.find(lineitem_query, lineitem_projection))

# Convert to DataFrame
df_lineitem = pd.DataFrame(lineitem_data)

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)
# Fetch part data as DataFrame from Redis
part_keys = redis_client.keys('part:*')
part_data = []
for key in part_keys:
    part_record = eval(redis_client.get(key))
    part_data.append(part_record)
df_part = pd.DataFrame(part_data)

# Implementing SQL logic in Pandas
merged_data = df_lineitem.merge(df_part, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_data['TOTAL_PRICE'] = merged_data['L_EXTENDEDPRICE'] * (1 - merged_data['L_DISCOUNT'])
promo_revenue = merged_data.loc[merged_data['P_TYPE'].str.startswith("PROMO"), 'TOTAL_PRICE'].sum()
total_revenue = merged_data['TOTAL_PRICE'].sum()
result = (100.00 * promo_revenue / total_revenue) if total_revenue else 0

# Creating DataFrame for output
output_df = pd.DataFrame([{"PROMO_REVENUE": result}])

# Writing output to CSV
output_df.to_csv("query_output.csv", index=False)
