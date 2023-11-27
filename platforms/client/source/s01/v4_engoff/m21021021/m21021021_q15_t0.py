import pymongo
import pandas as pd
from datetime import datetime

# Connection to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
lineitem_collection = mongo_db["lineitem"]

# Query for MongoDB to get lineitem data between the specified dates
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)
mongo_query = {
    "L_SHIPDATE": {
        "$gte": start_date,
        "$lt": end_date
    }
}
lineitem_df = pd.DataFrame(list(lineitem_collection.find(mongo_query, {"_id": 0})))

# Connection to Redis
import direct_redis
redis_client = direct_redis.DirectRedis(host="redis", port="6379", db=0)

# Query for Redis to get supplier data
supplier_df = pd.read_pickle(redis_client.get("supplier"))

# Combine data from MongoDB and Redis
combined_df = pd.merge(lineitem_df, supplier_df, how='left', left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Calculate revenue
combined_df['REVENUE'] = combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])

# Find the top supplier(s)
top_revenue = combined_df.groupby(['S_SUPPKEY', 'S_NAME'])['REVENUE'].sum().reset_index()
top_revenue = top_revenue.sort_values(by=['REVENUE', 'S_SUPPKEY'], ascending=[False, True])
max_revenue = top_revenue['REVENUE'].max()
top_suppliers = top_revenue[top_revenue['REVENUE'] == max_revenue]

# Write result to CSV
top_suppliers.to_csv('query_output.csv', index=False)
