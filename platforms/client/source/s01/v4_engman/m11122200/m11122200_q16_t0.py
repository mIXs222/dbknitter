import pymongo
import pandas as pd
import direct_redis
import csv

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_collection = mongo_db['part']

# Fetch the data from MongoDB and load into Pandas DataFrame
mongo_query = {
    'P_SIZE': {'$in': [49, 14, 23, 45, 19, 3, 36, 9]},
    'P_BRAND': {'$ne': 'Brand#45'},
    'P_TYPE': {'$not': {'$regex': 'MEDIUM POLISHED.*'}},
}
mongo_fields = {
    '_id': False,
    'P_PARTKEY': True
}

parts_df = pd.DataFrame(list(mongo_collection.find(mongo_query, mongo_fields)))

# Connect to Redis
redis_host = 'redis'
redis_port = 6379
redis_db = direct_redis.DirectRedis(host=redis_host, port=redis_port)

# Fetch data from Redis and load into Pandas DataFrame
supplier_df = pd.DataFrame(redis_db.get('supplier'))
partsupp_df = pd.DataFrame(redis_db.get('partsupp'))

# Filter the partsupp data to exclude supplier with complaints (assumed field)
complaints_supp_keys = supplier_df[supplier_df['S_COMMENT'].str.contains('Customer Complaints')]['S_SUPPKEY'].tolist()
partsupp_df = partsupp_df[~partsupp_df['PS_SUPPKEY'].isin(complaints_supp_keys)]

# Merge datasets to filter valid suppliers for valid parts
valid_supplier_parts = partsupp_df.merge(parts_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')
count_valid_suppliers = valid_supplier_parts.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg({'PS_SUPPKEY': 'nunique'}).reset_index()

# Sort the result as per the query
sorted_suppliers = count_valid_suppliers.sort_values(by=['PS_SUPPKEY', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write results to CSV file
sorted_suppliers.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
