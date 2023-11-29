import pymongo
import pandas as pd
import direct_redis
import csv

# MongoDB connection setup
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
supplier_collection = mongo_db["supplier"]

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetching data from Redis
part_df = pd.read_json(r.get('part'), orient='records')
partsupp_df = pd.read_json(r.get('partsupp'), orient='records')

# Query for MongoDB (supplier)
supplier_query = {
    'S_COMMENT': {'$not': {'$regex': '.*complaints.*Better Business Bureau.*', '$options': 'i'}}
}
supplier_df = pd.DataFrame(list(supplier_collection.find(supplier_query)))

# Merge partsupp and supplier dataframes
partsupp_supplier_df = partsupp_df.merge(supplier_df, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Filter part dataframe
part_df_filtered = part_df[
    (~part_df['P_BRAND'].eq('Brand#45')) &
    (~part_df['P_TYPE'].str.contains('MEDIUM POLISHED')) &
    (part_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))
]

# Merge filtered parts with partsupp_supplier dataframe
query_result_df = partsupp_supplier_df.merge(part_df_filtered, how='inner', left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Preparing output
output_df = query_result_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg({'S_SUPPKEY': 'count'}).reset_index()
output_df.rename(columns={'S_SUPPKEY': 'supplier_count'}, inplace=True)
output_df = output_df.sort_values(by=['supplier_count', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write result to CSV file
output_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
