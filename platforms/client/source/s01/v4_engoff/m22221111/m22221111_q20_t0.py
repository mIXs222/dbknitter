import pymongo
import pandas as pd
from bson import ObjectId
from direct_redis import DirectRedis

# Establish connection to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
partsupp_collection = mongo_db["partsupp"]
lineitem_collection = mongo_db["lineitem"]

# Query lineitem collection for relevant documents
start_date = ObjectId.from_datetime(pd.Timestamp('1994-01-01'))
end_date = ObjectId.from_datetime(pd.Timestamp('1995-01-01'))
lineitem_query = {
    'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}
}
lineitems = lineitem_collection.find(lineitem_query)
lineitems_df = pd.DataFrame(list(lineitems))

# Establish connection to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get data from Redis
nation_df = pd.DataFrame(eval(redis_client.get('nation')))
part_df = pd.DataFrame(eval(redis_client.get('part')))
supplier_df = pd.DataFrame(eval(redis_client.get('supplier')))

# Filtering for 'forest' parts and nation CANADA
forest_parts_df = part_df[part_df['P_NAME'].str.contains('forest')]
canada_nationkey = nation_df.loc[nation_df['N_NAME'] == 'CANADA', 'N_NATIONKEY'].iloc[0]

# Filter suppliers from CANADA
canada_supplier_df = supplier_df[supplier_df['S_NATIONKEY'] == canada_nationkey]

# Join Redis and MongoDB data
merged_df = lineitems_df.merge(partsupp_collection, how='inner', left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
merged_df = merged_df.merge(forest_parts_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(canada_supplier_df, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Filtering suppliers with excess of forest part
supplier_part_count = merged_df.groupby('S_SUPPKEY').size().reset_index(name='count')
supplier_excess_parts = supplier_part_count[supplier_part_count['count'] > (supplier_part_count['count'].sum() * 0.5)]

# Write output to CSV file
supplier_excess_parts.to_csv('query_output.csv', index=False)
