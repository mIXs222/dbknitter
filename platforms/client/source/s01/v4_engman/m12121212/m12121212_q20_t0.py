import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv
import datetime

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]
nation_tb = mongodb["nation"]
part_tb = mongodb["part"]
partsupp_tb = mongodb["partsupp"]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get data from MongoDB and Redis
nation_df = pd.DataFrame(list(nation_tb.find()))
part_df = pd.DataFrame(list(part_tb.find()))
partsupp_df = pd.DataFrame(list(partsupp_tb.find()))
supplier_df = pd.read_json(redis_client.get('supplier').decode('utf-8'))
lineitem_df = pd.read_json(redis_client.get('lineitem').decode('utf-8'))

# Filter data
canada_nations = nation_df[nation_df['N_NAME'] == 'CANADA']['N_NATIONKEY'].tolist()
supplier_canada_df = supplier_df[supplier_df['S_NATIONKEY'].isin(canada_nations)]
forest_parts_df = part_df[part_df['P_NAME'].str.contains('forest')]
relevant_line_items = lineitem_df[(lineitem_df['L_SHIPDATE'] >= datetime.date(1994, 1, 1)) &
                                  (lineitem_df['L_SHIPDATE'] < datetime.date(1995, 1, 1)) &
                                  (lineitem_df['L_PARTKEY'].isin(forest_parts_df['P_PARTKEY']))]

# Merge to get the necessary information
merged_df = relevant_line_items.merge(supplier_canada_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
grouped_df = merged_df.groupby(by=['L_SUPPKEY', 'S_NAME'], as_index=False).agg({
    'L_QUANTITY': 'sum'
})

# Filter suppliers with excess parts using partsupp information
supplier_totals = partsupp_df[partsupp_df['PS_PARTKEY'].isin(forest_parts_df['P_PARTKEY'])] \
    .groupby(by='PS_SUPPKEY', as_index=False).agg({
        'PS_AVAILQTY': 'sum'
    })
excess_suppliers_df = pd.merge(grouped_df, supplier_totals, how='left', left_on='L_SUPPKEY', right_on='PS_SUPPKEY')
excess_suppliers_df['EXCESS'] = excess_suppliers_df['L_QUANTITY'] > excess_suppliers_df['PS_AVAILQTY'] * 0.5
excess_suppliers_df = excess_suppliers_df[excess_suppliers_df['EXCESS']]

# Select relevant columns and save to CSV
output_df = excess_suppliers_df[['S_NAME']]
output_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)
