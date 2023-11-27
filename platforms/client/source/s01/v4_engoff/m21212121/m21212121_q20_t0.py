import pymongo
import direct_redis
import pandas as pd
from datetime import datetime

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
mongodb = client["tpch"]

# Connect to Redis
redis = direct_redis.DirectRedis(host="redis", port=6379, db=0)

# Load data from mongodb
lineitem_columns = ["L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE",
                    "L_DISCOUNT", "L_TAX", "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE", 
                    "L_RECEIPTDATE", "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT"]
supplier_columns = ["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT"]

lineitem_df = pd.DataFrame(list(mongodb.lineitem.find()), columns=lineitem_columns)
supplier_df = pd.DataFrame(list(mongodb.supplier.find()), columns=supplier_columns)

# Load data from redis
nation_df = pd.read_json(redis.get('nation'), orient='index')
part_df = pd.read_json(redis.get('part'), orient='index')
partsupp_df = pd.read_json(redis.get('partsupp'), orient='index')

# Convert string dates to datetime objects
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter lineitems for the date range
date_start = datetime(1994, 1, 1)
date_end = datetime(1995, 1, 1)
filtered_lineitems = lineitem_df[(lineitem_df['L_SHIPDATE'] >= date_start) & (lineitem_df['L_SHIPDATE'] < date_end)]

# Filter parts that share a certain naming convention (e.g., contain 'forest')
filtered_parts = part_df[part_df['P_NAME'].str.contains('forest', case=False)]

# Merge dataframes
merged_df = filtered_lineitems.merge(filtered_parts, left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter for Canada
canada_df = merged_df[merged_df['N_NAME'] == 'CANADA']

# Group by supplier and sum quantities
supplier_group = canada_df.groupby('S_SUPPKEY').agg({'L_QUANTITY': 'sum'})

# Identify suppliers with more than 50% of parts shipped
potential_promotion_suppliers = supplier_group[supplier_group['L_QUANTITY'] > (supplier_group['L_QUANTITY'].sum()*0.5)]

# Output results to CSV
final_suppliers = supplier_df[supplier_df['S_SUPPKEY'].isin(potential_promotion_suppliers.index)]
final_suppliers.to_csv('query_output.csv', index=False)
