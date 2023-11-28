import pymongo
import pandas as pd
import direct_redis
import re

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
db = client["tpch"]

# MongoDB Queries
nation_df = pd.DataFrame(list(db.nation.find({}, {'_id': 0})))
part_df = pd.DataFrame(list(db.part.find({"P_NAME": {"$regex": re.compile(".*dim.*", re.IGNORECASE)}}, {'_id': 0})))
supplier_df = pd.DataFrame(list(db.supplier.find({}, {'_id': 0})))

# Close MongoDB connection
client.close()

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Redis Queries
partsupp_df = pd.read_json(r.get("partsupp"))
orders_df = pd.read_json(r.get("orders"))
lineitem_df = pd.read_json(r.get("lineitem"))

# Data Processing
lineitem_df = lineitem_df.merge(part_df[['P_PARTKEY']], left_on='L_PARTKEY', right_on='P_PARTKEY', how='inner')
lineitem_df = lineitem_df.merge(partsupp_df[['PS_PARTKEY', 'PS_SUPPKEY', 'PS_SUPPLYCOST']], left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'], how='inner')
lineitem_df = lineitem_df.merge(orders_df[['O_ORDERKEY', 'O_ORDERDATE']], left_on='L_ORDERKEY', right_on='O_ORDERKEY', how='inner')
lineitem_df['profit'] = (lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])) - (lineitem_df['PS_SUPPLYCOST'] * lineitem_df['L_QUANTITY'])
lineitem_df['year'] = pd.to_datetime(lineitem_df['O_ORDERDATE']).dt.year
profit_nation_year_df = lineitem_df.merge(supplier_df[['S_SUPPKEY', 'S_NATIONKEY']], left_on='L_SUPPKEY', right_on='S_SUPPKEY', how='inner')
profit_nation_year_df = profit_nation_year_df.merge(nation_df[['N_NATIONKEY', 'N_NAME']], left_on='S_NATIONKEY', right_on='N_NATIONKEY', how='inner')
profit_nation_year_df = profit_nation_year_df.groupby(['N_NAME', 'year'])['profit'].sum().reset_index()

# Final sorting according to requirements
profit_nation_year_df.sort_values(by=['N_NAME', 'year'], ascending=[True, False], inplace=True)

# Write to CSV
profit_nation_year_df.to_csv('query_output.csv', index=False)
