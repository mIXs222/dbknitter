import pymongo
import redis
from direct_redis import DirectRedis
import pandas as pd

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
nation_col = mongo_db["nation"]
part_col = mongo_db["part"]
partsupp_col = mongo_db["partsupp"]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load data from Redis
region_df = pd.read_json(redis_client.get('region'), orient="records")
supplier_df = pd.read_json(redis_client.get('supplier'), orient="records")

# Convert Redis data frames to use the correct column names
region_df.columns = ['R_REGIONKEY', 'R_NAME', 'R_COMMENT']
supplier_df.columns = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']

# Query MongoDB
nation_docs = list(nation_col.find())
part_docs = list(part_col.find({"P_TYPE": "BRASS", "P_SIZE": 15}))
partsupp_docs = list(partsupp_col.find())

# Convert MongoDB docs to pandas dataframes
nation_df = pd.DataFrame(nation_docs)
part_df = pd.DataFrame(part_docs)
partsupp_df = pd.DataFrame(partsupp_docs)

# Merge the dataframes from different databases
merged_df = part_df.merge(partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')
merged_df = merged_df.merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df = merged_df.merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Filter to get only suppliers in the EUROPE region
europe_suppliers = merged_df[merged_df['R_NAME'] == 'EUROPE']

# Find the minimum cost by part key
min_cost_df = europe_suppliers.groupby('P_PARTKEY').PS_SUPPLYCOST.min().reset_index()
min_cost_df = min_cost_df.rename(columns={"PS_SUPPLYCOST": "MIN_SUPPLYCOST"})

# Merge minimum cost back to the original dataframe to filter the suppliers
final_df = europe_suppliers.merge(min_cost_df, left_on=['P_PARTKEY', 'PS_SUPPLYCOST'], right_on=['P_PARTKEY', 'MIN_SUPPLYCOST'])

# Order by account balance in descending order, nation name, supplier name, part key in ascending order
final_df = final_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Select the required fields
output_df = final_df[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']]

# Write to CSV file
output_df.to_csv('query_output.csv', index=False)
