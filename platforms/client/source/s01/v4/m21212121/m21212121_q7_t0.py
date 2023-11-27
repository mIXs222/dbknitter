from pymongo import MongoClient
import pandas as pd
from direct_redis import DirectRedis
import datetime

# Connect to MongoDB
mongo_client = MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]

# Load MongoDB tables
supplier_df = pd.DataFrame(list(mongo_db["supplier"].find()))
customer_df = pd.DataFrame(list(mongo_db["customer"].find()))
lineitem_df = pd.DataFrame(list(mongo_db["lineitem"].find()))

# Connect to Redis
redis_client = DirectRedis(host="redis", port=6379, db=0)

# Load Redis tables (convert bytes to string and load as DataFrame)
nation_df = pd.read_json(redis_client.get('nation').decode('utf-8'))
orders_df = pd.read_json(redis_client.get('orders').decode('utf-8'))

# Preprocess the data
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_df['L_YEAR'] = lineitem_df['L_SHIPDATE'].dt.year

# Filter time range for lineitem
lineitem_filtered = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= datetime.datetime(1995, 1, 1)) &
    (lineitem_df['L_SHIPDATE'] <= datetime.datetime(1996, 12, 31))
]

# Perform the join operation
merged_df = (
    supplier_df.merge(lineitem_filtered, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
    .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(nation_df.rename(columns={'N_NATIONKEY': 'S_NATIONKEY', 'N_NAME': 'SUPP_NATION'}), on='S_NATIONKEY')
    .merge(nation_df.rename(columns={'N_NATIONKEY': 'C_NATIONKEY', 'N_NAME': 'CUST_NATION'}), on='C_NATIONKEY')
)

# Filter for specific nations
filtered_merged_df = merged_df[
    ((merged_df['SUPP_NATION'] == 'JAPAN') & (merged_df['CUST_NATION'] == 'INDIA')) |
    ((merged_df['SUPP_NATION'] == 'INDIA') & (merged_df['CUST_NATION'] == 'JAPAN'))
]

# Calculate the volume and group by the required fields
result_df = filtered_merged_df.assign(VOLUME=filtered_merged_df['L_EXTENDEDPRICE'] * (1 - filtered_merged_df['L_DISCOUNT']))
result_df = (
    result_df[['SUPP_NATION', 'CUST_NATION', 'L_YEAR', 'VOLUME']]
    .groupby(['SUPP_NATION', 'CUST_NATION', 'L_YEAR'], as_index=False)
    .sum()
)

# Sort the result as required by the query
result_df = result_df.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Save to CSV
result_df.to_csv('query_output.csv', index=False)
