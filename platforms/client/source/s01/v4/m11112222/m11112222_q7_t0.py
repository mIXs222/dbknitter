import pymongo
import pandas as pd
import direct_redis
from datetime import datetime
from itertools import product

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
db = client['tpch']

# Fetch nation and supplier data from MongoDB
nation_data = list(db.nation.find({}, {'_id': 0}))
supplier_data = list(db.supplier.find({}, {'_id': 0}))

# Convert to Pandas DataFrame
nation_df = pd.DataFrame(nation_data)
supplier_df = pd.DataFrame(supplier_data)

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get Redis table data and convert to Pandas DataFrame
customer_df = pd.read_json(r.get('customer'))
orders_df = pd.read_json(r.get('orders'))
lineitem_df = pd.read_json(r.get('lineitem'))

# Filter nation data for 'INDIA' and 'JAPAN'
nations = nation_df[(nation_df['N_NAME'] == 'INDIA') | (nation_df['N_NAME'] == 'JAPAN')]

# Merge dataframes to apply conditions
merged_df = (
    supplier_df.merge(nations.rename(columns={'N_NATIONKEY': 'S_NATIONKEY', 'N_NAME': 'SUPP_NATION'}), on='S_NATIONKEY')
    .merge(lineitem_df, on='S_SUPPKEY')
    .merge(orders_df, on='O_ORDERKEY')
    .merge(customer_df, on='C_CUSTKEY')
    .merge(nations.rename(columns={'N_NATIONKEY': 'C_NATIONKEY', 'N_NAME': 'CUST_NATION'}), on='C_NATIONKEY')
)

# Filter by date and nation conditions
filtered_df = merged_df[
    ((merged_df['SUPP_NATION'] == 'JAPAN') & (merged_df['CUST_NATION'] == 'INDIA')) |
    ((merged_df['SUPP_NATION'] == 'INDIA') & (merged_df['CUST_NATION'] == 'JAPAN')) &
    merged_df['L_SHIPDATE'].between('1995-01-01', '1996-12-31')
]

# Calculate VOLUME and extract year from L_SHIPDATE
filtered_df['VOLUME'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])
filtered_df['L_YEAR'] = filtered_df['L_SHIPDATE'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").year)

# Group by SUPP_NATION, CUST_NATION, and L_YEAR
result = filtered_df.groupby(['SUPP_NATION', 'CUST_NATION', 'L_YEAR']).agg(REVENUE=('VOLUME', 'sum')).reset_index()

# Sort by SUPP_NATION, CUST_NATION, L_YEAR
result = result.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Write to file
result.to_csv('query_output.csv', index=False)
