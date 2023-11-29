import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MongoDB connection
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# Redis connection
redis_db = DirectRedis(host='redis', port=6379, db=0)

# Load data from MongoDB
customer_df = pd.DataFrame(list(mongo_db.customer.find()))
orders_df = pd.DataFrame(list(mongo_db.orders.find()))
lineitem_df = pd.DataFrame(list(mongo_db.lineitem.find()))

# Load data from Redis
nation_df = pd.read_msgpack(redis_db.get('nation'))
supplier_df = pd.read_msgpack(redis_db.get('supplier'))

# Filter nations for INDIA and JAPAN
nation_keys_india_japan = set(nation_df[nation_df['N_NAME'].isin(['INDIA', 'JAPAN'])]['N_NATIONKEY'])

# Filter suppliers for INDIA and JAPAN
supplier_df = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_keys_india_japan)]

# Filter customers for INDIA and JAPAN
customer_df = customer_df[customer_df['C_NATIONKEY'].isin(nation_keys_india_japan)]

# Merge the datasets
df_merged = (
    lineitem_df.merge(
        orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY'
    ).merge(
        customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY'
    ).merge(
        supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY'
    )
)

# Further filter the data for the years 1995 and 1996
df_merged['L_YEAR'] = pd.to_datetime(df_merged['L_SHIPDATE']).dt.year
df_merged = df_merged[(df_merged['L_YEAR'] >= 1995) & (df_merged['L_YEAR'] <= 1996)]

# Calculate revenue
df_merged['REVENUE'] = df_merged['L_EXTENDEDPRICE'] * (1 - df_merged['L_DISCOUNT'])

# Filter for suppliers in INDIA and customers in JAPAN, and the opposite
df_result = df_merged[
    ((df_merged['S_NATIONKEY'] == nation_df.loc[nation_df['N_NAME'] == 'INDIA'].iloc[0]['N_NATIONKEY']) &
     (df_merged['C_NATIONKEY'] == nation_df.loc[nation_df['N_NAME'] == 'JAPAN'].iloc[0]['N_NATIONKEY'])) |
    ((df_merged['S_NATIONKEY'] == nation_df.loc[nation_df['N_NAME'] == 'JAPAN'].iloc[0]['N_NATIONKEY']) &
     (df_merged['C_NATIONKEY'] == nation_df.loc[nation_df['N_NAME'] == 'INDIA'].iloc[0]['N_NATIONKEY']))
]

# Select and rename columns for final output
df_result_final = df_result[['C_NATIONKEY', 'L_YEAR', 'REVENUE', 'S_NATIONKEY']]
df_result_final.columns = ['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION']
df_result_final['CUST_NATION'].replace(nation_df.set_index('N_NATIONKEY')['N_NAME'], inplace=True)
df_result_final['SUPP_NATION'].replace(nation_df.set_index('N_NATIONKEY')['N_NAME'], inplace=True)

# Sort the result
df_result_final.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'], inplace=True)

# Write output to CSV
df_result_final.to_csv('query_output.csv', index=False)
