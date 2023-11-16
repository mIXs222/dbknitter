import pymongo
from pymongo import MongoClient
import pandas as pd

# Connect to Mongo
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Retrieve collections
nation = db['nation']
region = db['region']
part = db['part']
supplier = db['supplier']
partsupp = db['partsupp']

# Transform data to pandas DataFrame
nation_df = pd.DataFrame(list(nation.find()))
region_df = pd.DataFrame(list(region.find()))
part_df = pd.DataFrame(list(part.find()))
supplier_df = pd.DataFrame(list(supplier.find()))
partsupp_df = pd.DataFrame(list(partsupp.find()))

# Rename columns
nation_df.columns = ['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT']
region_df.columns = ['R_REGIONKEY', 'R_NAME', 'R_COMMENT']
part_df.columns = ['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT']
supplier_df.columns = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']
partsupp_df.columns = ['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT']

# Merge dataframes
df = pd.merge(part_df, partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')
df = pd.merge(df, supplier_df, left_on='S_SUPPKEY', right_on='S_SUPPKEY')
df = pd.merge(df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
df = pd.merge(df, region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Apply filters
df = df[df['P_SIZE'] == 15]
df = df[df['P_TYPE'].str.contains('BRASS')]
df = df[df['R_NAME'] == 'EUROPE']

# Find minimum PS_SUPPLYCOST
min_supplycost = df['PS_SUPPLYCOST'].min()
df = df[df['PS_SUPPLYCOST'] == min_supplycost]

# Order data
df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False,True,True,True], inplace=True)

# Select columns
df = df[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']]

# Write the output to csv file
df.to_csv('query_output.csv', index=False)
