import pymongo
from pymongo import MongoClient
import pandas as pd

# Connect with mongodb
client = MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

# Get all tables
nation = pd.DataFrame(list(db["nation"].find()))
supplier = pd.DataFrame(list(db["supplier"].find()))
partsupp = pd.DataFrame(list(db["partsupp"].find()))

# Rename columns
nation.columns = ['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT']
supplier.columns = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']
partsupp.columns = ['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT']

# Combine tables
combined = nation.merge(supplier, left_on='N_NATIONKEY', right_on='S_NATIONKEY').merge(partsupp, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Select nation GERMANY
combined = combined[combined["N_NAME"] == 'GERMANY']

# Create new column VALUE
combined["VALUE"] = combined["PS_SUPPLYCOST"] * combined["PS_AVAILQTY"]

# Get sum of VALUE
total_value = combined["VALUE"].sum() * 0.0001000000

# Filter data
combined = combined.groupby("PS_PARTKEY").filter(lambda x: x["VALUE"].sum() > total_value)

# Group by PS_PARTKEY and get sum of VALUE
result = combined.groupby("PS_PARTKEY")["VALUE"].sum().reset_index()

# Order by VALUE in DESC order
result = result.sort_values(by='VALUE', ascending=False)

# Write the output to csv file
result.to_csv("query_output.csv", index=False)
