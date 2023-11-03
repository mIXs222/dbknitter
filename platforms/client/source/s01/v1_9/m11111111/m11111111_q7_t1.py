from pymongo import MongoClient
import pandas as pd

# Create a MongoDB client
client = MongoClient("mongodb://mongodb:27017/")

# Connect to the tpch database
db = client.tpch

# Create pandas dataframes for each collections
nation_df = pd.DataFrame(list(db.nation.find()))
region_df = pd.DataFrame(list(db.region.find()))
part_df = pd.DataFrame(list(db.part.find()))
supplier_df = pd.DataFrame(list(db.supplier.find()))
partsupp_df = pd.DataFrame(list(db.partsupp.find()))
customer_df = pd.DataFrame(list(db.customer.find()))
orders_df = pd.DataFrame(list(db.orders.find()))
lineitem_df = pd.DataFrame(list(db.lineitem.find()))

# Merge dataframes
merged_df = lineitem_df.merge(supplier_df, how="inner", left_on="L_SUPPKEY", right_on="S_SUPPKEY")
merged_df = merged_df.merge(orders_df, how="inner", left_on="L_ORDERKEY", right_on="O_ORDERKEY")
merged_df = merged_df.merge(customer_df, how="inner", left_on="O_CUSTKEY", right_on="C_CUSTKEY")
merged_df = merged_df.merge(nation_df, how="inner", left_on="S_NATIONKEY", right_on="N_NATIONKEY")
merged_df = merged_df.merge(nation_df, how="inner", left_on="C_NATIONKEY", right_on="N_NATIONKEY")

# Filter rows
filtered_df = merged_df[((merged_df.N_NAME_x == "JAPAN") & (merged_df.N_NAME_y == "INDIA")) | ((merged_df.N_NAME_x == "INDIA") & (merged_df.N_NAME_y == "JAPAN"))]
filtered_df = filtered_df[((filtered_df.L_SHIPDATE >= "1995-01-01") & (filtered_df.L_SHIPDATE <= "1996-12-31"))]

# Calculate Volume column
filtered_df["VOLUME"] = filtered_df.L_EXTENDEDPRICE * (1 - filtered_df.L_DISCOUNT)

# Group by aggregation
result_df = filtered_df.groupby(["N_NAME_x", "N_NAME_y", "L_SHIPDATE"]).VOLUME.sum().reset_index()

# Rename columns
result_df.columns = ["SUPP_NATION", "CUST_NATION", "L_YEAR", "REVENUE"]

# Sort values
result_df.sort_values(by=["SUPP_NATION", "CUST_NATION", "L_YEAR"], inplace=True)

# Write the output to a csv file
result_df.to_csv("query_output.csv", index=False)
