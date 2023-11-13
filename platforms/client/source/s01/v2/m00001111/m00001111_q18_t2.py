import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://<username>:<password>@mongodb:27017/")
db = client['tpch']

# Convert MongoDB collections to pandas DataFrames
customer = pd.DataFrame(list(db.customer.find()))
orders = pd.DataFrame(list(db.orders.find()))
lineitem = pd.DataFrame(list(db.lineitem.find()))

# Join three tables
merged_df = pd.merge(customer, orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = pd.merge(merged_df, lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Subquery
subquery_df = lineitem.groupby("L_ORDERKEY").filter(lambda x: x["L_QUANTITY"].sum() > 300)

# Main SQL query
result_df = (merged_df[merged_df["O_ORDERKEY"].isin(subquery_df["L_ORDERKEY"].unique())]
                    .groupby(["C_NAME", "C_CUSTKEY", "O_ORDERKEY", "O_ORDERDATE", "O_TOTALPRICE"])
                    .agg({"L_QUANTITY": "sum"})
                    .sort_values(["O_TOTALPRICE", "O_ORDERDATE"], ascending=[False, True])
                    .reset_index())

# Save result to CSV
result_df.to_csv("query_output.csv", index=False)
