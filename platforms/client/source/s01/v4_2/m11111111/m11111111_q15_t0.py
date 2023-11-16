# connect.py

from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta

# Prepare the mongo client
client = MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

# Prepare the base filters
base_filters = {"L_SHIPDATE": {"$gte": datetime.strptime("1996-01-01", "%Y-%m-%d"), 
                               "$lt": datetime.strptime("1996-01-01", "%Y-%m-%d") + timedelta(90)
                              }
                }

lineitems = db["lineitem"].find(base_filters)
supplier_dict = {supp["S_SUPPKEY"]: supp for supp in db["supplier"].find()}

supplier_revenue = {}

for lineitem in lineitems:
    revenue = lineitem["L_EXTENDEDPRICE"] * (1 - lineitem["L_DISCOUNT"])
    supplier_revenue[lineitem["L_SUPPKEY"]] = supplier_revenue.get(lineitem["L_SUPPKEY"], 0) + revenue

revenue_df = pd.DataFrame([(suppkey, revenue) for suppkey, revenue in supplier_revenue.items()], columns=["S_SUPPKEY", "TOTAL_REVENUE"])
supplier_df = pd.DataFrame(list(supplier_dict.values()))

merged_df = pd.merge(supplier_df, revenue_df, on="S_SUPPKEY")
max_revenue = merged_df["TOTAL_REVENUE"].max()
result_df = merged_df[merged_df["TOTAL_REVENUE"] == max_revenue]

result_df.to_csv("query_output.csv", index=False)
