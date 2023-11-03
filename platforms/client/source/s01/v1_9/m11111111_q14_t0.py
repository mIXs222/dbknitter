from pymongo import MongoClient
from datetime import datetime
import pandas as pd

client = MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

lineitem = db["lineitem"].find({
    "L_SHIPDATE": {"$gte": datetime.strptime('1995-09-01', '%Y-%m-%d'), "$lt": datetime.strptime('1995-10-01', '%Y-%m-%d')}
})

part = db["part"].find()

join_data = []
for item in lineitem:
    for p in part:
        if item["L_PARTKEY"] == p["P_PARTKEY"]:
            join_data.append({
                "P_TYPE": p["P_TYPE"],
                "L_EXTENDEDPRICE": item["L_EXTENDEDPRICE"],
                "L_DISCOUNT": item["L_DISCOUNT"]
            })

df = pd.DataFrame(join_data)
df["CALC"] = df.apply(lambda row: (row["L_EXTENDEDPRICE"] * (1 - row["L_DISCOUNT"])) if row["P_TYPE"].startswith("PROMO") else 0, axis=1)

promo_revenue = 100.00 * df["CALC"].sum() / df["L_EXTENDEDPRICE"].sum() * (1 - df["L_DISCOUNT"].sum())
result = pd.DataFrame([promo_revenue], columns=["PROMO_REVENUE"])

result.to_csv("query_output.csv", index=False)
