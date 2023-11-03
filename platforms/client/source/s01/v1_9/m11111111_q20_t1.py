import pandas as pd
from pymongo import MongoClient

def mongo_connect(host, port, db):
    conn = MongoClient(host, port)
    return conn[db]

def execute_query():
    db = mongo_connect("mongodb", 27017, "tpch")

    suppliers = pd.DataFrame(list(db.supplier.find({}, {"_id": 0, "S_NAME": 1, "S_ADDRESS": 1, "S_SUPPKEY": 1, "S_NATIONKEY": 1})))
    nations = pd.DataFrame(list(db.nation.find({}, {"_id": 0, "N_NATIONKEY": 1, "N_NAME": 1})))
    parts = pd.DataFrame(list(db.part.find({"P_NAME": {"$regex": "^forest"}}, {"_id": 0, "P_PARTKEY": 1})))
    partsupps = pd.DataFrame(list(db.partsupp.find({}, {"_id": 0, "PS_PARTKEY": 1, "PS_SUPPKEY": 1, "PS_AVAILQTY": 1})))
    lineitems = pd.DataFrame(list(db.lineitem.find({"L_SHIPDATE": {"$gte": "1994-01-01", "$lt": "1995-01-01"} } , {"_id": 0, "L_PARTKEY": 1, "L_SUPPKEY": 1, "L_QUANTITY": 1})))

    merge1 = pd.merge(partsupps, parts, how="inner", left_on="PS_PARTKEY", right_on="P_PARTKEY")
    merge2 = pd.merge(merge1, lineitems, how="inner", left_on=["PS_PARTKEY", "PS_SUPPKEY"], right_on=["L_PARTKEY", "L_SUPPKEY"])
    merge3 = pd.merge(merge2, suppliers, how="inner", left_on="PS_SUPPKEY", right_on="S_SUPPKEY")
    final_df = pd.merge(merge3, nations, how="inner", left_on="S_NATIONKEY", right_on="N_NATIONKEY")

    final_df = final_df[final_df["N_NAME"] == "CANADA"]
    final_df = final_df[final_df["PS_AVAILQTY"] > 0.5 * final_df["L_QUANTITY"].sum()]
    final_df[["S_NAME", "S_ADDRESS"]].to_csv('query_output.csv', index=False)

execute_query()
