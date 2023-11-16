import pymongo
import pandas as pd

client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]
part = pd.DataFrame(list(db.part.find({})))
lineitem = pd.DataFrame(list(db.lineitem.find({})))

conditions = [
    ((part["P_PARTKEY"] == lineitem["L_PARTKEY"]) &
    (part["P_BRAND"] == 'Brand#12') &
    (part["P_CONTAINER"].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) &
    (lineitem["L_QUANTITY"].between(1, 1 + 10)) &
    (part["P_SIZE"].between(1, 5)) &
    (lineitem["L_SHIPMODE"].isin(['AIR', 'AIR REG'])) &
    (lineitem["L_SHIPINSTRUCT"] == 'DELIVER IN PERSON')),

    ((part["P_PARTKEY"] == lineitem["L_PARTKEY"]) &
    (part["P_BRAND"] == 'Brand#23') &
    (part["P_CONTAINER"].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) &
    (lineitem["L_QUANTITY"].between(10, 10 + 10)) &
    (part["P_SIZE"].between(1, 10)) &
    (lineitem["L_SHIPMODE"].isin(['AIR', 'AIR REG'])) &
    (lineitem["L_SHIPINSTRUCT"] == 'DELIVER IN PERSON')),

    ((part["P_PARTKEY"] == lineitem["L_PARTKEY"]) &
    (part["P_BRAND"] == 'Brand#34') &
    (part["P_CONTAINER"].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) &
    (lineitem["L_QUANTITY"].between(20, 20 + 10)) &
    (part["P_SIZE"].between(1, 15)) &
    (lineitem["L_SHIPMODE"].isin(['AIR', 'AIR REG'])) &
    (lineitem["L_SHIPINSTRUCT"] == 'DELIVER IN PERSON'))
]

combined_df = pd.concat([part[condition] for condition in conditions])

revenue = (combined_df['L_EXTENDEDPRICE']* (1 - combined_df['L_DISCOUNT'])).sum()
output_df = pd.DataFrame([revenue], columns=['REVENUE'])
output_df.to_csv('query_output.csv', index=False)
