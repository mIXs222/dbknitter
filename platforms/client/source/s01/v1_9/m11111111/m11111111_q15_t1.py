from pymongo import MongoClient
import pandas as pd
from datetime import datetime

client = MongoClient("mongodb://mongodb:27017")
db = client['tpch']

pipeline = [
    {
        "$match": {
            "L_SHIPDATE": {
                "$gte": datetime.strptime('1996-01-01', '%Y-%m-%d'),
                "$lt" : datetime.strptime('1996-04-01', '%Y-%m-%d')
            }
        }
    },
    {
        "$group" : {
            "_id": "$L_SUPPKEY",
            "TOTAL_REVENUE" : {
                "$sum" :  { "$multiply": [ "$L_EXTENDEDPRICE", {"$subtract": [1 , "$L_DISCOUNT"] } ] }
            }
        }
    }
]

revenue_list = list(db.lineitem.aggregate(pipeline))

supplier_list = list(db.supplier.find({}))

result = []
max_revenue = max(item['TOTAL_REVENUE'] for item in revenue_list)
for revenue in revenue_list:
    if revenue['TOTAL_REVENUE'] == max_revenue:
        for supplier in supplier_list:
            if supplier['S_SUPPKEY'] == revenue['_id']:
                result.append([supplier['S_SUPPKEY'], supplier['S_NAME'], supplier['S_ADDRESS'], supplier['S_PHONE'], revenue['TOTAL_REVENUE']])

result_df = pd.DataFrame(result, columns=["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_PHONE", "TOTAL_REVENUE"])
result_df.to_csv('query_output.csv', index=False)
