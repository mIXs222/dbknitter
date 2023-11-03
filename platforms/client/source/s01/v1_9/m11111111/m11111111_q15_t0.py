from pymongo import MongoClient
from decimal import Decimal
from dateutil.relativedelta import relativedelta
import csv

client = MongoClient('mongodb://mongodb:27017/')
database = client['tpch']
supplier_table = database['supplier']
lineitem_table = database['lineitem']

start_date = datetime.datetime(1996, 1, 1, 0, 0, 0)
end_date = start_date + relativedelta(months=3)

pipeline = [
    {
        "$match": 
        {
            "L_SHIPDATE": 
            {
                "$gte": start_date, 
                "$lt": end_date
            }
        }
    }, 
    {
        "$group": 
        {
            "_id": "$L_SUPPKEY", 
            "TOTAL_REVENUE": 
            {
                "$sum": 
                {
                    "$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]
                }
            }
        }
    }
]

revenue0 = list(lineitem_table.aggregate(pipeline))

max_revenue = max([data['TOTAL_REVENUE'] for data in revenue0])
best_suppliers = [supplier['_id'] for supplier in revenue0 if supplier['TOTAL_REVENUE'] == max_revenue]

query_result = list(supplier_table.find({"S_SUPPKEY": {"$in": best_suppliers}}, {"S_SUPPKEY": 1, "S_NAME": 1, "S_ADDRESS": 1, "S_PHONE": 1, "TOTAL_REVENUE": 1}))

with open("query_output.csv", "w") as csvfile:
    fieldnames = query_result[0].keys()
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for data in query_result:
        writer.writerow(data)
