from pymongo import MongoClient
import csv

# connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["tpch"]
table = db["lineitem"]

# perform the aggregation
pipeline = [
    {"$match": 
        {"L_SHIPDATE": {"$lte": '1998-09-02'}}
    }, 
    {"$group": 
        {
            "_id": {"L_RETURNFLAG": "$L_RETURNFLAG", "L_LINESTATUS": "$L_LINESTATUS"}, 
            "SUM_QTY": {"$sum": "$L_QUANTITY"}, 
            "SUM_BASE_PRICE": {"$sum": "$L_EXTENDEDPRICE"}, 
            "SUM_DISC_PRICE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]}}, 
            "SUM_CHARGE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}, {"$add": [1, "$L_TAX"]}]}}, 
            "AVG_QTY": {"$avg": "$L_QUANTITY"}, 
            "AVG_PRICE": {"$avg": "$L_EXTENDEDPRICE"}, 
            "AVG_DISC": {"$avg": "$L_DISCOUNT"}, 
            "COUNT_ORDER": {"$sum": 1}
        }
    }, 
    {"$sort": 
        {"_id.L_RETURNFLAG": 1, "_id.L_LINESTATUS": 1}
    }
]

result = list(table.aggregate(pipeline))

# prepare data for CSV
csv_data = []
for r in result:
    _id = r["_id"]
    row = [_id["L_RETURNFLAG"], _id["L_LINESTATUS"], r["SUM_QTY"], r["SUM_BASE_PRICE"], r["SUM_DISC_PRICE"], r["SUM_CHARGE"], r["AVG_QTY"], r["AVG_PRICE"], r["AVG_DISC"], r["COUNT_ORDER"]]
    csv_data.append(row)

# write to CSV
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["L_RETURNFLAG", "L_LINESTATUS", "SUM_QTY", "SUM_BASE_PRICE", "SUM_DISC_PRICE", "SUM_CHARGE", "AVG_QTY", "AVG_PRICE", "AVG_DISC", "COUNT_ORDER"])
    writer.writerows(csv_data)
