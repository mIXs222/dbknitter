import pymongo
import csv
import datetime

# Connection to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
db = client["tpch"]

# Date range for L_SHIPDATE
start_date = datetime.datetime(1994, 1, 1, 0, 0)
end_date = datetime.datetime(1995, 1, 1, 0, 0)

# Query lineitems in MongoDB
lineitems = db.lineitem.find(
    {
        "L_SHIPDATE": {"$gte": start_date, "$lt": end_date},
        "L_DISCOUNT": {"$gte": 0.06 - 0.01, "$lte": 0.06 + 0.01},
        "L_QUANTITY": {"$lt": 24}
    }
)

# Calculate additional revenue
additional_revenue = sum(
    item["L_EXTENDEDPRICE"] * item["L_DISCOUNT"] for item in lineitems
)

# Write output to csv
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['ADDITIONAL_REVENUE'])
    writer.writerow([additional_revenue])
