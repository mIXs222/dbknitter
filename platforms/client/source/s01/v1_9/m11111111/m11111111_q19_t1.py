from pymongo import MongoClient
import csv

# Create a MongoDB client
client = MongoClient("mongodb://mongodb:27017/")

# Connect to the tpch database
db = client['tpch']

# Access the collections (tables)
lineitem = db['lineitem']
part = db['part']

pipeline = [
    {
        "$lookup": {
            "from": "part",
            "localField": "L_PARTKEY",
            "foreignField": "P_PARTKEY",
            "as": "part_data"
        }
    },
    {
        "$match": {
            "$or": [
                {
                    "part_data.P_BRAND": "Brand#12",
                    "part_data.P_CONTAINER": { "$in": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"] },
                    "L_QUANTITY": { "$gte": 1, "$lte": 11 },
                    "part_data.P_SIZE": { "$gte": 1, "$lte": 5 },
                    "L_SHIPMODE": { "$in": ["AIR", "AIR REG"] },
                    "L_SHIPINSTRUCT": "DELIVER IN PERSON"
                },
                {
                    "part_data.P_BRAND": "Brand#23",
                    "part_data.P_CONTAINER": { "$in": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"] },
                    "L_QUANTITY": { "$gte": 10, "$lte": 20 },
                    "part_data.P_SIZE": { "$gte": 1, "$lte": 10 },
                    "L_SHIPMODE": { "$in": ["AIR", "AIR REG"] },
                    "L_SHIPINSTRUCT": "DELIVER IN PERSON"
                },
                {
                    "part_data.P_BRAND": "Brand#34",
                    "part_data.P_CONTAINER": { "$in": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"] },
                    "L_QUANTITY": { "$gte": 20, "$lte": 30 },
                    "part_data.P_SIZE": { "$gte": 1, "$lte": 15 },
                    "L_SHIPMODE": { "$in": ["AIR", "AIR REG"] },
                    "L_SHIPINSTRUCT": "DELIVER IN PERSON"
                },
            ]
        }
    },
    {
        "$group": {
            "_id": None,
            "REVENUE": { "$sum": { "$multiply": ["$L_EXTENDEDPRICE", { "$subtract": [1, "$L_DISCOUNT"] }] } }
        }
    }
]

# Execute the query
results = list(lineitem.aggregate(pipeline))

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["REVENUE"])
    writer.writerow([results[0]['REVENUE']])
