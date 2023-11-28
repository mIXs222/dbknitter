import pymysql
import pymongo
import pandas as pd
from pymongo import MongoClient

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client.tpch

# Fetch parts that satisfy brand and container conditions from MySQL
part_query = "SELECT P_PARTKEY FROM part WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG';"
with mysql_conn.cursor() as cursor:
    cursor.execute(part_query)
    # Convert list of tuples to list
    relevant_parts = [part[0] for part in cursor.fetchall()]

# Close MySQL connection
mysql_conn.close()

# Query to fetch lineitems in MongoDB
pipeline = [
    {
        "$match": {
            "L_PARTKEY": {"$in": relevant_parts}
        }
    },
    {
        "$group": {
            "_id": "$L_PARTKEY",
            "average_quantity": {"$avg": "$L_QUANTITY"}
        }
    },
    {
        "$project": {
            "twenty_percent_avg_qty": {"$multiply": ["$average_quantity", 0.20]}
        }
    }
]

average_quantities = list(mongodb.lineitem.aggregate(pipeline))
avg_qty_dict = {item['_id']: item['twenty_percent_avg_qty'] for item in average_quantities}

# Query to get lineitem details with quantity less than 20% of average quantity
lineitem_details = list(mongodb.lineitem.find(
    {
        "L_PARTKEY": {"$in": relevant_parts},
        "L_QUANTITY": {"$lt": {"$ref": "twenty_percent_avg_qty"}},
        "L_EXTENDEDPRICE": {"$exists": True}
    },
    {"_id": 0, "L_EXTENDEDPRICE": 1, "L_PARTKEY": 1}
))

# Calculating average yearly extended price
extended_prices = []
for item in lineitem_details:
    if item['L_PARTKEY'] in avg_qty_dict and item['L_QUANTITY'] < avg_qty_dict[item['L_PARTKEY']]:
        extended_prices.append(item['L_EXTENDEDPRICE'])

average_yearly_extended_price = sum(extended_prices) / 7.0 if extended_prices else 0

# Write the results to a CSV file
results_df = pd.DataFrame({"AverageYearlyExtendedPrice": [average_yearly_extended_price]})
results_df.to_csv('query_output.csv', index=False)
