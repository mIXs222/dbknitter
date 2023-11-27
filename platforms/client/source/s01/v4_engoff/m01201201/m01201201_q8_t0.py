import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

mysql_query = """
SELECT s.S_SUPPKEY FROM supplier s
INNER JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
WHERE n.N_NAME = 'INDIA'
"""

with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    supplier_keys = [row[0] for row in cursor.fetchall()]

mysql_conn.close()

# MongoDB connection and query
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongodb = mongo_client.tpch

pipeline = [
    {
        "$match": {
            "$expr": {
                "$and": [
                    {"$eq": ["$R_NAME", "ASIA"]}
                ]
            }
        }
    }
]

region_docs = list(mongodb.region.aggregate(pipeline))
asia_region_keys = [doc['R_REGIONKEY'] for doc in region_docs]

pipeline = [
    {
        "$match": {
            "L_PARTKEY": {"$in": ["SMALL PLATED COPPER"]},
            "L_SUPPKEY": {"$in": supplier_keys},
            "$expr": {
                "$or": [
                    {"$eq": [{"$year": "$L_SHIPDATE"}, 1995]},
                    {"$eq": [{"$year": "$L_SHIPDATE"}, 1996]},
                ]
            },
        }
    },
    {
        "$project": {
            "L_EXTENDEDPRICE": 1,
            "L_DISCOUNT": 1,
            "L_SHIPDATE": 1,
        }
    }
]

lineitems = list(mongodb.lineitem.aggregate(pipeline))
mongo_client.close()

# Redis connection and query
redis_conn = DirectRedis(host='redis', port=6379, db=0)

part_str = redis_conn.get('part')
part_df = pd.read_json(part_str)
small_plated_copper_parts = part_df[part_df['P_TYPE'] == 'SMALL PLATED COPPER']

# Combine all results
revenue = {}

for lineitem in lineitems:
    revenue_year = lineitem['L_SHIPDATE'].year
    if revenue_year in [1995, 1996]:
        if revenue_year not in revenue:
            revenue[revenue_year] = 0

        if lineitem['L_SUPPKEY'] in small_plated_copper_parts['P_PARTKEY'].tolist():
            price_after_discount = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
            revenue[revenue_year] += price_after_discount

# Writing to CSV
results = {
    "Year": ["1995", "1996"],
    "Revenue": [revenue.get(1995, 0), revenue.get(1996, 0)]
}

results_df = pd.DataFrame(results)
results_df.to_csv("query_output.csv", index=False)
