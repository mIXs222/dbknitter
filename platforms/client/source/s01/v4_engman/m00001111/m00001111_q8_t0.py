import pymysql
import pymongo
import pandas as pd
from pymongo import MongoClient

# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB Connection
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# MySQL Query to select ASIA's REGIONKEY and INDIA's NATIONKEY
mysql_query1 = """
SELECT r.R_REGIONKEY, n.N_NATIONKEY
FROM nation AS n
JOIN region AS r ON n.N_REGIONKEY = r.R_REGIONKEY
WHERE r.R_NAME = 'ASIA' AND n.N_NAME = 'INDIA'
"""

with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query1)
    result1 = cursor.fetchone()
    asia_regionkey, india_nationkey = result1

# MongoDB Aggregation for revenue by year for SMALL PLATED COPPER from ASIA supplied by INDIA
pipeline = [
    {
        '$match': {'P_TYPE': 'SMALL PLATED COPPER'}
    },
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'P_PARTKEY',
            'foreignField': 'L_PARTKEY',
            'as': 'lineitem'
        }
    },
    {"$unwind": "$lineitem"},
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'lineitem.L_ORDERKEY',
            'foreignField': 'O_ORDERKEY',
            'as': 'orders'
        }
    },
    {"$unwind": "$orders"},
    {
        '$lookup': {
            'from': 'supplier',
            'localField': 'lineitem.L_SUPPKEY',
            'foreignField': 'S_SUPPKEY',
            'as': 'supplier'
        }
    },
    {"$unwind": "$supplier"},
    {
        '$match': {'supplier.S_NATIONKEY': india_nationkey, 'orders.O_ORDERDATE': {'$regex': '199[56]'}}
    },
    {
        '$project': {
            'revenue': {'$multiply': ['$lineitem.L_EXTENDEDPRICE', {'$subtract': [1, '$lineitem.L_DISCOUNT']}]},
            'order_year': {'$substr': ['$orders.O_ORDERDATE', 0, 4]},
            'supplier_nationkey': '$supplier.S_NATIONKEY'
        }
    },
    {
        '$group': {
            '_id': '$order_year', 
            'total_revenue': {'$sum': '$revenue'}
        }
    }
]

mongo_results = list(mongo_db.part.aggregate(pipeline))

# Convert the results to DataFrame and calculate market share
df = pd.DataFrame(mongo_results)
df.columns = ['order_year', 'market_share']
df.sort_values(by='order_year', inplace=True)
df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
