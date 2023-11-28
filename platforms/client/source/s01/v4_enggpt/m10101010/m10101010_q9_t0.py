import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4')

# MongoDB connection
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_db = mongodb_client["tpch"]

# MySQL query for fetching 'dim' parts and associated line items
mysql_query = """
SELECT
    s.S_NATIONKEY,
    l.L_ORDERKEY,
    l.L_PARTKEY,
    l.L_EXTENDEDPRICE,
    l.L_DISCOUNT,
    l.L_QUANTITY,
    l.L_SUPPKEY
FROM
    lineitem l
JOIN
    part p ON l.L_PARTKEY = p.P_PARTKEY
JOIN
    supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
WHERE
    p.P_NAME LIKE '%dim%'
"""

# MongoDB query for fetching supply cost from 'partsupp'
partsupp_pipeline = [
    {
        '$lookup': {
            'from': 'part',
            'localField': 'PS_PARTKEY',
            'foreignField': 'P_PARTKEY',
            'as': 'part'
        }
    },
    {'$match': {'part.P_NAME': {'$regex': 'dim'}}},
    {
        '$project': {
            'PS_PARTKEY': 1,
            'PS_SUPPKEY': 1,
            'PS_SUPPLYCOST': 1
        }
    }
]
partsupp_data = list(mongodb_db.partsupp.aggregate(partsupp_pipeline))

# Prepare supply cost mapping
supply_cost_mapping = {(ps['PS_PARTKEY'], ps['PS_SUPPKEY']): ps['PS_SUPPLYCOST']
                       for ps in partsupp_data}

# MongoDB query for fetching order details
order_pipeline = [
    {
        '$lookup': {
            'from': 'nation',
            'localField': 'O_CUSTKEY',
            'foreignField': 'N_NATIONKEY',
            'as': 'nation'
        }
    },
    {'$project': {
        'O_ORDERKEY': 1,
        'O_ORDERDATE': 1
    }}
]
orders_data = list(mongodb_db.orders.aggregate(order_pipeline))

# Prepare order details mapping
order_details_mapping = {o['O_ORDERKEY']: datetime.strptime(o['O_ORDERDATE'], "%Y-%m-%d %H:%M:%S")
                         for o in orders_data}

# Fetch line items and process profit calculations
query_results = []
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    for row in cursor:
        nationkey, orderkey, partkey, extendedprice, discount, quantity, suppkey = row
        supply_cost = supply_cost_mapping.get((partkey, suppkey))
        if supply_cost is not None and orderkey in order_details_mapping:
            profit = (extendedprice * (1 - discount)) - (supply_cost * quantity)
            order_year = order_details_mapping[orderkey].year
            query_results.append([nationkey, order_year, round(profit, 2)])
            
query_results.sort(key=lambda x: (x[0], -x[1]))

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['N_NATIONKEY', 'YEAR', 'PROFIT'])
    for result in query_results:
        writer.writerow(result)

# Close the connections
mysql_conn.close()
mongodb_client.close()
