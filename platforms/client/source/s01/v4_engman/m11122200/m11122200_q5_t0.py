import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime
from pymongo import MongoClient

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_connection = DirectRedis(
    host='redis',
    port=6379,
    db=0
)

# Get the nations and regions from MongoDB
nation_cursor = mongo_db.nation.find({'N_REGIONKEY': {"$in": [mongo_db.region.find_one({'R_NAME': 'ASIA'})['R_REGIONKEY']]}})
asian_nations = {doc['N_NATIONKEY']: doc['N_NAME'] for doc in nation_cursor}

# Fetch supplier keys for nations in ASIA
supplier_keys = eval(redis_connection.get('supplier'))
asian_supplier_keys = {int(row['S_SUPPKEY']): row['S_NATIONKEY'] for row in supplier_keys if row['S_NATIONKEY'] in asian_nations}

# Fetch customer keys for nations in ASIA
customer_keys = eval(redis_connection.get('customer'))
asian_customer_keys = {int(row['C_CUSTKEY']): row['C_NATIONKEY'] for row in customer_keys if row['C_NATIONKEY'] in asian_nations}

# MySQL query to fetch lineitem and order data
mysql_cursor.execute("""
SELECT
    L_ORDERKEY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    O_CUSTKEY
FROM
    lineitem
JOIN
    orders ON L_ORDERKEY = O_ORDERKEY
WHERE
    O_ORDERDATE >= '1990-01-01' AND O_ORDERDATE < '1995-01-01'
""")
lineitem_orders = mysql_cursor.fetchall()

# Calculate REVENUE for each nation from lineitem and customer data
revenues = {}
for row in lineitem_orders:
    orderkey, price, discount, custkey = row
    if custkey in asian_customer_keys:
        nationkey = asian_customer_keys[custkey]
        revenue = price * (1 - discount)
        if nationkey in revenues:
            revenues[nationkey] += revenue
        else:
            revenues[nationkey] = revenue

# Prepare the final results
results = [{'N_NAME': asian_nations[nationkey], 'REVENUE': revenue} 
           for nationkey, revenue in revenues.items() if nationkey in asian_supplier_keys]

# Sort by REVENUE in descending order
results.sort(key=lambda x: x['REVENUE'], reverse=True)

# Create a DataFrame and write to CSV
df = pd.DataFrame(results)
df.to_csv('query_output.csv', index=False)

# Close all connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
