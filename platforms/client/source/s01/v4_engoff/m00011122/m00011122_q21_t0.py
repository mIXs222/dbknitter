# suppliers_awaiting_orders.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
with mysql_conn.cursor() as cursor:
    # Fetch nation key for "SAUDI ARABIA"
    cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'SAUDI ARABIA'")
    saudi_nation_key = cursor.fetchone()[0]

    # Fetch suppliers from nation "SAUDI ARABIA"
    cursor.execute("SELECT S_SUPPKEY, S_NAME FROM supplier WHERE S_NATIONKEY = %s", (saudi_nation_key,))
    suppliers = cursor.fetchall()

saudi_suppliers = {s[0]: s[1] for s in suppliers}

mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]

# Find all suppliers in "SAUDI ARABIA"
supplier_docs = list(mongo_db.supplier.find({"S_NATIONKEY": saudi_nation_key}, {"_id": 0, "S_SUPPKEY": 1}))
supplier_keys = {doc["S_SUPPKEY"] for doc in supplier_docs}

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Load orders and lineitems tables from Redis
orders_df = pd.read_json(redis_conn.get('orders'), orient='records')
lineitem_df = pd.read_json(redis_conn.get('lineitem'), orient='records')

# Identify orders with status 'F' (finished)
finished_orders = orders_df[orders_df['O_ORDERSTATUS'] == 'F']

# Merge lineitem with finished orders based on order key
merged_df = pd.merge(finished_orders, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter suppliers who are from "SAUDI ARABIA" and failed to deliver
awaiting_orders = merged_df[
    (merged_df['L_SUPPKEY'].isin(supplier_keys)) &
    (merged_df['L_COMMITDATE'] < merged_df['L_RECEIPTDATE'])
]

# Group by supplier key to filter only those orders where all lines are late
grouped = awaiting_orders.groupby("L_SUPPKEY").filter(lambda x: x['L_RECEIPTDATE'].count() == len(x))

# Select distinct supplier names
distinct_suppliers = grouped['L_SUPPKEY'].drop_duplicates().to_list()

# Output results
output_suppliers = [{'S_SUPPKEY': sup_key, 'S_NAME': saudi_suppliers[sup_key]} for sup_key in distinct_suppliers if sup_key in saudi_suppliers]

# Write to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=['S_SUPPKEY', 'S_NAME'])
    writer.writeheader()
    for data in output_suppliers:
        writer.writerow(data)
