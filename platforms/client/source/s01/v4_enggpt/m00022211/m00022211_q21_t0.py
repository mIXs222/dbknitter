# File: execute_query.py

import pymysql
import pymongo
import pandas as pd
import json
import direct_redis

# Connection to MySQL (for the 'nation' table)
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
with mysql_conn.cursor() as cursor:
    # Query to fetch nation data
    cursor.execute("SELECT * FROM nation WHERE N_NAME = 'SAUDI ARABIA';")
    nations = cursor.fetchall()
sa_nationkey = [n[0] for n in nations]

# Query to connect and collect data from Redis (assume supplier is in JSON format)
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
supplier_json_data = redis_client.get('supplier')
suppliers_df = pd.DataFrame(json.loads(supplier_json_data))

# Filter suppliers from Saudi Arabia
suppliers_sa = suppliers_df[suppliers_df['S_NATIONKEY'].isin(sa_nationkey)]

# Connection to MongoDB (for 'orders' and 'lineitem' tables)
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
orders_collection = mongo_db["orders"]
lineitem_collection = mongo_db["lineitem"]

# Fetch orders with status 'F'
orders_fulfilled = list(orders_collection.find({"O_ORDERSTATUS": "F"}))

# Convert to DataFrame for easier manipulation
orders_df = pd.DataFrame(orders_fulfilled)

# Fetch all line items
all_lineitems = list(lineitem_collection.find({}))

# Filter lineitems where L_RECEIPTDATE > L_COMMITDATE
lineitems_df = pd.DataFrame(all_lineitems)
lineitems_waiting = lineitems_df[lineitems_df['L_RECEIPTDATE'] > lineitems_df['L_COMMITDATE']]

# Join and filter the lineitems with the orders
final_lineitems = lineitems_waiting[lineitems_waiting['L_ORDERKEY'].isin(orders_df['O_ORDERKEY'])]

# Calculate NUMWAIT and prepare the final result
waiting_statistics = final_lineitems.groupby('L_SUPPKEY').size().reset_index(name='NUMWAIT')
waiting_statistics = waiting_statistics.rename(columns={'L_SUPPKEY': 'S_SUPPKEY'})

# Join with suppliers to get supplier names
final_result = pd.merge(waiting_statistics, suppliers_sa, left_on='S_SUPPKEY', right_on='S_SUPPKEY')

# Select and order the relevant columns
final_result = final_result[['S_NAME', 'NUMWAIT']].sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Output the query results to CSV
final_result.to_csv('query_output.csv', index=False)
