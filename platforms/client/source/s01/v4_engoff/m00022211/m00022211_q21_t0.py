# Python file: query.py

import csv
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Function to get nation_key for "SAUDI ARABIA" from MySQL
def get_nation_key(cursor):
    cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'SAUDI ARABIA';")
    result = cursor.fetchone()
    return result['N_NATIONKEY']

# Setup MySQL Connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
try:
    with mysql_conn.cursor() as cursor:
        saudi_nation_key = get_nation_key(cursor)
finally:
    mysql_conn.close()

# Setup MongoDB Connection
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_db = mongodb_client['tpch']

# Get orders with 'F' status and join with lineitem
orders = list(mongodb_db.orders.find({"O_ORDERSTATUS": "F"}, {"_id": 0}))
lineitem = list(mongodb_db.lineitem.find({"L_COMMITDATE": {"$lt": "L_RECEIPTDATE"}}, {"_id": 0}))

# Convert to DataFrame
orders_df = pd.DataFrame(orders)
lineitem_df = pd.DataFrame(lineitem)

# Merge orders with lineitem on order key
combined_df = pd.merge(orders_df, lineitem_df, left_on="O_ORDERKEY", right_on="L_ORDERKEY")

# Filter orders where the supplier didn't meet the committed date
filtered_df = combined_df[combined_df["L_COMMITDATE"] < combined_df["L_RECEIPTDATE"]]

# Get unique order keys from filtered data
unique_order_keys = filtered_df['L_ORDERKEY'].unique()

# Setup Redis Connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch supplier data from Redis and convert to DataFrame
supplier_data = eval(redis_conn.get('supplier'))
supplier_df = pd.DataFrame(supplier_data)

# Filter suppliers based on nation_key and join with the filtered order-supplier combinations
supplier_df = supplier_df[supplier_df['S_NATIONKEY'] == saudi_nation_key]
result_df = pd.merge(supplier_df, filtered_df, left_on="S_SUPPKEY", right_on="L_SUPPKEY")

# Selecting suppliers who kept orders waiting
final_df = result_df[result_df["L_ORDERKEY"].isin(unique_order_keys)]

# Write to CSV
final_df.to_csv('query_output.csv', index=False)
