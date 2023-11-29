import pymysql
import pymongo
from direct_redis import DirectRedis
import pandas as pd

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("SELECT S_SUPPKEY, S_NAME FROM supplier")
supplier_records = mysql_cursor.fetchall()
supplier_df = pd.DataFrame(supplier_records, columns=["S_SUPPKEY", "S_NAME"])

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']
lineitem_collection = mongo_db['lineitem']
orders_df = pd.DataFrame(list(orders_collection.find({}, {"_id": 0})))
lineitem_df = pd.DataFrame(list(lineitem_collection.find({}, {"_id": 0})))

# Connect to Redis
redis_client = DirectRedis(host="redis", port=6379, db=0)
nation_df = pd.read_json(redis_client.get('nation'))

# Query
saudi_nations = nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA']['N_NATIONKEY'].tolist()
saudi_suppliers = supplier_df[supplier_df['S_SUPPKEY'].isin(saudi_nations)]

# Find orders with status 'F'
orders_f_df = orders_df[orders_df['O_ORDERSTATUS'] == 'F']

# Multi-supplier orders
multi_lineitem_df = lineitem_df.groupby('L_ORDERKEY').filter(lambda x: x['L_SUPPKEY'].nunique() > 1)

# Lineitems that have failed to meet the commit date
failed_lineitems = multi_lineitem_df[multi_lineitem_df['L_COMMITDATE'] < multi_lineitem_df['L_RECEIPTDATE']]

# Find suppliers who were the only one who failed in a multi-supplier order
only_failed_suppliers = failed_lineitems.groupby('L_ORDERKEY').filter(lambda x: (x['L_SUPPKEY'].isin(saudi_suppliers['S_SUPPKEY']) & (x['L_RECEIPTDATE'] > x['L_COMMITDATE'])).all())

# Count the number of waits per supplier
numwait_df = only_failed_suppliers['L_SUPPKEY'].value_counts().reset_index()
numwait_df.columns = ['S_SUPPKEY', 'NUMWAIT']

# Merge with supplier names and sort
result_df = pd.merge(numwait_df, supplier_df, left_on='S_SUPPKEY', right_on='S_SUPPKEY')
result_df = result_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True], ignore_index=True)

# Write to CSV
result_df.to_csv('query_output.csv', index=False, columns=['NUMWAIT', 'S_NAME'])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
