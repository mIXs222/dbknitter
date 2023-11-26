import pymysql
import pymongo
import direct_redis
import pandas as pd

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Querying MySQL to get nation data
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_NAME = 'SAUDI ARABIA'")
    nations = cursor.fetchall()
    nations_df = pd.DataFrame(nations, columns=['N_NATIONKEY', 'N_NAME'])

# Querying MongoDB to get orders and lineitem data
orders = mongo_db.orders.find({'O_ORDERSTATUS': 'F'}, {'_id': 0})
lineitem = mongo_db.lineitem.find({'L_RECEIPTDATE':{'$gt': '$L_COMMITDATE'}}, {'_id': 0})

orders_df = pd.DataFrame(list(orders))
lineitem_df = pd.DataFrame(list(lineitem))

# Filtering lineitem to only include those that meet the Subquery conditions
l1_df = lineitem_df.copy()
for index, row in l1_df.iterrows():
    exists_cond = lineitem_df[
        (lineitem_df['L_ORDERKEY'] == row['L_ORDERKEY']) &
        (lineitem_df['L_SUPPKEY'] != row['L_SUPPKEY'])
    ].shape[0] > 0
    
    not_exists_cond = lineitem_df[
        (lineitem_df['L_ORDERKEY'] == row['L_ORDERKEY']) &
        (lineitem_df['L_SUPPKEY'] != row['L_SUPPKEY']) &
        (lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE'])
    ].shape[0] == 0
    
    if not (exists_cond and not_exists_cond):
        l1_df.drop(index, inplace=True)

# Get suppliers data from redis
supplier_data = redis_conn.get('supplier')
supplier_df = pd.read_json(supplier_data, orient='index')

# Merge the datasets
merged_df = supplier_df.merge(l1_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
merged_df = merged_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df.merge(nations_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Group by S_NAME
result = merged_df.groupby('S_NAME').size().reset_index(name='NUMWAIT')

# Sort by NUMWAIT DESC and S_NAME ASC
result.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True], inplace=True)

# Write to CSV
result.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
redis_conn.close()
