import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Establish MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Get nations data from MySQL
mysql_cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_NAME = 'SAUDI ARABIA'")
nations = mysql_cursor.fetchall()
nation_df = pd.DataFrame(nations, columns=['N_NATIONKEY', 'N_NAME'])

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Establish MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Get suppliers data from MongoDB
suppliers = list(mongo_db['supplier'].find({}, {'_id': 0, 'S_SUPPKEY': 1, 'S_NAME': 1, 'S_NATIONKEY': 1}))
supplier_df = pd.DataFrame(suppliers)

# Merge suppliers with nations based on nation key and filter by nation name
supplier_nation_df = supplier_df.merge(nation_df, left_on="S_NATIONKEY", right_on="N_NATIONKEY")

# Establish Redis connection
redis = DirectRedis(host='redis', port=6379, db=0)

# Get orders and lineitem data from Redis
orders_df = pd.read_json(redis.get('orders'), orient='records')
lineitem_df = pd.read_json(redis.get('lineitem'), orient='records')

# Filter orders and lineitems according to query conditions
filtered_orders = orders_df[(orders_df['O_ORDERSTATUS'] == 'F')]
filtered_lineitem = lineitem_df[(lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE'])]

# Prepare sub-query for EXISTS condition
l1_l2_df = lineitem_df[lineitem_df.duplicated(subset=['L_ORDERKEY'], keep=False)]
l1_l2_df = l1_l2_df.drop_duplicates(subset=['L_ORDERKEY', 'L_SUPPKEY'])

# Prepare sub-query for NOT EXISTS condition
l1_l3_df = lineitem_df[(lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE'])]
l1_l3_df = l1_l3_df.drop_duplicates(subset=['L_ORDERKEY', 'L_SUPPKEY'])

# Merge lineitems with orders
lineitem_orders_df = filtered_lineitem.merge(filtered_orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")

# Join supplier_nation with lineitem_orders excluding orders with L2
result_df = supplier_nation_df.merge(lineitem_orders_df, left_on="S_SUPPKEY", right_on="L_SUPPKEY")
result_df = result_df[~result_df['L_ORDERKEY'].isin(l1_l3_df['L_ORDERKEY'])]

# Perform group by and count operation
final_df = result_df.groupby('S_NAME').size().reset_index(name='NUMWAIT')

# Sort results
final_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True], inplace=True)

# Output to CSV
final_df.to_csv('query_output.csv', index=False)
