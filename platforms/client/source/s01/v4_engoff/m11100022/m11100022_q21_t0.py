import pandas as pd
import pymysql
import pymongo
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_nation = mongo_db['nation']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve nation key for 'SAUDI ARABIA' from MongoDB
nation_query = {'N_NAME': 'SAUDI ARABIA'}
nation_docs = mongo_nation.find(nation_query)
for doc in nation_docs:
    saudi_nationkey = doc['N_NATIONKEY']

# Retrieve suppliers from MySQL for the identified nation
supplier_query = f"""
SELECT
    S_SUPPKEY, S_NAME
FROM
    supplier
WHERE
    S_NATIONKEY = {saudi_nationkey}
"""
mysql_cursor.execute(supplier_query)
suppliers_data = mysql_cursor.fetchall()
supplier_df = pd.DataFrame(suppliers_data, columns=['S_SUPPKEY', 'S_NAME'])

# Retrieve orders with 'F' status from Redis
orders_df = pd.read_json(redis_client.get('orders'), orient='records')
orders_f_df = orders_df[orders_df['O_ORDERSTATUS'] == 'F']

# Retrieve lineitem data from Redis
lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])

# Merge and filter data to identify suppliers who kept orders waiting
waiting_suppliers_df = (
    supplier_df
    .merge(lineitem_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
    .merge(orders_f_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
)
waiting_suppliers_df = waiting_suppliers_df[
    (waiting_suppliers_df['L_RECEIPTDATE'] > waiting_suppliers_df['L_COMMITDATE']) &
    (waiting_suppliers_df['S_NATIONKEY'] == saudi_nationkey)
]

# Group by supplier and order to identify multi-supplier orders
multi_supplier_orders = (
    lineitem_df
    .groupby('L_ORDERKEY')
    .filter(lambda x: len(x['L_SUPPKEY'].unique()) > 1)
    ['L_ORDERKEY'].unique()
)

# Filter out suppliers who were the only to fail in a multi-supplier order
result_df = waiting_suppliers_df[
    waiting_suppliers_df['L_ORDERKEY'].isin(multi_supplier_orders)
].drop_duplicates(subset=['S_SUPPKEY'])

# Write the output to CSV
result_df[['S_SUPPKEY', 'S_NAME']].to_csv('query_output.csv', index=False)

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
