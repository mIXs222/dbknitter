import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Establish MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

# Fetch suppliers from MySQL and store in DataFrame
supplier_query = """
SELECT S_SUPPKEY, S_NAME, S_NATIONKEY
FROM supplier
"""
supplier_df = pd.read_sql(supplier_query, mysql_conn)

# Close MySQL Connection
mysql_conn.close()

# Establish MongoDB Connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Fetch orders and lineitems from MongoDB and store in DataFrames
orders_col = mongo_db['orders']
orders_query = {'O_ORDERSTATUS': 'F'}
orders_df = pd.DataFrame(list(orders_col.find(orders_query, projection={'_id': False})))

lineitem_col = mongo_db['lineitem']
lineitem_df = pd.DataFrame(list(lineitem_col.find({}, projection={'_id': False})))

# Filter lineitems as per SQL query conditions
liningroup = lineitem_df.groupby('L_ORDERKEY').filter(lambda x: x['L_RECEIPTDATE'].gt(x['L_COMMITDATE']).any())

# Apply EXISTS condition
liningroup['EXISTS'] = liningroup.groupby('L_ORDERKEY')['L_SUPPKEY'].transform(
    lambda x: x.nunique() > 1
)

# Apply NOT EXISTS condition
liningroup['NOTEXISTS'] = liningroup.groupby('L_ORDERKEY')['L_SUPPKEY'].transform(
    lambda x: x.nunique() == 1
)

# Filter needed data
linedata = liningroup[
    (liningroup['EXISTS']) &
    (~liningroup['NOTEXISTS']) &
    (liningroup['L_RECEIPTDATE'] > liningroup['L_COMMITDATE'])
].drop_duplicates(subset=['L_ORDERKEY'])

# Merge Frames
merged_df = linedata.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY').merge(
    supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY'
)

# Establish Redis Connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load nation as DataFrame
nation_df = pd.read_pickle(redis_client.get('nation'))

# Filter for 'SAUDI ARABIA'
nation_df = nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA']

# Merge and aggregate final data
final_df = merged_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
final_df = final_df.groupby('S_NAME').size().reset_index(name='NUMWAIT')

# Sort the results
final_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True], inplace=True)

# Write to CSV
final_df.to_csv('query_output.csv', index=False)
