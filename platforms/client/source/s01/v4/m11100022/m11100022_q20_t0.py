import pandas as pd
import pymysql
import pymongo
import pickle
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cur = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MySQL
mysql_cur.execute("SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY FROM supplier WHERE S_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'CANADA')")
suppliers = pd.DataFrame(mysql_cur.fetchall(), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY'])

# Retrieve data from MongoDB
part_cursor = mongo_db['part'].find({'P_NAME': {'$regex': '^forest'}}, {'P_PARTKEY': 1})
part_keys = [doc['P_PARTKEY'] for doc in part_cursor]

# Retrieve data from Redis
lineitems = pickle.loads(redis_client.get('lineitem'))
lineitems_df = pd.DataFrame(lineitems)
lineitems_df = lineitems_df[(lineitems_df['L_SHIPDATE'] >= '1994-01-01') & (lineitems_df['L_SHIPDATE'] < '1995-01-01')]

# Get the AVG quantity of each (P_PARTKEY, S_SUPPKEY) from lineitems
avg_qty = lineitems_df.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum() * 0.5

# Execute query for PARTSUPP in MySQL
mysql_cur.execute("SELECT PS_SUPPKEY, PS_PARTKEY FROM partsupp")
partsupp = pd.DataFrame(mysql_cur.fetchall(), columns=['PS_SUPPKEY', 'PS_PARTKEY'])
partsupp = partsupp[partsupp['PS_PARTKEY'].isin(part_keys)]

# Merge dataframes to filter the suppliers
merged_df = pd.merge(left=suppliers, right=partsupp, how='inner', left_on='S_SUPPKEY', right_on='PS_SUPPKEY')
merged_df = merged_df.join(avg_qty, on=['PS_PARTKEY', 'S_SUPPKEY'], rsuffix='_AVG')
merged_df = merged_df[merged_df['L_QUANTITY'] <= merged_df['PS_AVAILQTY']]

# Select the required columns and sort by S_NAME
output_df = merged_df[['S_NAME', 'S_ADDRESS']]
output_df = output_df.sort_values('S_NAME')

# Save to CSV file
output_df.to_csv('query_output.csv', index=False)

# Close all connections
mysql_cur.close()
mysql_conn.close()
mongo_client.close()
