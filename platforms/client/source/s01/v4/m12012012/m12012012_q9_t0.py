import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute("SELECT * FROM part WHERE P_NAME LIKE '%dim%'")
parts = pd.DataFrame(mysql_cursor.fetchall(), columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
nations = pd.DataFrame(list(mongodb_db['nation'].find()))
suppliers = pd.DataFrame(list(mongodb_db['supplier'].find()))
orders = pd.DataFrame(list(mongodb_db['orders'].find({}, {'_id': 0})))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
partsupp_df = pd.read_json(redis_client.get('partsupp'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Close databases connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()

# Data Preprocessing
# Convert order date to datetime
orders['O_ORDERDATE'] = pd.to_datetime(orders['O_ORDERDATE'])

# Merge the data
merged_df = partsupp_df.merge(lineitem_df, how='inner', left_on=['PS_PARTKEY', 'PS_SUPPKEY'], right_on=['L_PARTKEY', 'L_SUPPKEY'])
merged_df = merged_df.merge(parts, how='inner', left_on='PS_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(suppliers, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df.merge(nations, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Creating AMOUNT column
merged_df['AMOUNT'] = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])) - (merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY'])

# Group by and sort
result_df = merged_df.groupby(['N_NAME', merged_df['O_ORDERDATE'].dt.year]).agg(SUM_PROFIT=('AMOUNT', 'sum')).reset_index()
result_df.rename(columns={'N_NAME': 'NATION', 'O_ORDERDATE': 'O_YEAR'}, inplace=True)
result_df.sort_values(by=['NATION', 'O_YEAR'], ascending=[True, False], inplace=True)

# Save to CSV
result_df.to_csv('query_output.csv', index=False)
