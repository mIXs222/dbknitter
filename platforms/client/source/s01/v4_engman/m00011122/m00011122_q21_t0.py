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

# Execute MySQL Query to get Nation Key for SAUDI ARABIA
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'SAUDI ARABIA'")
    nation_key_result = cursor.fetchone()
    saudi_arabia_nation_key = nation_key_result[0]

mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Query MongoDB for suppliers from SAUDI ARABIA
suppliers_saudi_arabia = list(mongodb.supplier.find({'S_NATIONKEY': saudi_arabia_nation_key}, {'_id': 0, 'S_SUPPKEY': 1, 'S_NAME': 1}))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get orders with status 'F' from Redis
orders_df = pd.read_json(redis_client.get('orders') or '[]')
orders_with_f = orders_df[orders_df['O_ORDERSTATUS'] == 'F']

# Get lineitem dataframe from Redis
lineitem_df = pd.read_json(redis_client.get('lineitem') or '[]')

# Join and process data
joined_df = pd.merge(lineitem_df, orders_with_f, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
filtered_lineitem = joined_df[joined_df['L_COMMITDATE'] < joined_df['L_RECEIPTDATE']]

filtered_lineitem['NUMWAIT'] = filtered_lineitem.groupby('L_ORDERKEY')['L_ORDERKEY'].transform('count')
filtered_suppliers_lineitem = pd.merge(pd.DataFrame(suppliers_saudi_arabia), filtered_lineitem, how='inner', left_on='S_SUPPKEY', right_on='L_SUPPKEY')

results = filtered_suppliers_lineitem.groupby(['S_NAME'])['NUMWAIT'].max().reset_index()
results = results.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write to CSV
results.to_csv('query_output.csv', index=False)
