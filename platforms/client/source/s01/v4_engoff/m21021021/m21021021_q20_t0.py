# Import the required libraries
import pymysql
import pymongo
from direct_redis import DirectRedis
import pandas as pd
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    user='root',
    password='my-secret-pw',
    host='mysql',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL for parts similar to 'forest'
mysql_query = '''
SELECT P_PARTKEY, P_NAME
FROM part
WHERE P_NAME LIKE '%forest%'
'''
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    parts_forest = cursor.fetchall()

# Convert parts_forest to DataFrame
parts_forest_df = pd.DataFrame(parts_forest, columns=['P_PARTKEY', 'P_NAME'])

# Query MongoDB for lineitem and partsupp
pipeline = [
    {"$match": {"L_SHIPDATE": {"$gte": datetime(1994, 1, 1), "$lt": datetime(1995, 1, 1)}}},
    {"$group": {"_id": "$L_PARTKEY", "total_qty": {"$sum": "$L_QUANTITY"}}}
]
lineitem_total_qty = list(mongo_db['lineitem'].aggregate(pipeline))
lineitem_total_qty_df = pd.DataFrame(lineitem_total_qty)
lineitem_total_qty_df.rename(columns={'_id': 'L_PARTKEY', 'total_qty': 'total_shipped_qty'}, inplace=True)

# Query MongoDB for partsupp records and convert to DataFrame
partsupp_records = list(mongo_db['partsupp'].find({}, {'_id': 0}))
partsupp_df = pd.DataFrame(partsupp_records)

# Get supplier keys for CANADA from Redis and create dataframe
nation_df = pd.read_json(redis_client.get('nation'))
supplier_df = pd.read_json(redis_client.get('supplier'))
canada_suppliers = supplier_df[supplier_df['S_NATIONKEY'] == nation_df[nation_df['N_NAME'] == 'CANADA']['N_NATIONKEY'].values[0]]

# Join DataFrames to compute the final result
merged_df = parts_forest_df \
    .merge(lineitem_total_qty_df, left_on='P_PARTKEY', right_on='L_PARTKEY') \
    .merge(partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Filtering suppliers with excess of forest parts
merged_df['excess'] = (merged_df['total_shipped_qty'] > (merged_df['PS_AVAILQTY'] * 1.5))
final_result = merged_df[merged_df['excess']].merge(canada_suppliers, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Selecting only relevant columns for output
final_output = final_result[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT', 'N_NAME']]

# Save to CSV
final_output.to_csv('query_output.csv', index=False)

# Close the database connection
mysql_conn.close()
mongo_client.close()
redis_client.connection_pool.disconnect()
