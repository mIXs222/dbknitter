import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# MySQL Query to get parts starting with 'forest'
with mysql_conn.cursor() as cursor:
    part_query = "SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%'"
    cursor.execute(part_query)
    part_keys = cursor.fetchall()
part_keys = [p[0] for p in part_keys]

# MongoDB Query to get partsupp for the parts obtained from MySQL
partsupp_df = pd.DataFrame(list(mongo_db.partsupp.find({'PS_PARTKEY': {'$in': part_keys}})))

# Subquery equivalent in Pandas for third nested query
start_date = datetime.strptime('1994-01-01', '%Y-%m-%d')
end_date = datetime.strptime('1995-01-01', '%Y-%m-%d')
lineitem_df = pd.DataFrame(list(mongo_db.lineitem.find({
    'L_PARTKEY': {'$in': part_keys},
    'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}
})))

# Calculate 50% of the sum of line item quantities for each part-supplier combination
threshold_df = lineitem_df.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum().reset_index()
threshold_df['L_QUANTITY'] = 0.5 * threshold_df['L_QUANTITY']

# Filter partsupp based on the threshold quantity condition
partsupp_df = partsupp_df.merge(threshold_df, left_on=['PS_PARTKEY', 'PS_SUPPKEY'], right_on=['L_PARTKEY', 'L_SUPPKEY'])
partsupp_df = partsupp_df[partsupp_df['PS_AVAILQTY'] > partsupp_df['L_QUANTITY']]

supplier_keys = partsupp_df['PS_SUPPKEY'].unique().tolist()

# Get nation data from Redis
nation_df = pd.read_json(redis_client.get('nation'))

# Filter nation for 'CANADA'
canada_nationkey = nation_df[nation_df['N_NAME'] == 'CANADA']['N_NATIONKEY'].iloc[0]

# Get supplier data from Redis
supplier_df = pd.read_json(redis_client.get('supplier'))

# Filter suppliers based on the conditions
supplier_df = supplier_df[supplier_df['S_NATIONKEY'] == canada_nationkey]
supplier_df = supplier_df[supplier_df['S_SUPPKEY'].isin(supplier_keys)]

# Select required columns and sort by name
output_df = supplier_df[['S_NAME', 'S_ADDRESS']].sort_values('S_NAME')

# Write result to CSV
output_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
redis_client.connection_pool.disconnect()
