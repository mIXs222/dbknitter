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

# Connect to MongoDB
mongodb_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb = mongodb_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve 'part' table from MongoDB where P_NAME like 'forest%'
part_collection = mongodb['part']
part_cursor = part_collection.find({'P_NAME': {'$regex': '^forest'}})
part_df = pd.DataFrame(list(part_cursor))

# Retrieve 'partsupp' table from Redis
partsupp_df = pd.read_json(redis_client.get('partsupp'))
# Filter part records retrieved from MongoDB in partsupp
partsupp_df = partsupp_df[partsupp_df['PS_PARTKEY'].isin(part_df['P_PARTKEY'])]

# Retrieve 'lineitem' table from Redis
lineitem_df = pd.read_json(redis_client.get('lineitem'))
# Filter based on partsupp keys and date
lineitem_df = lineitem_df[(lineitem_df['L_PARTKEY'].isin(partsupp_df['PS_PARTKEY'])) &
                          (lineitem_df['L_SUPPKEY'].isin(partsupp_df['PS_SUPPKEY'])) &
                          (lineitem_df['L_SHIPDATE'] >= '1994-01-01') &
                          (lineitem_df['L_SHIPDATE'] < '1995-01-01')]

# Calculate the sum of L_QUANTITY grouped by L_PARTKEY and L_SUPPKEY
lineitem_qty_df = lineitem_df.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum().reset_index()
lineitem_qty_df['L_QUANTITY'] = lineitem_qty_df['L_QUANTITY'] * 0.5

# Filter partsupp records based on the quantity from lineitem
partsupp_df = partsupp_df.merge(lineitem_qty_df, how='left', left_on=['PS_PARTKEY', 'PS_SUPPKEY'], right_on=['L_PARTKEY', 'L_SUPPKEY'])
partsupp_df = partsupp_df[partsupp_df['PS_AVAILQTY'] > partsupp_df['L_QUANTITY']]

# Get the list of supplier keys after filtering
supplier_keys = partsupp_df['PS_SUPPKEY']

# MySQL query
supplier_query = """
SELECT
    S_NAME,
    S_ADDRESS
FROM
    supplier,
    nation
WHERE
    S_SUPPKEY IN %s
    AND S_NATIONKEY = N_NATIONKEY
    AND N_NAME = 'CANADA'
ORDER BY
    S_NAME
"""

# Execute MySQL query
with mysql_conn.cursor() as cursor:
    cursor.execute(supplier_query, (tuple(supplier_keys),))
    supplier_result = cursor.fetchall()

# Create DataFrame from the MySQL result
supplier_df = pd.DataFrame(supplier_result, columns=['S_NAME', 'S_ADDRESS'])

# Write the final result to CSV file
supplier_df.to_csv('query_output.csv', index=False)

# Close all connections
mysql_conn.close()
mongodb_client.close()
redis_client.close()
