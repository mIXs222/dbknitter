import pymysql
import pymongo
import pandas as pd
from pandas.io.json import json_normalize
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Run Query parts separately due to different databases

# MySQL: Select parts that satisfy the condition
mysql_cursor.execute(
    "SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%'"
)
part_keys = [row[0] for row in mysql_cursor.fetchall()]

# MongoDB: Select partsupp entries satisfying the above part keys
partsupp_df = json_normalize(
    mongo_db.partsupp.find(
        {"PS_PARTKEY": {"$in": part_keys}},
        {"_id": False, "PS_SUPPKEY": True, "PS_AVAILQTY": True, "PS_PARTKEY": True }
    )
)

# Get the aggregated quantity for each part from Redis
lineitem_keys = redis_client.get('lineitem')
lineitem_df = pd.read_json(lineitem_keys, orient='records')
lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1994-01-01') &
    (lineitem_df['L_SHIPDATE'] < '1995-01-01')
]

# Calculate 0.5 * sum(L_QUANTITY) grouped by L_PARTKEY and L_SUPPKEY
sum_quantity_df = lineitem_df.groupby(['L_PARTKEY', 'L_SUPPKEY']).agg({'L_QUANTITY': 'sum'}).reset_index()
sum_quantity_df['L_QUANTITY'] = sum_quantity_df['L_QUANTITY'] * 0.5

# Filter partsupp entries based on the availability and aggregate quantity
partsupp_df = pd.merge(partsupp_df, sum_quantity_df, how='left', left_on=['PS_PARTKEY', 'PS_SUPPKEY'], right_on=['L_PARTKEY', 'L_SUPPKEY'])
partsupp_df = partsupp_df[partsupp_df['PS_AVAILQTY'] > partsupp_df['L_QUANTITY']]

# Select supplier for filtered partsupp records
supplier_df = json_normalize(
    mongo_db.supplier.find(
        {"S_SUPPKEY": {"$in": partsupp_df['PS_SUPPKEY'].tolist()}},
        {"_id": False, "S_SUPPKEY": True, "S_NAME": True, "S_ADDRESS": True, "S_NATIONKEY": True}
    )
)

# MySQL: Select CANADA nations
mysql_cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'CANADA'")
canada_nationkey = mysql_cursor.fetchone()[0]

# Filter suppliers by nation key (CANADA)
supplier_df = supplier_df[supplier_df['S_NATIONKEY'] == canada_nationkey]

# Arrange result and write to CSV
final_result = supplier_df[['S_NAME', 'S_ADDRESS']].sort_values('S_NAME')
final_result.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
