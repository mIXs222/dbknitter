import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Establish a connection to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Query the MySQL database
with mysql_conn.cursor() as cursor:
    mysql_query = """
    SELECT
        P_PARTKEY,
        P_BRAND,
        P_TYPE,
        P_SIZE
    FROM
        part
    WHERE
        P_BRAND <> 'Brand#45'
        AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
        AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
    """
    cursor.execute(mysql_query)
    parts_data = cursor.fetchall()

# Convert the MySQL data to a DataFrame
parts_df = pd.DataFrame(parts_data, columns=['P_PARTKEY', 'P_BRAND', 'P_TYPE', 'P_SIZE'])

# Establish a connection to the MongoDB database
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
partsupp_collection = mongodb_db['partsupp']

# Query the MongoDB database
parts_sups = partsupp_collection.find({}, {'PS_PARTKEY': 1, 'PS_SUPPKEY': 1, '_id': 0})
parts_sups_df = pd.DataFrame(list(parts_sups))

# Establish a connection to the Redis database
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query the Redis database
supplier_keys = redis_client.keys('supplier:*')
suppliers_with_complaints = set()

for key in supplier_keys:
    supplier_data = redis_client.hgetall(key)
    if 'Customer Complaints' in str(supplier_data[b'S_COMMENT']):
        suppliers_with_complaints.add(int(supplier_data[b'S_SUPPKEY']))

# Merge and process the data from MySQL and MongoDB
combined_df = pd.merge(parts_df, parts_sups_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Filter out suppliers with complaints
combined_df = combined_df[~combined_df['PS_SUPPKEY'].isin(suppliers_with_complaints)]

# Group and sort the results according to the SQL query
result_df = combined_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']) \
    .agg(SUPPLIER_CNT=('PS_SUPPKEY', 'nunique')) \
    .reset_index() \
    .sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write the results to a CSV file
result_df.to_csv('query_output.csv', index=False)
