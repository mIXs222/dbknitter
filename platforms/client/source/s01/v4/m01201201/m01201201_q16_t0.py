# import required libraries
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Define the subquery to exclude suppliers with specified comments
subquery = "SELECT S_SUPPKEY FROM supplier WHERE S_COMMENT LIKE '%Customer%Complaints%'"

# Perform the supplier query
supplier_exc_query = pd.read_sql(subquery, mysql_conn)
supplier_exc_list = supplier_exc_query['S_SUPPKEY'].tolist()

# Close the MySQL connection
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
partsupp_collection = mongo_db['partsupp']

# Perform the partsupp query
partsupp_cursor = partsupp_collection.find(
    {
        'PS_SUPPLYCOST': {'$not': {'$eq': None}},
        'PS_PARTKEY': {'$not': {'$eq': None}},
        'PS_SUPPKEY': {'$nin': supplier_exc_list}
    },
    {
        'PS_PARTKEY': 1, 'PS_SUPPKEY': 1, 'PS_AVAILQTY': 1,
        'PS_SUPPLYCOST': 1, 'PS_COMMENT': 1, '_id': 0
    }
)
partsupp_df = pd.DataFrame(list(partsupp_cursor))

# Close the MongoDB connection
mongo_client.close()

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Perform the part query
part_df = pd.read_json(redis_client.get('part'))

# Filter the dataframe based on the conditions
filtered_part_df = part_df[
    (~part_df['P_BRAND'].eq('Brand#45')) &
    (~part_df['P_TYPE'].str.startswith('MEDIUM POLISHED')) &
    (part_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))
]

# Join the DataFrames and perform the final group and aggregation
result_df = (
    partsupp_df.merge(filtered_part_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')
    .loc[partsupp_df['PS_SUPPKEY'].isin(supplier_exc_list) == False]
    .groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])
    .agg(SUPPLIER_CNT=('PS_SUPPKEY', 'nunique'))
    .reset_index()
    .sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])
)

# Write the query's output to a CSV file
result_df.to_csv('query_output.csv', index=False)
