import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_con = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

# Query to extract relevant data from 'supplier' table in MySQL
supplier_query = """
    SELECT S_SUPPKEY, S_COMMENT
    FROM supplier
    WHERE S_COMMENT NOT LIKE '%Customer Complaints%'
"""
supplier_df = pd.read_sql(supplier_query, mysql_con)
mysql_con.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Extract relevant data from 'partsupp' collection in MongoDB
partsupp_cursor = mongodb.partsupp.find({},
                                        {'PS_PARTKEY': 1, 'PS_SUPPKEY': 1, '_id': 0})
partsupp_df = pd.DataFrame(list(partsupp_cursor))

# Connect to Redis
redis_con = DirectRedis(host='redis', port=6379, db=0)

# Read 'part' table as a pandas DataFrame
part_df = pd.DataFrame(redis_con.get('part'))

# Filter part_df according to the specified conditions
filtered_part_df = part_df[
    (~part_df['P_BRAND'].eq('Brand#45')) &
    (~part_df['P_TYPE'].str.startswith('MEDIUM POLISHED')) &
    (part_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))
]

# Merge dataframes to create a combined dataset
merged_df = partsupp_df.merge(filtered_part_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Group by brand, type, and size then count distinct suppliers
result_df = merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg(SUPPLIER_CNT=('S_SUPPKEY', 'nunique')).reset_index()

# Sort the results as specified
final_df = result_df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Save to CSV
final_df.to_csv('query_output.csv', index=False)
