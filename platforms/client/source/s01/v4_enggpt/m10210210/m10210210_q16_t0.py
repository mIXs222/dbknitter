import pymysql
import pymongo
import pandas as pd
from redis.exceptions import DataError
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
cur = mysql_conn.cursor()
cur.execute("""SELECT PS_PARTKEY, PS_SUPPKEY
               FROM partsupp""")
partsupp_data = cur.fetchall()
cur.close()
mysql_conn.close()
df_partsupp = pd.DataFrame(partsupp_data, columns=['PS_PARTKEY', 'PS_SUPPKEY'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
supplier_collection = mongo_db['supplier']
supplier_data = supplier_collection.find(
    {"S_COMMENT": {'$not': {'$regex': '.*Customer Complaints.*'}}},
    {'S_SUPPKEY': 1}
)
df_supplier = pd.DataFrame(list(supplier_data))

# Try to get data from Redis
try:
    import direct_redis
    redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    df_part = redis_conn.get('part')
except DataError:
    df_part = pd.DataFrame()  # if data not present or error occurs

# Filtering and combining the data
if not df_part.empty:
    df_part_filtered = df_part[
        (df_part['P_BRAND'] != 'Brand#45') &
        (~df_part['P_TYPE'].str.startswith('MEDIUM POLISHED')) &
        (df_part['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))
    ]

    # Merge DataFrames on part key
    df = pd.merge(df_part_filtered, df_partsupp, left_on='P_PARTKEY', right_on='PS_PARTKEY')

    # Further merge with supplier data on supplier key
    df_final = pd.merge(df, df_supplier, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

    # Calculate the count of distinct suppliers
    df_grouped = df_final.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg(SUPPLIER_CNT=('S_SUPPKEY', 'nunique')).reset_index()

    # Sort the results
    df_sorted = df_grouped.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

    # Output the result to a CSV file
    df_sorted.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

else:
    print("No data found in one of the sources.")

