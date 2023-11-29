import pymysql
import pymongo
import pandas as pd
import direct_redis

# Connect to MySQL for 'part' table
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
query_part = """
SELECT P_PARTKEY, P_NAME
FROM part
WHERE P_BRAND <> 'Brand#45'
AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
"""
df_part = pd.read_sql(query_part, mysql_conn)
mysql_conn.close()

# Connect to MongoDB for 'supplier' table
client = pymongo.MongoClient('mongodb', 27017)
mongo_db = client['tpch']
supplier_collection = mongo_db['supplier']
df_supplier = pd.DataFrame(list(supplier_collection.find(
    {'$nor': [
        {'S_COMMENT': {'$regex': ".*Customer.*Complaints.*"}}
    ]}
)))

# Connect to Redis for 'partsupp' table using direct_redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
df_partsupp = pd.DataFrame(eval(redis_conn.get('partsupp').decode('utf-8')))

# Joining the dataframes into a single one for further calculation
df_part.columns = ['PS_PARTKEY', 'P_NAME']  # Renaming for the join
result_df = pd.merge(df_partsupp, df_part, on='PS_PARTKEY', how='inner')
result_df = pd.merge(result_df, df_supplier, left_on='PS_SUPPKEY', right_on='S_SUPPKEY', how='inner')

# Counting the number of suppliers by part attributes
final_result = result_df.groupby(['P_NAME']).agg({'S_SUPPKEY': 'count'}).reset_index()
final_result = final_result.rename(columns={'S_SUPPKEY': 'supplier_count'})
final_result = final_result.sort_values(by=['supplier_count', 'P_NAME'], ascending=[False, True])

# Output the result to CSV file
final_result.to_csv('query_output.csv', index=False)
