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
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# MySQL Query
mysql_query = """
    SELECT 
        l.L_SUPPKEY, 
        s.S_NAME, 
        SUM(l.L_QUANTITY) AS total_quantity
    FROM 
        lineitem l
        INNER JOIN supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
    WHERE 
        l.L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
    GROUP BY 
        l.L_SUPPKEY
    HAVING 
        total_quantity > 0.5 * (SELECT SUM(L_QUANTITY) FROM lineitem WHERE L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01')
"""

# Retrieve data from MySQL
df_mysql = pd.read_sql(mysql_query, con=mysql_conn)

# Retrieve data from MongoDB
part_docs = mongo_db['part'].find({"P_NAME": {"$regex": "^.*forest.*$"}})
df_mongo = pd.DataFrame(list(part_docs))

# Merge MySQL and MongoDB data based on part keys
df_merged = pd.merge(df_mysql, df_mongo, left_on='L_SUPPKEY', right_on='P_PARTKEY')

# Retrieve data from Redis
supplier_data = redis_client.get('supplier')
partsupp_data = redis_client.get('partsupp')

# Convert JSON strings from Redis to Pandas DataFrames
df_supplier = pd.read_json(supplier_data)
df_partsupp = pd.read_json(partsupp_data)

# Merge Redis data with previous data based on supplier keys
final_df = df_merged.merge(df_supplier, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Filter based on the naming convention for "forest" parts and excess quantity condition
final_df = final_df[(final_df['P_NAME'].str.contains('forest', case=False)) & (final_df['total_quantity'] > 50)]

# Write the final DataFrame to a CSV file
final_df.to_csv('query_output.csv', index=False)

# Close the MySQL connection
mysql_conn.close()
