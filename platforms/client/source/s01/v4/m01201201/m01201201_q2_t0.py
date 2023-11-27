import csv
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MySQL
mysql_cursor.execute("""
    SELECT S_SUPPKEY, S_ACCTBAL, S_NAME, S_ADDRESS, S_PHONE, S_COMMENT, N_NAME
    FROM supplier JOIN nation ON S_NATIONKEY = N_NATIONKEY
""")
suppliers = pd.DataFrame(mysql_cursor.fetchall(), columns=[
    'S_SUPPKEY', 'S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'N_NAME'
])

# Retrieve data from MongoDB
regions = pd.DataFrame(list(mongodb.region.find({'R_NAME': 'EUROPE'})))
partsupp = pd.DataFrame(list(mongodb.partsupp.find()))
partsupp_min_supplycost = partsupp[partsupp.PS_SUPPLYCOST == partsupp.PS_SUPPLYCOST.min()]

# Retrieve data from Redis
parts_raw = redis_client.get('part')
parts = pd.read_json(parts_raw.decode("utf-8"))
parts_filtered = parts[(parts.P_SIZE == 15) & (parts.P_TYPE.str.contains("BRASS"))]

# Join the data from MySQL and MongoDB
joined_data = pd.merge(suppliers, partsupp_min_supplycost, how='inner', left_on='S_SUPPKEY', right_on='PS_SUPPKEY')
joined_data = joined_data[joined_data.N_NAME.isin(regions.R_NAME)]

# Now join the data from Redis
final_result = pd.merge(joined_data, parts_filtered, how='inner', left_on='S_SUPPKEY', right_on='P_PARTKEY')

# Sort the result as per the given order
final_result_sorted = final_result.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Select the required columns for the final output
output_columns = [
    'S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT'
]
output_df = final_result_sorted[output_columns]

# Write the output to a CSV file
output_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_MINIMAL)

# Close the MySQL connection
mysql_conn.close()
