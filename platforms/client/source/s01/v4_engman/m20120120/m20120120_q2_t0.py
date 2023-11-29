import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL Connection
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB Connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis Connection
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Execute query on MySQL and Redis databases
with mysql_connection.cursor() as mysql_cursor:
    mysql_cursor.execute(
        """
        SELECT r.R_REGIONKEY, ps.PS_PARTKEY, ps.PS_SUPPKEY, ps.PS_SUPPLYCOST
        FROM region r
        JOIN nation n ON n.N_REGIONKEY = r.R_REGIONKEY
        JOIN supplier s ON s.S_NATIONKEY = n.N_NATIONKEY
        JOIN partsupp ps ON ps.PS_SUPPKEY = s.S_SUPPKEY
        WHERE r.R_NAME = 'EUROPE'
        """
    )
    mysql_data = mysql_cursor.fetchall()

supplier_details = pd.DataFrame(redis_connection.get('supplier'), columns=[
    'S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'
])

nation_details = pd.DataFrame(redis_connection.get('nation'), columns=[
    'N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'
])

# Convert MySQL data to DataFrame
mysql_df = pd.DataFrame(mysql_data, columns=[
    'R_REGIONKEY', 'PS_PARTKEY', 'PS_SUPPKEY', 'PS_SUPPLYCOST'
])

# Get MongoDB 'part' table data
mongo_part_data = mongo_db.part.find(
    {'P_TYPE': 'BRASS', 'P_SIZE': 15},
    {'P_PARTKEY': 1, 'P_MFGR': 1, 'P_NAME': 1}
)
part_details = pd.DataFrame(list(mongo_part_data))

# Combine all the data from different sources
combined_df = mysql_df.merge(part_details, how='inner', left_on='PS_PARTKEY', right_on='P_PARTKEY')
combined_df = combined_df.merge(supplier_details, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
combined_df = combined_df.merge(nation_details, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Apply the conditions from the query and sort the data
query_result = combined_df.loc[combined_df['PS_SUPPLYCOST'] == combined_df.groupby('PS_PARTKEY')['PS_SUPPLYCOST'].transform('min')]
query_result = query_result.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Only select the columns necessary for the output
output_columns = ['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']
final_output = query_result[output_columns]

# Write to CSV file
final_output.to_csv('query_output.csv', index=False)

# Close all connections
mysql_connection.close()
mongo_client.close()
redis_connection.close()
