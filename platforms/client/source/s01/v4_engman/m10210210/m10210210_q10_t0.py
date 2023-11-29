import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get data from Redis (Customer)
customer_data = pd.read_json(redis_client.get('customer'))

# Get data from MongoDB (Nation and Orders)
orders_collection = mongo_db['orders']
nation_collection = mongo_db['nation']
orders_data = pd.DataFrame(list(orders_collection.find()))
nation_data = pd.DataFrame(list(nation_collection.find()))

# MySQL query to fetch relevant lineitem data
lineitem_query = """
SELECT
    L_ORDERKEY,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue_lost
FROM
    lineitem
WHERE
    L_SHIPDATE >= '1993-10-01'
    AND L_SHIPDATE < '1994-01-01'
GROUP BY L_ORDERKEY;
"""
mysql_cursor.execute(lineitem_query)
lineitem_data = pd.DataFrame(mysql_cursor.fetchall(), columns=['L_ORDERKEY', 'revenue_lost'])

# Merge data from different databases
merged_data = (lineitem_data
               .merge(orders_data, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
               .merge(customer_data, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
               .merge(nation_data, left_on='C_NATIONKEY', right_on='N_NATIONKEY'))

# Select and sort the final output
output_data = (merged_data[['C_CUSTKEY', 'C_NAME', 'revenue_lost', 'C_ACCTBAL', 'N_NAME',
                            'C_ADDRESS', 'C_PHONE', 'C_COMMENT']]
               .sort_values(by=['revenue_lost', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'],
                            ascending=[True, True, True, False]))

# Write to CSV
output_data.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
