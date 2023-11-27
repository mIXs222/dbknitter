import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch',
                                   cursorclass=pymysql.cursors.Cursor)

# Retrieve customers from MySQL database
with mysql_connection.cursor() as cursor:
    cursor.execute("SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_PHONE, C_ACCTBAL, C_COMMENT "
                   "FROM customer")
    customers_data = cursor.fetchall()
    customers_df = pd.DataFrame(customers_data, columns=['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT'])

mysql_connection.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongodb = mongo_client['tpch']

# Retrieve line items from MongoDB database
lineitem_data = list(mongodb.lineitem.find({
    'L_RETURNFLAG': 'R',
    'L_SHIPDATE': {'$gte': '1993-10-01', '$lt': '1994-01-01'}
}))
lineitem_df = pd.DataFrame(lineitem_data)

# Connect to Redis
redis_connection = DirectRedis(port=6379, host='redis')

# Retrieve nations from Redis database
nation_df = pd.read_json(redis_connection.get('nation'), orient='records')

# Prepare the final query result
query_result = pd.merge(customers_df, lineitem_df, left_on='C_CUSTKEY', right_on='L_ORDERKEY')
query_result = pd.merge(query_result, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY', how='left')
query_result['REVENUE_LOST'] = query_result['L_EXTENDEDPRICE'] * (1 - query_result['L_DISCOUNT'])
query_result = query_result.groupby(['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT', 'N_NAME']) \
    .agg({"REVENUE_LOST": "sum"}) \
    .reset_index() \
    .sort_values(by=['REVENUE_LOST', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, True])

# Write output to csv
query_result.to_csv('query_output.csv', index=False)
