# Required Libraries
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL Connection
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

# MongoDB Connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Redis Connection
redis = DirectRedis(host='redis', port=6379, db=0)

# Get data from MySQL
with mysql_connection.cursor() as cursor:
    cursor.execute("""
        SELECT
            O_CUSTKEY,
            O_ORDERKEY,
            SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
        FROM
            orders INNER JOIN lineitem ON L_ORDERKEY = O_ORDERKEY
        WHERE
            O_ORDERDATE >= '1993-10-01'
            AND O_ORDERDATE < '1994-01-01'
            AND L_RETURNFLAG = 'R'
        GROUP BY
            O_CUSTKEY,
            O_ORDERKEY
        """)
    order_lineitem_results = cursor.fetchall()

# Create a DataFrame from the MySQL result
order_revenue_df = pd.DataFrame(order_lineitem_results, columns=['C_CUSTKEY', 'O_ORDERKEY', 'REVENUE'])

# Get the 'customer' collection from MongoDB
customer_cursor = mongodb['customer'].find({})
# Create a DataFrame from MongoDB customers collection
customer_df = pd.DataFrame(list(customer_cursor))

# Get the 'nation' table from Redis
nation_df = pd.read_json(redis.get('nation').decode('utf-8'))

# Combining the data
combined_df = pd.merge(order_revenue_df, customer_df, on='C_CUSTKEY')
combined_df = pd.merge(combined_df, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Final select with renamed columns
result = combined_df[['C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']]
result.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, False], inplace=True)

# Write the final result to CSV
result.to_csv('query_output.csv', index=False)

# Close all connections
mysql_connection.close()
mongo_client.close()
