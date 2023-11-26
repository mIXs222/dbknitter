import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')
try:
    with connection.cursor() as cursor:
        # Execute the MySQL part of the query
        cursor.execute("""
            SELECT
                O_CUSTKEY,
                O_ORDERKEY,
                O_ORDERDATE,
                O_TOTALPRICE,
                SUM(L_QUANTITY)
            FROM
                orders
            JOIN
                lineitem ON O_ORDERKEY = L_ORDERKEY
            GROUP BY
                O_CUSTKEY,
                O_ORDERKEY,
                O_ORDERDATE,
                O_TOTALPRICE
            HAVING
                SUM(L_QUANTITY) > 300
            """)
        mysql_result = cursor.fetchall()
finally:
    connection.close()

# Put result into DataFrame
mysql_df = pd.DataFrame(mysql_result, columns=['C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'SUM_QUANTITY'])

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get customer data from Redis and convert to DataFrame
customer_data = redis_client.get('customer')
customer_df = pd.read_json(customer_data)

# Merge MySQL and Redis data, filtering to match the original SQL query
merged_df = pd.merge(customer_df, mysql_df, on='C_CUSTKEY')

# Sort by O_TOTALPRICE DESC, O_ORDERDATE
merged_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)

# Selecting relevant columns
result_df = merged_df[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'SUM_QUANTITY']]

# Write output to CSV
result_df.to_csv('query_output.csv', index=False)
