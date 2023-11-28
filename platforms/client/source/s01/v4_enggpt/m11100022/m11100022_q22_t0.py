# query.py
import pandas as pd
import pymysql
from direct_redis import DirectRedis

# Connect to mysql
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Run the query on mysql to get customers
customer_query = """
SELECT *,
SUBSTR(C_PHONE, 1, 2) AS CNTRYCODE
FROM customer
WHERE C_ACCTBAL > (
    SELECT AVG(C_ACCTBAL)
    FROM customer
    WHERE C_ACCTBAL > 0 AND SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')
)
AND NOT EXISTS (
    SELECT 1
    FROM orders
    WHERE customer.C_CUSTKEY = orders.O_CUSTKEY
)
"""

with mysql_connection.cursor() as cursor:
    cursor.execute(customer_query)
    customer_data = cursor.fetchall()

# Assume we get orders data as pandas DataFrame from redis
orders_df = pd.DataFrame(redis_connection.get('orders'))

mysql_connection.close()

# Processing data to get the desired output
customer_df = pd.DataFrame(customer_data, columns=[
    'C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT', 'CNTRYCODE'])

# Exclude customers who have placed orders
customer_df = customer_df[~customer_df['C_CUSTKEY'].isin(orders_df['O_CUSTKEY'])]

# Aggregating the result
result = customer_df.groupby('CNTRYCODE').agg({'C_CUSTKEY': 'count', 'C_ACCTBAL': 'sum'}).reset_index()
result.columns = ['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL']
result.sort_values('CNTRYCODE', inplace=True)

# Save to CSV
result.to_csv('query_output.csv', index=False)
