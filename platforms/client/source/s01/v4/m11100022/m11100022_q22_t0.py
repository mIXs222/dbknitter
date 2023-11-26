import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Get customers that match the criteria from MySQL
customer_query = """
SELECT
    C_CUSTKEY, SUBSTR(C_PHONE, 1 , 2) AS CNTRYCODE, C_ACCTBAL
FROM
    customer
WHERE
    SUBSTR(C_PHONE, 1 , 2) IN ('20', '40', '22', '30', '39', '42', '21')
    AND C_ACCTBAL > (
        SELECT AVG(C_ACCTBAL)
        FROM customer
        WHERE C_ACCTBAL > 0.00
        AND SUBSTR(C_PHONE, 1 , 2) IN ('20', '40', '22', '30', '39', '42', '21')
    );
"""

with mysql_connection.cursor() as cursor:
    cursor.execute(customer_query)
    customers = pd.DataFrame(cursor.fetchall(), columns=['C_CUSTKEY', 'CNTRYCODE', 'C_ACCTBAL'])

# Connect to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Retrieve orders from Redis and convert to pandas DataFrame
orders_keys_bytes = redis_connection.keys('orders:*')
orders = pd.concat(
    [pd.DataFrame.from_dict(redis_connection.get(key), orient='index').transpose() for key in orders_keys_bytes],
    ignore_index=True
)
orders.columns = ['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT']
orders['O_CUSTKEY'] = orders['O_CUSTKEY'].astype(int)  # Ensure correct data type for joining

# Filtering out customers that exist in orders
customers = customers[~customers['C_CUSTKEY'].isin(orders['O_CUSTKEY'])]

# Grouping and aggregating as per the SQL query
result = customers.groupby('CNTRYCODE').agg(NUMCUST=('CNTRYCODE', 'count'), TOTACCTBAL=('C_ACCTBAL', 'sum')).reset_index()
result.sort_values(by='CNTRYCODE', inplace=True)

# Write result to file
result.to_csv('query_output.csv', index=False)

# Clean up connections
mysql_connection.close()
