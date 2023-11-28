import pandas as pd
import pymysql
from direct_redis import DirectRedis
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Prepare and execute the query for MySQL
try:
    with mysql_conn.cursor() as cursor:
        cursor.execute("""
            SELECT 
                SUBSTR(C_PHONE, 1, 2) AS CNTRYCODE,
                COUNT(*) AS NUMCUST,
                SUM(C_ACCTBAL) AS TOTACCTBAL
            FROM customer
            WHERE C_ACCTBAL > (
                SELECT AVG(C_ACCTBAL)
                FROM customer
                WHERE C_ACCTBAL > 0 AND SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')
            )
            AND SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')
            AND NOT EXISTS (
                SELECT 1
                FROM orders
                WHERE orders.O_CUSTKEY = customer.C_CUSTKEY
            )
            GROUP BY CNTRYCODE
            ORDER BY CNTRYCODE ASC;
        """)
        mysql_results = cursor.fetchall()
finally:
    mysql_conn.close()

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379)

# Get order data from Redis as DataFrame
redis_orders = pd.DataFrame.from_records(
    eval(redis_conn.get('orders')),
    columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT']
)

# Transform MySQL results into DataFrame
mysql_df = pd.DataFrame(mysql_results, columns=['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL'])

# Merge results
merged_df = mysql_df[~mysql_df.C_CUSTKEY.isin(redis_orders.O_CUSTKEY)]

# Write output to CSV
merged_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
