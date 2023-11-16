import pymysql
import csv
from decimal import Decimal

# Connection information
mysql_connection_info = {
    "database": "tpch",
    "user": "root",
    "password": "my-secret-pw",
    "host": "mysql"
}

# Connect to MySQL database
mysql_conn = pymysql.connect(**mysql_connection_info)
mysql_cursor = mysql_conn.cursor()

# Perform the query on MySQL database
mysql_query = '''
SELECT
    100.00 * SUM(CASE WHEN P_TYPE LIKE 'PROMO%%' THEN L_EXTENDEDPRICE * (1 - L_DISCOUNT)
    ELSE 0
    END) / SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS PROMO_REVENUE
FROM
    lineitem,
    part
WHERE
    L_PARTKEY = P_PARTKEY
    AND L_SHIPDATE >= '1995-09-01'
    AND L_SHIPDATE < '1995-10-01'
'''

mysql_cursor.execute(mysql_query)

# Get query result
result = mysql_cursor.fetchone()

# Close the MySQL cursor and connection
mysql_cursor.close()
mysql_conn.close()

# Write query result to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['PROMO_REVENUE'])
    writer.writerow([str(Decimal(result[0]))])
