import pymysql
import csv

# Connection details
MYSQL_DB = 'tpch'
MYSQL_USER = 'root'
MYSQL_PASS = 'my-secret-pw'
MYSQL_HOST = 'mysql'

# Establish connection to MySQL database
try:
    mysql_conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASS, db=MYSQL_DB)
except pymysql.MySQLError as e:
    print(e)
    exit()

# Query for MySQL
mysql_query = '''
SELECT C.C_NAME, COUNT(O.O_ORDERKEY) AS ORDER_COUNT
FROM customer C
LEFT JOIN orders O ON C.C_CUSTKEY = O.O_CUSTKEY AND O.O_ORDERSTATUS NOT IN ('pending', 'deposits')
GROUP BY C.C_NAME
ORDER BY ORDER_COUNT;
'''

try:
    with mysql_conn.cursor() as cursor:
        cursor.execute(mysql_query)
        mysql_results = cursor.fetchall()
finally:
    mysql_conn.close()

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['C_NAME', 'ORDER_COUNT'])
    for row in mysql_results:
        csvwriter.writerow(row)
