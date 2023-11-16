import pymysql
import csv

# MySQL connection details
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Connect to MySQL
mysql_conn = pymysql.connect(
    host=mysql_conn_info['host'],
    user=mysql_conn_info['user'],
    password=mysql_conn_info['password'],
    database=mysql_conn_info['database']
)

try:
    with mysql_conn.cursor() as cursor:
        # Perform the query 
        cursor.execute("""
            SELECT
                SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY
            FROM
                lineitem,
                part
            WHERE
                P_PARTKEY = L_PARTKEY
                AND P_BRAND = 'Brand#23'
                AND P_CONTAINER = 'MED BAG'
                AND L_QUANTITY < (
                    SELECT
                        0.2 * AVG(L_QUANTITY)
                    FROM
                        lineitem
                    WHERE
                        L_PARTKEY = P_PARTKEY
                )
            """)
        
        # Fetch the result
        result = cursor.fetchall()
        # Write result to CSV
        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['AVG_YEARLY']) # header
            for row in result:
                writer.writerow(row)
finally:
    mysql_conn.close()
