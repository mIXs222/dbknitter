import pymysql
import csv

def create_mysql_connection(db_name, user, password, host):
    return pymysql.connect(db=tpch, user=root, passwd=my-secret-pw, host=mysql)

try:
    # Creating MySQL Connection
    mysql_conn = create_mysql_connection('tpch', 'root', 'my-secret-pw', 'mysql')

    # Query to execute
    query = """
    SELECT p.P_BRAND, p.P_TYPE, p.P_SIZE, COUNT(DISTINCT s.S_SUPPKEY) AS SUPPLIER_CNT
    FROM part AS p
    JOIN partsupp AS ps ON p.P_PARTKEY = ps.PS_PARTKEY
    JOIN supplier AS s ON s.S_SUPPKEY = ps.PS_SUPPKEY
    WHERE p.P_BRAND <> 'Brand#45'
    AND p.P_TYPE NOT LIKE 'MEDIUM POLISHED%'
    AND p.P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND s.S_COMMENT NOT LIKE '%Customer Complaints%'
    GROUP BY p.P_BRAND, p.P_TYPE, p.P_SIZE
    ORDER BY SUPPLIER_CNT DESC, p.P_BRAND, p.P_TYPE, p.P_SIZE
    """

    with mysql_conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

    # Write the result to a CSV file
    with open('query_output.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        # Write headers
        writer.writerow(['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT'])
        # Write results
        for row in result:
            writer.writerow(row)

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close MySQL Connection
    if mysql_conn:
        mysql_conn.close()
