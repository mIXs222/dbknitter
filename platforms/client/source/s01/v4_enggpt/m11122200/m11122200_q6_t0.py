import pymysql
import csv
from datetime import datetime

# Connection settings
connection_settings = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch',
}

# Connect to MySQL
connection = pymysql.connect(**connection_settings)

try:
    with connection.cursor() as cursor:
        # SQL query
        sql_query = """
        SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue
        FROM lineitem
        WHERE L_SHIPDATE BETWEEN '1994-01-01' AND '1994-12-31'
          AND L_DISCOUNT BETWEEN 0.05 AND 0.07
          AND L_QUANTITY < 24;
        """
        cursor.execute(sql_query)
        result = cursor.fetchone()

        # Write result to CSV
        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['total_revenue'])
            writer.writerow(result)
finally:
    connection.close()
