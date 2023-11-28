import pymysql
import csv

# Connection details
mysql_conn_details = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

# Connect to the MySQL database
mysql_conn = pymysql.connect(**mysql_conn_details)
mysql_cursor = mysql_conn.cursor()

# Execute the query
query = """
SELECT PS_PARTKEY, SUM(PS_SUPPLYCOST * PS_AVAILQTY) AS total_value
FROM partsupp
JOIN supplier ON PS_SUPPKEY = S_SUPPKEY
JOIN nation ON S_NATIONKEY = N_NATIONKEY
WHERE N_NAME = 'GERMANY'
GROUP BY PS_PARTKEY
HAVING total_value > (
    SELECT SUM(PS_SUPPLYCOST * PS_AVAILQTY) * 0.05
    FROM partsupp
    JOIN supplier ON PS_SUPPKEY = S_SUPPKEY
    JOIN nation ON S_NATIONKEY = N_NATIONKEY
    WHERE N_NAME = 'GERMANY'
)
ORDER BY total_value DESC;
"""

mysql_cursor.execute(query)

# Fetch the results
results = mysql_cursor.fetchall()

# Write to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['PS_PARTKEY', 'TOTAL_VALUE'])
    csvwriter.writerows(results)

# Close connections
mysql_cursor.close()
mysql_conn.close()
