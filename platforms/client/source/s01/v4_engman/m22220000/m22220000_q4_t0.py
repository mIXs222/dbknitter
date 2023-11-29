# query.py
import pymysql
import csv

# MySQL connection parameters
mysql_params = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

# SQL query
sql_query = """
SELECT COUNT(DISTINCT o.O_ORDERKEY) AS ORDER_COUNT, o.O_ORDERPRIORITY
FROM orders o
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE o.O_ORDERDATE >= '1993-07-01'
AND o.O_ORDERDATE < '1993-10-01'
AND l.L_RECEIPTDATE > l.L_COMMITDATE
GROUP BY o.O_ORDERPRIORITY
ORDER BY o.O_ORDERPRIORITY;
"""

# Function to run the query and write to the CSV file
def run_query():
    # Connect to MySQL
    mysql_conn = pymysql.connect(**mysql_params)
    try:
        with mysql_conn.cursor() as cursor:
            cursor.execute(sql_query)
            results = cursor.fetchall()
        
        # Write to query_output.csv
        with open('query_output.csv', 'w', newline='') as csvfile:
            fieldnames = ['ORDER_COUNT', 'O_ORDERPRIORITY']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for row in results:
                writer.writerow({'ORDER_COUNT': row[0], 'O_ORDERPRIORITY': row[1]})
    finally:
        mysql_conn.close()

# Run the function if the script is executed
if __name__ == "__main__":
    run_query()
