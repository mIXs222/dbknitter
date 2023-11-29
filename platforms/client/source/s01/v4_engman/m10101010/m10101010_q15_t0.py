# top_supplier.py
import pymysql
import csv

# Database connection parameters
mysql_params = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

# SQL query
sql_query = """
SELECT s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS, s.S_PHONE, 
       SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS TOTAL_REVENUE
FROM supplier AS s
JOIN lineitem AS l ON s.S_SUPPKEY = l.L_SUPPKEY
WHERE l.L_SHIPDATE BETWEEN '1996-01-01' AND '1996-04-01'
GROUP BY s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS, s.S_PHONE
ORDER BY TOTAL_REVENUE DESC, s.S_SUPPKEY
"""

def main():
    # Connect to the MySQL database
    mysql_connection = pymysql.connect(**mysql_params)
    cursor = mysql_connection.cursor()

    # Execute the query
    cursor.execute(sql_query)

    # Fetch all the results
    results = cursor.fetchall()

    # Close the connection
    cursor.close()
    mysql_connection.close()

    # Determine the maximum revenue
    max_revenue = max(row[4] for row in results)

    # Filter results to include only top suppliers
    top_suppliers = [row for row in results if row[4] == max_revenue]

    # Write results to CSV
    with open('query_output.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE'])
        for row in top_suppliers:
            writer.writerow(row)

if __name__ == "__main__":
    main()
