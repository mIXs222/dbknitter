import pymysql
import csv

# Define the connection information for the MySQL database
mysql_conn_info = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

# Establish a connection to the MySQL database
connection = pymysql.connect(host=mysql_conn_info['host'],
                             user=mysql_conn_info['user'],
                             password=mysql_conn_info['password'],
                             database=mysql_conn_info['database'])

try:
    with connection.cursor() as cursor:
        # Define the query
        query = """
        SELECT s.S_SUPPKEY, s.S_NAME, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as total_revenue
        FROM supplier s
        JOIN lineitem l ON s.S_SUPPKEY = l.L_SUPPKEY
        WHERE l.L_SHIPDATE >= '1996-01-01' AND l.L_SHIPDATE < '1996-04-01'
        GROUP BY s.S_SUPPKEY, s.S_NAME
        ORDER BY total_revenue DESC, s.S_SUPPKEY;
        """

        # Execute the query
        cursor.execute(query)
        
        # Get the maximum revenue
        max_revenue = cursor.fetchone()[2]
        
        # Fetch all suppliers that have maximum revenue
        cursor.execute(query)
        top_suppliers = [row for row in cursor if row[2] == max_revenue]

        # Write the result to CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['S_SUPPKEY', 'S_NAME', 'TOTAL_REVENUE'])
            csvwriter.writerows(top_suppliers)

finally:
    connection.close()
