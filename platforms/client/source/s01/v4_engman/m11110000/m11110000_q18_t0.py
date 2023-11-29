import pymysql
import csv

def execute_query():
    # Connection details
    connection_details = {
        'host': 'mysql',
        'user': 'root',
        'password': 'my-secret-pw',
        'db': 'tpch',
        'charset': 'utf8mb4'
    }
    
    # SQL query to execute
    query = """
    SELECT c.C_NAME, c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE, SUM(l.L_QUANTITY) as quantity
    FROM customer c
    JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
    JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
    GROUP BY c.C_CUSTKEY, o.O_ORDERKEY
    HAVING SUM(l.L_QUANTITY) > 300
    ORDER BY o.O_TOTALPRICE DESC, o.O_ORDERDATE;
    """

    # Connect to the database
    connection = pymysql.connect(**connection_details)
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Write results to CSV
            with open('query_output.csv', mode='w', newline='') as file:
                csv_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                # Write the header
                csv_writer.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'QUANTITY'])
                # Write the data
                for row in results:
                    csv_writer.writerow(row)
    finally:
        connection.close()

if __name__ == "__main__":
    execute_query()
