import pymysql
import csv
from datetime import datetime

# Define MySQL connection parameters.
mysql_conf = {
    'host': 'mysql',
    'port': 3306,
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch',
}

def execute_query(connection):
    query = """
    SELECT s.S_SUPPKEY, s.S_NAME, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as revenue
    FROM supplier s
    JOIN lineitem l ON s.S_SUPPKEY = l.L_SUPPKEY
    WHERE l.L_SHIPDATE >= '1996-01-01' AND l.L_SHIPDATE < '1996-04-01'
    GROUP BY s.S_SUPPKEY, s.S_NAME
    ORDER BY revenue DESC, s.S_SUPPKEY ASC;
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            if result:
                return result
            else:
                return []
    except Exception as e:
        print("Error executing query:", str(e))
        return []

def main():
    # Connect to MySQL database.
    connection = pymysql.connect(**mysql_conf)

    # Execute the query and fetch result.
    result = execute_query(connection)

    # Write results to a CSV file.
    output_file = 'query_output.csv'
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['S_SUPPKEY', 'S_NAME', 'REVENUE'])
        for row in result:
            writer.writerow(row)
    
    # Close the database connection.
    connection.close()

    print(f"Query results written to {output_file}")

if __name__ == "__main__":
    main()
