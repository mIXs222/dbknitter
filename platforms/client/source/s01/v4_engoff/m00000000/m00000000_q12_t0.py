import pymysql
import csv
from datetime import datetime

# Define connection parameters
db_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# Connect to the MySQL database
connection = pymysql.connect(host=db_params['host'],
                             user=db_params['user'],
                             password=db_params['password'],
                             db=db_params['db'])

try:
    with connection.cursor() as cursor:
        # SQL query
        query = """
            SELECT 
                L_SHIPMODE,
                O_ORDERPRIORITY,
                COUNT(*) as LateLineItemCount
            FROM 
                lineitem INNER JOIN orders ON L_ORDERKEY = O_ORDERKEY
            WHERE 
                L_SHIPMODE IN ('MAIL', 'SHIP') AND
                L_RECEIPTDATE BETWEEN '1994-01-01' AND '1995-01-01' AND
                L_SHIPDATE < L_COMMITDATE AND
                L_RECEIPTDATE > L_COMMITDATE AND
                ((O_ORDERPRIORITY = 'URGENT' OR O_ORDERPRIORITY = 'HIGH') OR
                (O_ORDERPRIORITY NOT IN ('URGENT', 'HIGH')))
            GROUP BY 
                L_SHIPMODE, O_ORDERPRIORITY
            """
        
        # Execute SQL query
        cursor.execute(query)
        
        # Fetch all the records
        result = cursor.fetchall()
        
        # Output file
        output_file = 'query_output.csv'
        
        # Columns for CSV file
        columns = ['L_SHIPMODE', 'O_ORDERPRIORITY', 'LateLineItemCount']

        # Writing to the csv file
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # write the header
            writer.writerow(columns)

            # write the data rows
            for row in result:
                writer.writerow(row)
                
    # Committing any pending transaction to the database
    connection.commit()

finally:
    connection.close()

print("Query executed and output written to query_output.csv")
