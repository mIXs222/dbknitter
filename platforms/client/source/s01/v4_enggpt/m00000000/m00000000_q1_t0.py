import pymysql
import csv
from datetime import datetime

# Function to run the query and write output to CSV
def execute_query_and_save_to_csv(connection_info, output_file):
    try:
        # Connect to MySQL database
        conn = pymysql.connect(
            host=connection_info['hostname'],
            user=connection_info['username'],
            password=connection_info['password'],
            db=connection_info['database'],
            charset='utf8mb4'
        )
        with conn.cursor() as cursor:
            # Prepare the SQL query
            sql_query = """
            SELECT
                L_RETURNFLAG,
                L_LINESTATUS,
                SUM(L_QUANTITY) AS SUM_QTY,
                SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE,
                SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS SUM_DISC_PRICE,
                SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) AS SUM_CHARGE,
                AVG(L_QUANTITY) AS AVG_QTY,
                AVG(L_EXTENDEDPRICE) AS AVG_PRICE,
                AVG(L_DISCOUNT) AS AVG_DISC,
                COUNT(*) AS COUNT_ORDER
            FROM
                lineitem
            WHERE
                L_SHIPDATE <= %s
            GROUP BY
                L_RETURNFLAG, L_LINESTATUS
            ORDER BY
                L_RETURNFLAG, L_LINESTATUS;
            """
            ship_date = datetime(1998, 9, 2)

            # Execute SQL query
            cursor.execute(sql_query, (ship_date,))

            # Fetch all the results
            results = cursor.fetchall()

            # Write results to CSV
            with open(output_file, 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)
                # Write header
                csvwriter.writerow([
                    'L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE',
                    'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE',
                    'AVG_DISC', 'COUNT_ORDER'
                ])
                # Write result rows
                for row in results:
                    csvwriter.writerow(row)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

# Database connection information
db_connection_info = {
    'database': 'tpch',
    'username': 'root',
    'password': 'my-secret-pw',
    'hostname': 'mysql'
}

# Output CSV file path
output_csv_file = 'query_output.csv'

# Run the function
execute_query_and_save_to_csv(db_connection_info, output_csv_file)
