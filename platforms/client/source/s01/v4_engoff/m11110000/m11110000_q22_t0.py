import pymysql
import csv
from datetime import date, timedelta

# Function to create database connection
def connect_to_mysql():
    return pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.Cursor
    )

# Function to execute the query and fetch the data
def execute_global_sales_opportunity_query(connection, valid_country_codes, years):
    end_date = date.today() - timedelta(days=365 * years)

    query = """
    SELECT LEFT(C_PHONE, 2) AS country_code, COUNT(*) AS customer_count,
           AVG(C_ACCTBAL) AS avg_balance
    FROM customer
    WHERE LEFT(C_PHONE, 2) IN (%s) AND C_ACCTBAL > 0.00
      AND NOT EXISTS (
        SELECT * FROM orders
        WHERE customer.C_CUSTKEY = orders.O_CUSTKEY
          AND O_ORDERDATE >= %s
      )
    GROUP BY LEFT(C_PHONE, 2)
    """
    country_code_string = ",".join(f"'{code}'" for code in valid_country_codes)
    with connection.cursor() as cursor:
        cursor.execute(query, (country_code_string, end_date))
        result = cursor.fetchall()
    return result

# Main executable function
def main():
    # Connect to MySQL
    mysql_conn = connect_to_mysql()
    
    # Country codes to filter
    valid_country_codes = ['20', '40', '22', '30', '39', '42', '21']

    # Fetch the data
    data = execute_global_sales_opportunity_query(mysql_conn, valid_country_codes, 7)
    
    # Close the MySQL connection
    mysql_conn.close()
    
    # Write the output to a CSV file
    output_file = 'query_output.csv'
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow(['country_code', 'customer_count', 'avg_balance'])
        # Write rows
        for row in data:
            writer.writerow(row)

# Execute the main function
if __name__ == '__main__':
    main()
