import pymysql
import csv
from datetime import datetime

# Connect to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    with connection.cursor() as cursor:
        # SQL query to perform analysis
        sql_query = """
        SELECT 
            L_RETURNFLAG, 
            L_LINESTATUS, 
            SUM(L_QUANTITY) AS SUM_QTY, 
            SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE, 
            SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS SUM_DISC_PRICE, 
            SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)*(1+L_TAX)) AS SUM_CHARGE, 
            AVG(L_QUANTITY) AS AVG_QTY, 
            AVG(L_EXTENDEDPRICE) AS AVG_PRICE, 
            AVG(L_DISCOUNT) AS AVG_DISC, 
            COUNT(*) AS COUNT_ORDER 
        FROM 
            lineitem 
        WHERE 
            L_SHIPDATE <= '1998-09-02' 
        GROUP BY 
            L_RETURNFLAG, 
            L_LINESTATUS 
        ORDER BY 
            L_RETURNFLAG, 
            L_LINESTATUS;
        """
        
        # Execute the SQL query
        cursor.execute(sql_query)
        
        # Fetch all the rows
        results = cursor.fetchall()
        
        # Define output filename
        output_file = 'query_output.csv'
        
        # Write the query result to a CSV file
        with open(output_file, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            # Write the header
            csv_writer.writerow([
                'L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE',
                'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 
                'AVG_DISC', 'COUNT_ORDER'
            ])
            # Write the data rows
            for row in results:
                csv_writer.writerow(row)

finally:
    # Close the database connection
    connection.close()
