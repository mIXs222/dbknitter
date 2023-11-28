import pymysql
import csv

# Connect to the MySQL database
conn = pymysql.connect(
    host='mysql',
    port=3306,
    user='root',
    passwd='my-secret-pw',
    db='tpch'
)

try:
    with conn.cursor() as cursor:
        # Prepare the query
        query = """
            SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue
            FROM lineitem
            WHERE
                L_SHIPDATE BETWEEN '1994-01-01' AND '1994-12-31'
                AND L_DISCOUNT BETWEEN 0.05 AND 0.07
                AND L_QUANTITY < 24;
        """
        
        # Execute the query
        cursor.execute(query)
        
        # Fetch the results
        result = cursor.fetchone()
        total_revenue = result[0]
        
        # Write the output to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['total_revenue'])
            writer.writerow([total_revenue])
        
finally:
    conn.close()
