import pymysql
import csv

# Connect to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Prepare the SQL query
query = """
SELECT 
    YEAR(L_SHIPDATE) AS year, 
    SUM(L_EXTENDEDPRICE) / 7.0 AS avg_yearly_extended_price
FROM 
    part JOIN lineitem ON P_PARTKEY = L_PARTKEY
WHERE
    P_BRAND = 'Brand#23' AND
    P_CONTAINER = 'MED BAG' AND
    L_QUANTITY < (
        SELECT 
            0.20 * AVG(L_QUANTITY)
        FROM 
            lineitem
        WHERE
            L_PARTKEY = part.P_PARTKEY
    )
GROUP BY 
    YEAR(L_SHIPDATE);
"""

# Perform the query and write the results to a CSV file
with connection.cursor() as cursor:
    cursor.execute(query)
    rows = cursor.fetchall()
    with open('query_output.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['year', 'avg_yearly_extended_price'])
        for row in rows:
            writer.writerow(row)

# Close the connection
connection.close()
