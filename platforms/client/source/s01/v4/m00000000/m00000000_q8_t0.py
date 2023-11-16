import pymysql
import csv
from datetime import datetime

# Function to connect to the MySQL database and execute a query
def mysql_query(query):
    result = []
    try:
        # Connection parameters
        connection = pymysql.connect(host='mysql',
                                     user='root',
                                     password='my-secret-pw',
                                     database='tpch')
        with connection.cursor() as cursor:
            # Execute the query
            cursor.execute(query)
            # Fetch the result
            for row in cursor.fetchall():
                result.append(row)
    except Exception as e:
        print("Error: ", e)
    finally:
        if connection:
            connection.close()
    return result

# Query to execute (adjusted for MySQL syntax)
mysql_query_text = """
SELECT
    YEAR(O_ORDERDATE) AS O_YEAR,
    SUM(CASE WHEN N_NAME = 'INDIA' THEN L_EXTENDEDPRICE * (1 - L_DISCOUNT) ELSE 0 END) / SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS MKT_SHARE
FROM
    part, supplier, lineitem, orders, customer, nation, region
WHERE
    P_PARTKEY = L_PARTKEY
    AND S_SUPPKEY = L_SUPPKEY
    AND L_ORDERKEY = O_ORDERKEY
    AND O_CUSTKEY = C_CUSTKEY
    AND C_NATIONKEY = nation.N_NATIONKEY
    AND nation.N_REGIONKEY = R_REGIONKEY
    AND R_NAME = 'ASIA'
    AND S_NATIONKEY = nation.N_NATIONKEY
    AND O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
    AND P_TYPE = 'SMALL PLATED COPPER'
GROUP BY
    O_YEAR
ORDER BY
    O_YEAR;
"""

# Execute the query on MySQL
mysql_result = mysql_query(mysql_query_text)

# Write the result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['O_YEAR', 'MKT_SHARE']
    writer = csv.writer(csvfile)
    writer.writerow(fieldnames)

    for row in mysql_result:
        # Adjust the MKT_SHARE format
        formatted_row = [row[0], "{:.2f}".format(row[1] * 100)]
        writer.writerow(formatted_row)
