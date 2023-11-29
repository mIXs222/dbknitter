# pricing_summary_report.py

import pymysql
import csv

# Connection information
db_info = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql'
}

# SQL Query
sql_query = """
SELECT L_RETURNFLAG, 
       L_LINESTATUS, 
       SUM(L_QUANTITY) AS SUM_QTY,
       SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE,
       SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS SUM_DISC_PRICE,
       SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) AS SUM_CHARGE,
       AVG(L_QUANTITY) AS AVG_QTY,
       AVG(L_EXTENDEDPRICE) AS AVG_PRICE,
       AVG(L_DISCOUNT) AS AVG_DISC,
       COUNT(*) AS COUNT_ORDER
FROM lineitem
WHERE L_SHIPDATE <= '1998-09-02'
GROUP BY L_RETURNFLAG, L_LINESTATUS
ORDER BY L_RETURNFLAG, L_LINESTATUS;
"""

# Establish connection to MySQL server
conn = pymysql.connect(host=db_info['host'],
                       user=db_info['user'],
                       password=db_info['password'],
                       database=db_info['database'])

try:
    with conn.cursor() as cursor:
        cursor.execute(sql_query)
        results = cursor.fetchall()

        # Writing results to query_output.csv
        with open('query_output.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            # Write header
            csv_writer.writerow([i[0] for i in cursor.description])
            # Write data
            csv_writer.writerows(results)

finally:
    conn.close()
