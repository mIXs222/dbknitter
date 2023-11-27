import pymysql
import csv
from decimal import Decimal
from datetime import datetime

# Function to connect to MySQL and execute the query
def execute_mysql_query():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 database='tpch',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.Cursor)
    try:
        with connection.cursor() as cursor:
            query = """SELECT
                        L_RETURNFLAG,
                        L_LINESTATUS,
                        SUM(L_QUANTITY) as SUM_QTY,
                        SUM(L_EXTENDEDPRICE) as SUM_BASE_PRICE,
                        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as SUM_DISC_PRICE,
                        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) as SUM_CHARGE,
                        AVG(L_QUANTITY) as AVG_QTY,
                        AVG(L_EXTENDEDPRICE) as AVG_PRICE,
                        AVG(L_DISCOUNT) as AVG_DISC,
                        COUNT(*) as COUNT_ORDER
                     FROM lineitem
                     WHERE
                        L_SHIPDATE <= '1998-09-02'
                     GROUP BY L_RETURNFLAG, L_LINESTATUS
                     ORDER BY L_RETURNFLAG, L_LINESTATUS"""
            cursor.execute(query)
            result = cursor.fetchall()
            return result
    finally:
        connection.close()

# Function to write query output to a CSV file
def write_to_csv(data):
    with open('query_output.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow(['L_RETURNFLAG', 'L_LINESTATUS',
                         'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE', 'SUM_CHARGE',
                         'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER'])
        # Write data
        for row in data:
            writer.writerow([row[0], row[1],
                            float(row[2]), float(row[3]), float(row[4]), float(row[5]),
                            float(row[6]), float(row[7]), float(row[8]), row[9]])

# Execute the query and write to CSV
result_data = execute_mysql_query()
write_to_csv(result_data)
