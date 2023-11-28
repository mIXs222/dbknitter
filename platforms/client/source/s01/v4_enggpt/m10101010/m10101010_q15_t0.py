# query.py

import pymysql
import csv
from datetime import datetime

# Connection details
mysql_connection_details = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

# Connect to MySQL Database
def get_mysql_connection():
    try:
        return pymysql.connect(db=mysql_connection_details['database'],
                               user=mysql_connection_details['user'],
                               password=mysql_connection_details['password'],
                               host=mysql_connection_details['host'],
                               )
    except pymysql.MySQLError as e:
        print("Error in connecting to MySQL database: ", e)
        return None

def get_max_revenue_supplier(connection):
    query = """
    WITH revenue0 AS (
        SELECT L_SUPPKEY AS SUPPLIER_NO, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE
        FROM lineitem
        WHERE L_SHIPDATE BETWEEN '1996-01-01' AND '1996-03-31'
        GROUP BY L_SUPPKEY
    )
    SELECT S.S_SUPPKEY, S.S_NAME, S.S_ADDRESS, S.S_PHONE, R.TOTAL_REVENUE
    FROM supplier S
    INNER JOIN revenue0 R ON S.S_SUPPKEY = R.SUPPLIER_NO
    WHERE R.TOTAL_REVENUE = (
        SELECT MAX(TOTAL_REVENUE) FROM revenue0
    )
    ORDER BY S.S_SUPPKEY ASC;
    """
    
    with connection.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
    
    return results

def save_to_csv(data):
    with open('query_output.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE'])
        for row in data:
            csvwriter.writerow(row)

def main():
    mysql_conn = get_mysql_connection()
    if mysql_conn:
        try:
            max_revenue_supplier = get_max_revenue_supplier(mysql_conn)
            save_to_csv(max_revenue_supplier)
        finally:
            mysql_conn.close()

if __name__ == "__main__":
    main()
