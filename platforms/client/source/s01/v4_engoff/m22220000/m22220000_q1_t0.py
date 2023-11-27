# pricing_summary_report.py
import pymysql
import csv

# MySQL connection details
mysql_details = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

# SQL query
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
    L_SHIPDATE < '1998-09-02'
GROUP BY
    L_RETURNFLAG, L_LINESTATUS
ORDER BY
    L_RETURNFLAG, L_LINESTATUS;
"""

# Main block
def main():
    try:
        # Connect to the MySQL Database
        mysql_connection = pymysql.connect(**mysql_details)
        
        with mysql_connection.cursor() as cursor:
            cursor.execute(sql_query)
            results = cursor.fetchall()

            # Write query output to CSV file
            with open('query_output.csv', 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([i[0] for i in cursor.description])  # Write headers
                for row in results:
                    writer.writerow(row)

    except pymysql.MySQLError as e:
        print(f"Error connecting to MySQL Database: {e}")
    finally:
        if mysql_connection:
            mysql_connection.close()

if __name__ == "__main__":
    main()
