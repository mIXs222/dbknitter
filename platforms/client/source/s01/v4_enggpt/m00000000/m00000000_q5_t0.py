import pymysql
import csv
from datetime import datetime

# MySQL connection function
def get_mysql_connection(db_name, user, password, host):
    return pymysql.connect(host=host, user=user, passwd=password, db=db_name)

# Main function to get data and write to CSV
def main():
    try:
        # Connection to the MySQL Database
        mysql_conn = get_mysql_connection('tpch', 'root', 'my-secret-pw', 'mysql')

        # SQL Query
        query = """
        SELECT n.N_NAME, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue
        FROM customer c
        JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
        JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
        JOIN supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
        JOIN nation n ON c.C_NATIONKEY = n.N_NATIONKEY
        JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
        WHERE r.R_NAME = 'ASIA'
        AND o.O_ORDERDATE >= '1990-01-01'
        AND o.O_ORDERDATE <= '1994-12-31'
        GROUP BY n.N_NAME
        ORDER BY revenue DESC;
        """

        with mysql_conn.cursor() as cursor:
            cursor.execute(query)
            result_set = cursor.fetchall()

            # Writing to CSV
            with open('query_output.csv', mode='w', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow(['N_NAME', 'REVENUE'])   # header row
                for row in result_set:
                    csv_writer.writerow(row)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        mysql_conn.close()

if __name__ == '__main__':
    main()
