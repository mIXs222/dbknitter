import pymysql
import csv

# Function to execute the volume shipping query
def volume_shipping_query(connection):
    with connection.cursor() as cursor:
        query = """
        SELECT
            s_nation_name,
            c_nation_name,
            EXTRACT(YEAR FROM l_shipdate) AS year,
            SUM(l_extendedprice * (1 - l_discount)) AS revenue
        FROM 
            (SELECT s_name, n_name AS s_nation_name FROM supplier JOIN nation WHERE s_nationkey = n_nationkey) AS suppliers,
            (SELECT c_name, n_name AS c_nation_name FROM customer JOIN nation WHERE c_nationkey = n_nationkey) AS customers,
            lineitem 
        WHERE 
            s_name = l_suppkey AND
            c_name = l_orderkey AND
            ((s_nation_name = 'INDIA' AND c_nation_name = 'JAPAN') OR (s_nation_name = 'JAPAN' AND c_nation_name = 'INDIA')) AND
            l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
        GROUP BY
            s_nation_name,
            c_nation_name,
            year
        ORDER BY
            s_nation_name,
            c_nation_name,
            year;
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows

# Connect to the MySQL database using pymysql
mysql_connection_info = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "db": "tpch",
}

try:
    mysql_connection = pymysql.connect(**mysql_connection_info)
    results = volume_shipping_query(mysql_connection)

    # Write query results to csv file
    with open('query_output.csv', mode='w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["supplier_nation", "customer_nation", "year", "revenue"])
        for row in results:
            csv_writer.writerow(row)

except pymysql.MySQLError as e:
    print(f"An error occurred connecting to MySQL: {e}")

finally:
    if mysql_connection:
        mysql_connection.close()
