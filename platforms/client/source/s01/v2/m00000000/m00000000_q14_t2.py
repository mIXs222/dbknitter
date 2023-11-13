import mysql.connector
from mysql.connector import Error
import csv


try:
    connection = mysql.connector.connect(host='mysql',
                                         database='tpch',
                                         user='root',
                                         password='my-secret-pw')

    if connection.is_connected():
        db_info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_info)

        cursor = connection.cursor()
        cursor.execute("SELECT DATABASE();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

        query = """
        SELECT
            100.00 * SUM(CASE WHEN P_TYPE LIKE 'PROMO%' THEN L_EXTENDEDPRICE * (1 - L_DISCOUNT)
            ELSE 0
            END) / SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS PROMO_REVENUE
        FROM
            lineitem,
            part
        WHERE
            L_PARTKEY = P_PARTKEY
            AND L_SHIPDATE >= '1995-09-01'
            AND L_SHIPDATE < '1995-10-01'
        """
        cursor.execute(query)
        result = cursor.fetchone()

        with open("query_output.csv", "w") as file:
            writer = csv.writer(file)
            writer.writerow(['PROMO_REVENUE'])
            writer.writerow(result)

except Error as e:
    print("Error while connecting to MySQL", e)

finally:
    if (connection.is_connected()):
        cursor.close()
        connection.close()
        print("MySQL connection is closed")

