# query.py
import pymysql
import csv

# MySQL database connection information
db_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Connect to MySQL database
connection = pymysql.connect(host=db_info['host'],
                             user=db_info['user'],
                             password=db_info['password'],
                             database=db_info['database'])

try:
    with connection.cursor() as cursor:
        # SQL query to execute
        sql = """
        SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
        FROM lineitem, part
        WHERE 
        (
            (P_BRAND = 'Brand#12' AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND L_QUANTITY >= 1 AND L_QUANTITY <= 11 AND P_SIZE BETWEEN 1 AND 5)
            OR
            (P_BRAND = 'Brand#23' AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND L_QUANTITY >= 10 AND L_QUANTITY <= 20 AND P_SIZE BETWEEN 1 AND 10)
            OR
            (P_BRAND = 'Brand#34' AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND L_QUANTITY >= 20 AND L_QUANTITY <= 30 AND P_SIZE BETWEEN 1 AND 15)
        )
        AND L_PARTKEY = P_PARTKEY
        AND L_SHIPMODE IN ('AIR', 'AIR REG')
        AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
        """

        # Execute SQL query
        cursor.execute(sql)
        result = cursor.fetchone()

        # Write result to CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['REVENUE'])  # Header
            writer.writerow([result['REVENUE']])

finally:
    connection.close()
