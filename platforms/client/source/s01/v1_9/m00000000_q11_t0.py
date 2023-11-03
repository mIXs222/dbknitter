# Python code
import pymysql
import csv

# Database Connection
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:
        # Executing the SQL Query
        sql = """
        SELECT PS_PARTKEY, SUM(PS_SUPPLYCOST * PS_AVAILQTY) AS VALUE
        FROM partsupp, supplier, nation
        WHERE PS_SUPPKEY = S_SUPPKEY
        AND S_NATIONKEY = N_NATIONKEY
        AND N_NAME = 'GERMANY'
        GROUP BY PS_PARTKEY HAVING SUM(PS_SUPPLYCOST * PS_AVAILQTY) > 
        (SELECT SUM(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001000000 FROM partsupp, supplier, nation 
        WHERE PS_SUPPKEY = S_SUPPKEY AND S_NATIONKEY = N_NATIONKEY AND N_NAME = 'GERMANY')
        ORDER BY VALUE DESC
        """
        cursor.execute(sql)
        # Fetching the results
        query_result = cursor.fetchall()

        # Writing the results to CSV file
        keys = query_result[0].keys()
        with open('query_output.csv', 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(query_result)

finally:
    connection.close()
