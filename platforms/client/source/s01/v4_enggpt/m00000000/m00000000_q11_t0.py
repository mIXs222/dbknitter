import pymysql
import csv

# Establish a connection to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    with connection.cursor() as cursor:
        # SQL query
        sql = """
        SELECT ps.PS_PARTKEY,
               SUM(ps.PS_SUPPLYCOST * ps.PS_AVAILQTY) AS total_value
        FROM partsupp AS ps
        JOIN supplier AS s ON ps.PS_SUPPKEY = s.S_SUPPKEY
        JOIN nation AS n ON s.S_NATIONKEY = n.N_NATIONKEY
        WHERE n.N_NAME = 'GERMANY'
        GROUP BY ps.PS_PARTKEY
        HAVING total_value > (
            SELECT SUM(ps_inner.PS_SUPPLYCOST * ps_inner.PS_AVAILQTY) * 0.05
            FROM partsupp AS ps_inner
            JOIN supplier AS s_inner ON ps_inner.PS_SUPPKEY = s_inner.S_SUPPKEY
            JOIN nation AS n_inner ON s_inner.S_NATIONKEY = n_inner.N_NATIONKEY
            WHERE n_inner.N_NAME = 'GERMANY'
        )
        ORDER BY total_value DESC;
        """

        # Execute the query
        cursor.execute(sql)
        results = cursor.fetchall()

        # Write the output to the query_output.csv file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write headers based on the first result's keys, if results are present
            if results:
                headers = results[0].keys()
                csvwriter.writerow(headers)
            # Write data rows
            for row in results:
                csvwriter.writerow(row.values())

finally:
    connection.close()
