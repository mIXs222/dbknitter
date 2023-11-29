import pymysql
import csv

# MySQL connection setup
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')

try:
    # Query to execute
    query = """
    SELECT COUNT(O_ORDERKEY) AS num_orders, COUNT(DISTINCT C_CUSTKEY) AS num_customers
    FROM customer
    JOIN orders ON customer.C_CUSTKEY = orders.O_CUSTKEY
    WHERE O_ORDERSTATUS NOT IN ('pending', 'deposit')
    AND O_COMMENT NOT LIKE '%pending%deposits%'
    GROUP BY C_CUSTKEY
    ORDER BY num_orders;
    """
    
    with mysql_conn.cursor() as cursor:
        # Execute the query
        cursor.execute(query)

        # Fetch all the results
        results = cursor.fetchall()

        # Calculate the distribution
        distribution = {}
        for row in results:
            num_orders = row[0]
            distribution[num_orders] = distribution.get(num_orders, 0) + 1

        # Write the query output to file
        with open('query_output.csv', mode='w', newline='') as outfile:
            csv_writer = csv.writer(outfile)
            csv_writer.writerow(['num_orders', 'num_customers'])
            for num_orders, num_customers in distribution.items():
                csv_writer.writerow([num_orders, num_customers])

finally:
    mysql_conn.close()
