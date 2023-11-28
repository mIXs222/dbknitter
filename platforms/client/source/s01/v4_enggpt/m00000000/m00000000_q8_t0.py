import pymysql
import csv

# Define the MySQL connection parameters
mysql_config = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Connect to the MySQL database
mysql_conn = pymysql.connect(**mysql_config)
try:
    mysql_cursor = mysql_conn.cursor()

    # Define the query to be executed
    mysql_query = """
    SELECT O_ORDERDATE, SUM(IF(N_NAME = 'INDIA', L_EXTENDEDPRICE * (1 - L_DISCOUNT), 0)) AS volume_india,
           SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_volume
    FROM lineitem
    JOIN orders ON L_ORDERKEY = O_ORDERKEY
    JOIN customer ON O_CUSTKEY = C_CUSTKEY
    JOIN nation ON C_NATIONKEY = N_NATIONKEY
    JOIN region ON N_REGIONKEY = R_REGIONKEY
    JOIN supplier ON L_SUPPKEY = S_SUPPKEY
    JOIN part ON L_PARTKEY = P_PARTKEY
    WHERE P_TYPE = 'SMALL PLATED COPPER'
      AND R_NAME = 'ASIA'
      AND O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
    GROUP BY YEAR(O_ORDERDATE)
    ORDER BY YEAR(O_ORDERDATE) ASC;
    """

    # Execute the query
    mysql_cursor.execute(mysql_query)

    # Fetch the results
    results = mysql_cursor.fetchall()

    # Write the query output to a CSV file
    with open('query_output.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write the headers
        writer.writerow(["Year", "Market Share in India"])
        # Write the data
        for row in results:
            year = row[0].year
            market_share = row[1] / row[2] if row[2] else 0  # Avoid division by zero
            writer.writerow([year, market_share])

finally:
    mysql_conn.close()
