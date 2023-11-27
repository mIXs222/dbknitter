import pymysql
import csv

# Database connection details
mysql_connection_details = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

try:
    # Connect to the MySQL database
    mysql_conn = pymysql.connect(**mysql_connection_details)
    mysql_cursor = mysql_conn.cursor()

    # SQL Query
    sql_query = """
    SELECT
        S.S_ACCTBAL, S.S_NAME, S.S_ADDRESS, S.S_PHONE, S.S_COMMENT,
        P.P_PARTKEY, P.P_MFGR, P.P_SIZE,
        N.N_NAME
    FROM
        supplier S
        JOIN nation N ON S.S_NATIONKEY = N.N_NATIONKEY
        JOIN region R ON N.N_REGIONKEY = R.R_REGIONKEY
        JOIN partsupp PS ON S.S_SUPPKEY = PS.PS_SUPPKEY
        JOIN part P ON PS.PS_PARTKEY = P.P_PARTKEY
    WHERE
        P.P_SIZE = 15
        AND P.P_TYPE LIKE '%BRASS'
        AND R.R_NAME = 'EUROPE'
    GROUP BY S.S_SUPPKEY
    HAVING PS.PS_SUPPLYCOST = MIN(PS.PS_SUPPLYCOST)
    ORDER BY
        S.S_ACCTBAL DESC, N.N_NAME, S.S_NAME, P.P_PARTKEY
    """

    # Execute the SQL query
    mysql_cursor.execute(sql_query)
    result_set = mysql_cursor.fetchall()

    # Write the results to a CSV file
    with open('query_output.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        # Write the header row
        csvwriter.writerow(['S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'P_PARTKEY', 'P_MFGR', 'P_SIZE', 'N_NAME'])
        # Write the data rows
        for row in result_set:
            csvwriter.writerow(row)
finally:
    # Close cursors and database connections
    mysql_cursor.close()
    mysql_conn.close()
