import pymysql
import csv

# Connect to MySQL database
connection = pymysql.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)

try:
    with connection.cursor() as cursor:
        # Execute the actual query
        query = """
        SELECT
            s.S_ACCTBAL, s.S_NAME, n.N_NAME, p.P_PARTKEY, p.P_MFGR,
            s.S_ADDRESS, s.S_PHONE, s.S_COMMENT
        FROM
            part p
            JOIN partsupp ps ON p.P_PARTKEY = ps.PS_PARTKEY
            JOIN supplier s ON ps.PS_SUPPKEY = s.S_SUPPKEY
            JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
            JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
        WHERE
            p.P_TYPE = 'BRASS' AND p.P_SIZE = 15
            AND r.R_NAME = 'EUROPE'
        ORDER BY
            ps.PS_SUPPLYCOST, s.S_ACCTBAL DESC, n.N_NAME, s.S_NAME, p.P_PARTKEY
        """
        cursor.execute(query)
        result = cursor.fetchall()
        
        # Write the query output to a CSV file
        with open("query_output.csv", "w", newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            # Write the header
            csv_writer.writerow(["Account Balance", "Supplier Name", "Nation Name",
                                 "Part Key", "Manufacturer", "Address", "Phone", "Comment"])
            # Write the data
            for row in result:
                csv_writer.writerow(row)

finally:
    connection.close()
