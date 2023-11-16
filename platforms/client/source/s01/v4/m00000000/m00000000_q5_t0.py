# query.py
import pymysql
import csv

# MySQL connection function
def mysql_connection():
    return pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch',
        charset='utf8mb4')

def execute_query(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()

# Main script
def main():
    mysql_sql = """
    SELECT
        N_NAME,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
    FROM
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
    WHERE
        C_CUSTKEY = O_CUSTKEY
        AND L_ORDERKEY = O_ORDERKEY
        AND L_SUPPKEY = S_SUPPKEY
        AND C_NATIONKEY = S_NATIONKEY
        AND S_NATIONKEY = N_NATIONKEY
        AND N_REGIONKEY = R_REGIONKEY
        AND R_NAME = 'ASIA'
        AND O_ORDERDATE >= '1990-01-01'
        AND O_ORDERDATE < '1995-01-01'
    GROUP BY
        N_NAME
    ORDER BY
        REVENUE DESC
    """

    # Connect to MySQL
    mysql_conn = mysql_connection()

    try:
        # Execute the query for MySQL
        results = execute_query(mysql_conn, mysql_sql)

        # Write results to CSV
        with open('query_output.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['N_NAME', 'REVENUE'])  # Header
            for row in results:
                csv_writer.writerow(row)
    finally:
        mysql_conn.close()

if __name__ == "__main__":
    main()
