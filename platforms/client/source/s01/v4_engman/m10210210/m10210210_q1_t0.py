import pymysql
import csv

# Database connection details
mysql_config = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# SQL Query
sql_query = """
SELECT
    L_RETURNFLAG,
    L_LINESTATUS,
    SUM(L_QUANTITY) AS SUM_QTY,
    SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS SUM_DISC_PRICE,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) AS SUM_CHARGE,
    AVG(L_QUANTITY) AS AVG_QTY,
    AVG(L_EXTENDEDPRICE) AS AVG_PRICE,
    AVG(L_DISCOUNT) AS AVG_DISCOUNT,
    COUNT(*) AS COUNT_ORDER
FROM lineitem
WHERE L_SHIPDATE < '1998-09-02'
GROUP BY L_RETURNFLAG, L_LINESTATUS
ORDER BY L_RETURNFLAG, L_LINESTATUS;
"""

# Function to execute and write query results to CSV
def execute_query_to_csv(connection_config, query, output_file):
    try:
        with pymysql.connect(**connection_config) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                header = [i[0] for i in cursor.description]

                with open(output_file, 'w', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile)
                    csvwriter.writerow(header)
                    csvwriter.writerows(result)

    except Exception as e:
        print(f"An error occurred: {e}")

# Call the function to execute the query and write to 'query_output.csv'
execute_query_to_csv(mysql_config, sql_query, 'query_output.csv')
