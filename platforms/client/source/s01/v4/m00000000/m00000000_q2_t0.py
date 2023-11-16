# Python code to execute the query across different databases and combine results
import csv
import pymysql

# Function to connect to the MySQL database
def get_mysql_connection(db_name, user, password, host):
    return pymysql.connect(host=host, user=user, passwd=password, db=db_name)

# Function to execute the query on the MySQL database
def execute_mysql_query(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()

# Main execution logic
if __name__ == '__main__':
    # Specify your MySQL connection details
    DATABASE_NAME = "tpch"
    USERNAME = "root"
    PASSWORD = "my-secret-pw"
    HOSTNAME = "mysql"

    # The query to be executed
    sql_query = """
    SELECT
        S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT
    FROM
        part, supplier, partsupp, nation, region
    WHERE
        P_PARTKEY = PS_PARTKEY AND S_SUPPKEY = PS_SUPPKEY AND P_SIZE = 15
        AND P_TYPE LIKE '%BRASS' AND S_NATIONKEY = N_NATIONKEY
        AND N_REGIONKEY = R_REGIONKEY AND R_NAME = 'EUROPE'
        AND PS_SUPPLYCOST = (
            SELECT
                MIN(PS_SUPPLYCOST)
            FROM
                partsupp, supplier, nation, region
            WHERE
                P_PARTKEY = PS_PARTKEY AND S_SUPPKEY = PS_SUPPKEY
                AND S_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY
                AND R_NAME = 'EUROPE'
        )
    ORDER BY
        S_ACCTBAL DESC, N_NAME, S_NAME, P_PARTKEY
    """

    # Establish a MySQL connection and execute the query
    mysql_connection = get_mysql_connection(DATABASE_NAME, USERNAME, PASSWORD, HOSTNAME)
    try:
        mysql_results = execute_mysql_query(mysql_connection, sql_query)
        
        # Output the combined results to a CSV file
        with open('query_output.csv', mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for row in mysql_results:
                writer.writerow(row)
    finally:
        mysql_connection.close()

    print("Query execution complete. Results stored in 'query_output.csv'.")

