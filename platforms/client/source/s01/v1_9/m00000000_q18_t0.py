import mysql.connector
import pandas as pd

# Create a function to connect to the MySQL database
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Exception as err:
        print(f"Error: '{err}'")

    return connection

# Function to execute query
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query successful")
        result = cursor.fetchall()
        return result
    except Exception as err:
        print(f"Error: '{err}'")

# Database Connection Details
host = "mysql"
user = "root"
password = "my-secret-pw"
database = "tpch"

# SQL Query
query = '''SELECT C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE, SUM(L_QUANTITY)
           FROM customer, orders, lineitem
           WHERE O_ORDERKEY IN (SELECT L_ORDERKEY FROM lineitem GROUP BY L_ORDERKEY HAVING SUM(L_QUANTITY) > 300)
           AND C_CUSTKEY = O_CUSTKEY
           AND O_ORDERKEY = L_ORDERKEY
           GROUP BY C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE
           ORDER BY O_TOTALPRICE DESC,O_ORDERDATE'''

# Create a connection to the database
connection = create_db_connection(host, user, password, database)

# Execute the query and fetch the result
res = execute_query(connection, query)

# Convert the result into pandas DataFrame
df = pd.DataFrame(res, columns=['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'SUM(L_QUANTITY)'])

# Write output to a CSV file
df.to_csv('query_output.csv', index=False)

