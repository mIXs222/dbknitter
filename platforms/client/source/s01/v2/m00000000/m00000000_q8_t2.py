# query.py 
import mysql.connector
import csv
import pandas as pd

# Create a connection
cnx = mysql.connector.connect(user='root', password='my-secret-pw',
                              host='mysql', database='tpch')

# Create a cursor
cursor = cnx.cursor()

# Define the query
query = """YOUR_SQL_QUERY"""

# Execute
cursor.execute(query)

# Fetch all the rows
rows = cursor.fetchall()

# Get column names
column_names = [i[0] for i in cursor.description]

# Create dataframe
df = pd.DataFrame(rows, columns=column_names)

# Write to CSV
df.to_csv('query_output.csv', index=False)

# Close the cursor
cursor.close()

# Close the connection
cnx.close()
