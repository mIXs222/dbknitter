import pyodbc
import pymongo
import pandas as pd

# Connect to MySQL
conn = pyodbc.connect(
    driver='{MySQL ODBC 8.0 Unicode Driver}',
    host='mysql',
    database='tpch',
    user='root',
    password='my-secret-pw')
cursor = conn.cursor()

# Execute Query
query = 'SELECT * FROM supplier'
supplier = pd.read_sql(query, conn)
query = 'SELECT * FROM nation'
nation = pd.read_sql(query, conn)

# Close Connection
conn.close()

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)

# Select Database
db = client['tpch']

# Select Collection
orders = pd.DataFrame(list(db.orders.find()))
lineitem = pd.DataFrame(list(db.lineitems.find()))

# Drop MongoDB connection
client.close()

# Data processing according to SQL query
# Skipping this step as the actual processing might depend on the specific data and the logic of the SQL query

output.to_csv('query_output.csv', index=False)
