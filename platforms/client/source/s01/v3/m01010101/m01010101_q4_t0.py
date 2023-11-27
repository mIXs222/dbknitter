# Required Libraries
import mysql.connector
from pymongo import MongoClient
import pandas as pd

# Connection MySQL Database
mysql_conn = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)
mysql_cur = mysql_conn.cursor()

# Connection MongoDB Database
mongo_client = MongoClient("mongodb://mongodb:27017/")
mongo_conn = mongo_client["tpch"]

# Define Query
query = """
SELECT O_ORDERPRIORITY, COUNT(*) AS ORDER_COUNT
FROM orders
WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01' AND EXISTS (
    SELECT * FROM lineitem WHERE L_ORDERKEY = O_ORDERKEY AND L_COMMITDATE < L_RECEIPTDATE
)
GROUP BY O_ORDERPRIORITY
ORDER BY O_ORDERPRIORITY
"""

# Execute Query in MySQL
mysql_cur.execute(query)

# Fetching data from Mysql
mysql_data = mysql_cur.fetchall()

# Convert to Pandas dataframe
df_mysql = pd.DataFrame(mysql_data, columns=['O_ORDERPRIORITY', 'ORDER_COUNT'])

# Fetching data from MongoDB
mongo_data = list(mongo_conn['lineitem'].find())
df_mongo = pd.DataFrame(mongo_data)

# Merge data
df_merge = pd.merge(df_mysql, df_mongo, left_on='O_ORDERPRIORITY', right_on='L_ORDERKEY')

# Save result
df_merge.to_csv("query_output.csv", index=False)
