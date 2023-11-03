import pymysql
import pandas as pd
import pymongo

# MySQL connection details
mysql_db_name = "tpch"
mysql_username = "root"
mysql_password = "my-secret-pw"
mysql_hostname = "mysql"
mysql_port = 3306

# MongoDB connection details
mongodb_db_name = "tpch"
mongodb_hostname = "mongodb"
mongodb_port = 27017

# Connect to MySQL
mysql_connection = pymysql.connect(host=mysql_hostname, user=mysql_username, password=mysql_password, db=mysql_db_name)
df_customer_mysql = pd.read_sql('SELECT * FROM customer', con=mysql_connection)

# Connect to MongoDB 
mongodb_connection = pymongo.MongoClient(mongodb_hostname, mongodb_port)
mongodb_database = mongodb_connection[mongodb_db_name]

# Fetch data from MongoDB into Pandas DataFrames
df_customer_mongodb = pd.DataFrame(list(mongodb_database.customer.find()))
df_orders = pd.DataFrame(list(mongodb_database.orders.find()))

# Merge customer data from MySQL and MongoDB
df_customer = pd.concat([df_customer_mysql, df_customer_mongodb])

# Perform the left JOIN and filtering 
C_ORDERS = pd.merge(df_customer, df_orders, left_on='C_CUSTKEY', right_on = 'O_CUSTKEY', how='left')
C_ORDERS = C_ORDERS[C_ORDERS['O_COMMENT'].str.contains('%pending%deposits%') == False]

# Perform the GROUP BY and count
C_ORDERS_grouped = C_ORDERS.groupby('C_CUSTKEY').agg({'O_ORDERKEY': pd.Series.nunique})

# Perform a final GROUP BY and sort
final_df = C_ORDERS_grouped.groupby('O_ORDERKEY').size().reset_index(name='CUSTDIST').sort_values(by=['CUSTDIST','O_ORDERKEY'], ascending=False)

# Write the output to a CSV
final_df.to_csv('query_output.csv', index=False)
