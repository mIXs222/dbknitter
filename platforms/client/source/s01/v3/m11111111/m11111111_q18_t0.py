import pymongo
from pymongo import MongoClient
from mysql.connector import connect, Error
import pandas as pd

# Connecting to MongoDB server
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Extract data from MongoDB
customer_df = pd.DataFrame(list(db.customer.find({},{"_id":0})))
orders_df = pd.DataFrame(list(db.orders.find({},{"_id":0})))
lineitem_df = pd.DataFrame(list(db.lineitem.find({},{"_id":0})))

# Create dictionary to store MySQL and MongoDB data
data = {
    'customer': customer_df,
    'orders': orders_df,
    'lineitem': lineitem_df
}

# If there is MySQL data
# Connect to MySQL server
with connect(
    host="localhost",  # replace with your server host
    user="root",  # replace with your username
    password="password",  # replace with your password
) as connection:
    # Check if tpch exists in MySQL databases
    with connection.cursor() as cursor:
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        if 'tpch' in databases:
            cursor.execute("USE tpch")
            # Extract data from MySQL and add to data dictionary
            for table_name in ['customer', 'orders', 'lineitem']:
                query = f"SELECT * FROM {table_name}"
                cursor.execute(query)
                result = cursor.fetchall()
                df = pd.DataFrame(result)
                if table_name in data:
                    data[table_name] = pd.concat([data[table_name], df])
                else:
                    data[table_name] = df

# Perform join operations and filter data
filtered_lineitems = data['lineitem'][data['lineitem']['L_QUANTITY'] > 300].groupby('L_ORDERKEY').sum().reset_index()
result = pd.merge(data['customer'], data['orders'], left_on='C_CUSTKEY', right_on='O_CUSTKEY')
result = pd.merge(result, filtered_lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Output data
result.to_csv('query_output.csv', index=False)
