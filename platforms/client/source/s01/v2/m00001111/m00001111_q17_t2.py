import pandas as pd
from pymongo import MongoClient
import pymysql

# MySQL Setup
mysql_db = pymysql.connect(
    host="mysql",
    user="root",
    passwd="my-secret-pw",
    db="tpch"
)

# MongoDB Setup
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Loading MySQL data into DataFrame
mysql_df = pd.read_sql('SELECT * FROM part WHERE P_BRAND = "Brand#23" AND P_CONTAINER = "MED BAG"', con=mysql_db)

# Loading MongoDB data into DataFrame
mongo_df = pd.DataFrame(list(mongo_db.lineitem.find()))

# Merge and joining MySQL and MongoDB DataFrame
joined_df = pd.merge(mysql_df, mongo_df, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Subquery operation
avg_quantity = joined_df['L_QUANTITY'].mean() * 0.2
filtered_df = joined_df[joined_df['L_QUANTITY'] < avg_quantity]

# Final Calculation
query_result = filtered_df['L_EXTENDEDPRICE'].sum()  / 7.0

# Saving the output to CSV
pd.DataFrame([query_result], columns=['AVG_YEARLY']).to_csv('query_output.csv', index=False)

# Closing connections
mysql_db.close()
mongo_client.close()
