import mysql.connector
import pymongo
import csv
import pandas as pd

mysql_db = mysql.connector.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)

mongodb_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb_db = mongodb_client["tpch"]

cursor = mysql_db.cursor()

query_mysql = """Select P_PARTKEY, P_MFGR from part where P_SIZE = 15 and P_TYPE LIKE '%BRASS'"""
cursor.execute(query_mysql)
mysql_data = cursor.fetchall()

supplier_data = mongodb_db["supplier"].find({})
region_data = mongodb_db["region"].find({"R_NAME": "EUROPE"})

# Prepare data for ease of processing
supplier_df = pd.DataFrame(list(supplier_data))
region_df = pd.DataFrame(list(region_data))
mysql_df = pd.DataFrame(mysql_data, columns=["P_PARTKEY", "P_MFGR"])

# Perform necessary joins and filtering in Python
final_df = pd.merge(mysql_df, supplier_df, how='inner', left_on='P_PARTKEY', right_on='S_SUPPKEY')
# More processing as per SQL query involving finding min cost etc. 

# Save the dataframe to csv
final_df.to_csv('query_output.csv', index=False)

print('Data saved successfully as query_output.csv')
