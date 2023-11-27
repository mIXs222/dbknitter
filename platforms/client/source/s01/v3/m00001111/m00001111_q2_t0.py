import mysql.connector
import pymongo
import pandas as pd

# Connect to MySQL
mysql_db = mysql.connector.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)

cursor = mysql_db.cursor()

# Execute first part of the query on MySQL
cursor.execute("""SELECT S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, 
                  S_ADDRESS, S_PHONE, S_COMMENT
                  FROM part, supplier, nation,region
                  WHERE P_SIZE = 15 AND P_TYPE LIKE '%BRASS'
                  AND S_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY
                  AND R_NAME = 'EUROPE'""")

mysql_data = cursor.fetchall()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]

# Execute second part of the query on MongoDB
partsupp_data = mongodb.partsupp.find()

# Combine two sets of data
combined_data = mysql_data + list(partsupp_data)

# Convert combined_data to pandas dataframe for saving it as CSV
df = pd.DataFrame(combined_data)
df.to_csv('query_output.csv')
