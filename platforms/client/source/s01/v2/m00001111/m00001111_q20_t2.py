import pandas as pd
import mysql.connector
import pymongo

# MYSQL Connection
mydb = mysql.connector.connect(
    host="mysql",  # Host, for example "localhost"
    user="root",   # User name, for example "root"
    passwd="my-secret-pw",   # Password, for example "password"
    db="tpch"      # Database name, for example "tpch"
)

mycursor = mydb.cursor()

query1 = """SELECT S_NAME, S_ADDRESS, S_SUPPKEY
             FROM supplier, nation
             WHERE S_NATIONKEY = N_NATIONKEY AND N_NAME = 'CANADA'"""

mycursor.execute(query1)

df1 = pd.DataFrame(mycursor.fetchall(), columns=['S_NAME', 'S_ADDRESS', 'S_SUPPKEY'])

# MongoDB Connection
try:
    client = pymongo.MongoClient("mongodb://mongodb:27017/")  # Replace with MongoDB connection string
    db = client['tpch']
    partsupp = db['partsupp']
    lineitem = db['lineitem']
    
    df2 = pd.DataFrame(list(partsupp.find({}, {"PS_PARTKEY": 1, "PS_SUPPKEY": 1, "PS_AVAILQTY":1})))
    df3 = pd.DataFrame(list(lineitem.find({"L_SHIPDATE": {"$gte": pd.to_datetime('1994-01-01'),"$lt": pd.to_datetime('1995-01-01')} }, {"L_PARTKEY":1, "L_SUPPKEY":1, "L_QUANTITY":1})))

    combined_df = pd.merge(df1, df2, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')
    final_df = pd.merge(combined_df, df3, left_on=['PS_PARTKEY', 'PS_SUPPKEY'], right_on=['L_PARTKEY', 'L_SUPPKEY'])

    final_df[['S_NAME', 'S_ADDRESS']].to_csv('query_output.csv', index=False)

except Exception as e:
    print(e)
