import mysql.connector
from pymongo import MongoClient
import pandas as pd

# connect to MySQL
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

# get data from MySQL
mycursor = mydb.cursor()

mycursor.execute("SELECT * FROM NATION WHERE N_NAME = 'GERMANY'")
nation = pd.DataFrame(mycursor.fetchall(), columns=[i[0] for i in mycursor.description])

mycursor.execute("SELECT * FROM SUPPLIER")
supplier = pd.DataFrame(mycursor.fetchall(), columns=[i[0] for i in mycursor.description])

# connect to MongoDB
client = MongoClient("mongodb")
db = client.tpch

# get data from MongoDB
part = pd.DataFrame(list(db.part.find()))
partsupp = pd.DataFrame(list(db.partsupp.find()))

# process data
supply = partsupp.merge(supplier, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
combined = supply.merge(nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

grouped = combined.groupby('PS_PARTKEY').\
    apply(lambda x: (x['PS_SUPPLYCOST'] * x['PS_AVAILQTY']).sum()).reset_index(name='VALUE')

total_value = grouped['VALUE'].sum() * 0.0001000000

final_df = grouped[grouped['VALUE'] > total_value].sort_values('VALUE', ascending=False)

# write to csv
final_df.to_csv('output.csv', index=False)