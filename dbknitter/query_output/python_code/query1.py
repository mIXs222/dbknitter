import pandas as pd
from pymongo import MongoClient 
import pymysql.cursors

# Connect to the MySQL databases
connection1 = pymysql.connect(host='localhost',
                              user='root',
                              password='mySecret',
                              db='basicInfo')
cursor1 = connection1.cursor()

connection2 = pymysql.connect(host='localhost2',
                              user='root',
                              password='mySecret2',
                              db='familyInfo')
cursor2 = connection2.cursor()

# Connect to the MongoDB
client = MongoClient('localhost3')
db = client['interestInfo']
collection = db['table3']

# Query MySQL database
query1 = "SELECT NAME FROM table1"
df1 = pd.read_sql(query1, connection1)

# Query MongoDB
records = collection.find({})
df3 =  pd.DataFrame(records)

# Merge pandas dataframes
merged_df = pd.merge(df1, df3, on='NAME')

# Write to csv
merged_df.to_csv('query_output.csv')
