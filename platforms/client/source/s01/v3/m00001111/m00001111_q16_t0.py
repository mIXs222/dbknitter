import mysql.connector
from pymongo import MongoClient
import pandas as pd

# MySQL Connection
cnx = mysql.connector.connect(user='root', password='my-secret-pw',
                              host='mysql',
                              database='tpch')

cursor = cnx.cursor()

#MySQL Query
#Note that the partsupp table was removed from this query since it doesn't exist on the MySQL db.
query1 = ("SELECT " +
    "P_BRAND," +
    "P_TYPE," +
    "P_SIZE," +
    "S_SUPPKEY " +
    "FROM part, supplier " +
    "WHERE " +
    "P_PARTKEY = S_SUPPKEY " +
    "AND P_BRAND <> 'Brand#45' " +
    "AND P_TYPE NOT LIKE 'MEDIUM POLISHED%' " +
    "AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9) " +
    "AND S_SUPPKEY NOT IN ( " +
    "SELECT S_SUPPKEY " +
    "FROM supplier " +
    "WHERE S_COMMENT LIKE '%Customer%Complaints%' " +
    ")" +
    "GROUP BY P_BRAND,P_TYPE,P_SIZE " +
    "ORDER BY S_SUPPKEY DESC")

cursor.execute(query1)

mySQL_data = pd.DataFrame(cursor.fetchall(), columns=["P_BRAND","P_TYPE","P_SIZE","S_SUPPKEY"])

cursor.close()
cnx.close()

#MongoDB Connection
client = MongoClient('mongodb:27017')
db = client['tpch']
collection = db['partsupp']
#Get all documents from the collection
mongo_data = pd.DataFrame(list(collection.find()))

# Join and Group the DataFrames
result = pd.merge(mySQL_data, mongo_data, left_on='S_SUPPKEY', right_on='PS_SUPPKEY', how='inner')
result = result.groupby(['P_BRAND','P_TYPE', 'P_SIZE'], as_index=False).agg({'PS_SUPPKEY': "nunique"}).rename(columns={'PS_SUPPKEY':'SUPPLIER_CNT'})
result.sort_values(['SUPPLIER_CNT','P_BRAND','P_TYPE', 'P_SIZE'], ascending=[False,True,True,True])

result.to_csv('query_output.csv', index=False)
