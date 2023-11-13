import mysql.connector
import pandas as pd
import pymongo
from pymongo import MongoClient

# Establish connections to MySQL and MongoDB
mysql_cn = mysql.connector.connect(user='root', password='my-secret-pw',
                              host='mysql',
                              database='tpch')

client = MongoClient("mongodb://mongodb:27017/")
mongodb = client["tpch"]

# Execute query on MySQL
query1 = """SELECT P_PARTKEY, P_BRAND, P_TYPE, P_SIZE 
            FROM part 
            WHERE P_BRAND <> 'Brand#45' 
            AND P_TYPE NOT LIKE 'MEDIUM POLISHED%' 
            AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)"""

df_mysql_part = pd.read_sql(query1, mysql_cn)

query2 = """SELECT S_SUPPKEY 
            FROM supplier 
            WHERE S_COMMENT  LIKE '%Customer%Complaints%'"""

df_mysql_supp = pd.read_sql(query2, mysql_cn)

# Fetch data from MongoDB
df_mongo = pd.DataFrame(list(mongodb.partsupp.find()))

# Merge data
df_merged = pd.merge(df_mysql_part, df_mongo, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')
df_merged = df_merged[~df_merged['PS_SUPPKEY'].isin(df_mysql_supp['S_SUPPKEY'])]

# Groupby and count distinct supplier
df_final = df_merged.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE'])['PS_SUPPKEY'].nunique().reset_index(name='SUPPLIER_CNT')
df_final.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True], inplace=True)

# Write the DataFrame to a csv
df_final.to_csv('query_output.csv', index=False)

# Close out the connections
mysql_cn.close()
client.close()
