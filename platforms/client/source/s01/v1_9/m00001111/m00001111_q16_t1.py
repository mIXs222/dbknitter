import mysql.connector
import pandas as pd
import pymongo

# Connect to MySQL
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

mysql_cursor = mydb.cursor()
mysql_cursor.execute('''SELECT * FROM part WHERE P_BRAND <> 'Brand#45' 
                        AND P_TYPE NOT LIKE 'MEDIUM POLISHED%' 
                        AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)''')
part_table = pd.DataFrame(mysql_cursor.fetchall(), columns=[*map(lambda x: x[0], mysql_cursor.description)])

mysql_cursor.execute('''SELECT S_SUPPKEY FROM supplier WHERE S_COMMENT LIKE '%Customer%Complaints%' ''')
excluded_suppliers = list(map(lambda x: x[0], mysql_cursor.fetchall()))

# Connect to MongoDB
myclient = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = myclient["tpch"]
partsupp_table = pd.DataFrame(list(mongodb["partsupp"].find()))

filtered_partsupp_table = partsupp_table[~partsupp_table["PS_SUPPKEY"].isin(excluded_suppliers)]
merged_result = pd.merge(part_table, filtered_partsupp_table,left_on='P_PARTKEY', right_on='PS_PARTKEY')

grouped_result = merged_result.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg(SUPPLIER_CNT=('PS_SUPPKEY', 'nunique')).reset_index().sort_values(['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])
grouped_result.to_csv('query_output.csv', index=False)
