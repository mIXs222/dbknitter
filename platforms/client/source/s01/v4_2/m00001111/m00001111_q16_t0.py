import pymysql
import pymongo
import pandas as pd

# Connect to mysql
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Prepare and execute SQL to fetch data from mysql
mysql_query = '''
SELECT P_PARTKEY, P_BRAND, P_TYPE, P_SIZE FROM part WHERE
P_BRAND <> 'Brand#45'
AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
'''

mysql_cursor.execute(mysql_query)
mysql_result = mysql_cursor.fetchall()

# Transform data to pandas DataFrame
df_mysql = pd.DataFrame(mysql_result, columns=['P_PARTKEY', 'P_BRAND', 'P_TYPE', 'P_SIZE'])

# Connect to mongodb
mongo_conn = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_conn['tpch']

# Prepare query and fetch data from mongodb
mongo_query = {'PS_PARTKEY': {'$in': df_mysql['P_PARTKEY'].tolist()}}
mongo_result = mongo_db.partsupp.find(mongo_query)

# Transform data to pandas DataFrame
df_mongo = pd.DataFrame(list(mongo_result))

# Merge data from two data sources
df_combined = pd.merge(df_mysql, df_mongo, left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Filter out unwanted data
supplier_query = '''
SELECT S_SUPPKEY FROM supplier WHERE
S_COMMENT LIKE '%Customer%Complaints%'
'''
mysql_cursor.execute(supplier_query)
s_supplier = set([i[0] for i in mysql_cursor.fetchall()])
df_combined = df_combined[~df_combined['PS_SUPPKEY'].isin(s_supplier)]

# Output
output = df_combined.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg({'PS_SUPPKEY': 'nunique'}).reset_index()
output.columns = ['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_CNT']
output.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True], inplace=True)

# Write output to csv file
output.to_csv('query_output.csv', index=False)
