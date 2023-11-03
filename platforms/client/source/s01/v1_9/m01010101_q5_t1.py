import mysql.connector
from pymongo import MongoClient
import pandas as pd
from functools import reduce

# MySQL connection & query
mysql_conn = mysql.connector.connect(user='root', password='my-secret-pw',
                              host='mysql', database='tpch')
mysql_cursor = mysql_conn.cursor()

mysql_query_1 = ("SELECT N_NAME, C_CUSTKEY, O_ORDERKEY, L_EXTENDEDPRICE, L_DISCOUNT "
                "FROM nation, customer, orders, lineitem "
                "WHERE C_CUSTKEY = O_CUSTKEY AND "
                "L_ORDERKEY = O_ORDERKEY AND "
                "C_NATIONKEY = N_NATIONKEY AND "
                "O_ORDERDATE >= '1990-01-01' AND "
                "O_ORDERDATE < '1995-01-01'")

mysql_cursor.execute(mysql_query_1)
mysql_df_1 = pd.DataFrame(mysql_cursor.fetchall(), 
                          columns=['N_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT'])

mysql_conn.close()

# MongoDB connection & query
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

region_df = pd.DataFrame(list(mongo_db['region'].find({'R_NAME': 'ASIA'}, {'_id':0, 'R_REGIONKEY':1})))
supplier_df = pd.DataFrame(list(mongo_db['supplier'].find({}, {'_id':0, 'S_SUPPKEY':1, 'S_NATIONKEY':1})))

dfs = [region_df, supplier_df, mysql_df_1]

# merge all dataframes on common keys
df_final = reduce(lambda left,right: pd.merge(left,right), dfs)
df_final['REVENUE'] = df_final['L_EXTENDEDPRICE'] * (1 - df_final['L_DISCOUNT'])

# Group and order according to the original query
output_df = df_final.groupby('N_NAME').agg({'REVENUE': 'sum'}).sort_values('REVENUE', ascending=False)

output_df.to_csv('query_output.csv')
