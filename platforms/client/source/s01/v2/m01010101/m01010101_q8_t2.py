import mysql.connector
from pymongo import MongoClient
import pandas as pd

#Establish connection to MySQL
mysql_conn = mysql.connector.connect(user='root', password='my-secret-pw',
                              host='mysql',
                              database='tpch')

#establish connection to MongoDB
mongo_client = MongoClient('mongodb', 27017)
db = mongo_client.tpch

#Get data from MySQL
mysql_query =  """SELECT orders.*,part.*,nation.* 
                FROM orders,part,nation 
                WHERE orders.P_PARTKEY = part.P_PARTKEY 
                AND orders.C_NATIONKEY = nation.N_NATIONKEY"""

mysql_data = pd.read_sql(mysql_query, mysql_conn)

#Create mongo query
mongo_query = {"R_NAME": "ASIA"}

#Get data from MongoDB
supplier_data = pd.DataFrame(list(db.supplier.find(mongo_query)))
customer_data = pd.DataFrame(list(db.customer.find(mongo_query)))
region_data = pd.DataFrame(list(db.region.find(mongo_query)))
lineitem_data = pd.DataFrame(list(db.lineitem.find(mongo_query)))

#merge all data by proper key as per your business requirement
merged_data = pd.merge(mysql_data,supplier_data,on='S_SUPPKEY',how='inner')
merged_data = pd.merge(merged_data,customer_data,on='C_CUSTKEY',how='inner')
merged_data = pd.merge(merged_data,region_data,on='N_REGIONKEY',how='inner')
merged_data = pd.merge(merged_data,lineitem_data,on='L_ORDERKEY',how='inner')  # assuming L_ORDERKEY as unique

#ginerated merged data and apply the provided filters then calculate as required
merged_data['O_ORDERDATE'] = pd.to_datetime(merged_data['O_ORDERDATE']) # convert O_ORDERDATE from string to date
merged_data = merged_data[((merged_data.O_ORDERDATE.dt.year>=1995) &(merged_data.O_ORDERDATE.dt.year<=1996) & (merged_data.P_TYPE=="SMALL PLATED COPPER"))]

#calculating final values per year
final_values = merged_data.groupby(merged_data.O_ORDERDATE.dt.year).apply(lambda x:sum((x.N_NAME=="INDIA")*x.L_EXTENDEDPRICE*(1-x.L_DISCOUNT))/sum(x.L_EXTENDEDPRICE*(1-x.L_DISCOUNT)))

# write to CSV
final_values.to_csv('query_output.csv')
