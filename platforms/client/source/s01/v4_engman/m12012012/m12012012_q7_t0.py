# multiplatform_query.py
import pymysql
import pymongo
import pandas as pd
from bson import ObjectId
from direct_redis import DirectRedis

# pandas options
pd.set_option('display.float_format', '{:.2f}'.format)

# MySQL connection
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# fetch customers from India and Japan from MySQL
customer_query = "SELECT C_CUSTKEY, C_NATIONKEY FROM customer WHERE C_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'INDIA' OR N_NAME = 'JAPAN');"
customers_df = pd.read_sql(customer_query, mysql_connection)
mysql_connection.close()

# MongoDB connection
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# fetch suppliers from India and Japan from MongoDB
suppliers = list(mongo_db.supplier.find({"S_NATIONKEY": {"$in": mongo_db.nation.find({"N_NAME": {"$in": ["INDIA", "JAPAN"]}}, {"N_NATIONKEY": 1})}}, {"_id": 0}))
suppliers_df = pd.DataFrame(suppliers)

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)
lineitems_raw = redis_client.get('lineitem')
lineitems_df = pd.read_json(lineitems_raw)

# Filter lineitems by the year 1995 and 1996.
lineitems_df['L_SHIPDATE'] = pd.to_datetime(lineitems_df['L_SHIPDATE'])
lineitems_df = lineitems_df[(lineitems_df['L_SHIPDATE'].dt.year == 1995) | (lineitems_df['L_SHIPDATE'].dt.year == 1996)]

# Merge DataFrames to combine information and calculate revenue
merged_df = (
    lineitems_df.merge(customers_df, left_on='L_CUSTKEY', right_on='C_CUSTKEY')
                .merge(suppliers_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
)

# Filter lineitems where supplier and customer are from different nations
merged_df = merged_df[(merged_df['C_NATIONKEY'] != merged_df['S_NATIONKEY']) & 
                      ((merged_df['C_NATIONKEY'].isin([ObjectId('INDIA'), ObjectId('JAPAN')])) |
                       (merged_df['S_NATIONKEY'].isin([ObjectId('INDIA'), ObjectId('JAPAN')])))]
                      
# Calculate the revenue
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
# Create year column
merged_df['L_YEAR'] = merged_df['L_SHIPDATE'].dt.year

# Select and rename columns as per requirement
result_df = merged_df[['C_NATIONKEY', 'L_YEAR', 'REVENUE', 'S_NATIONKEY']]
result_df.columns = ['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION']

# Order by Supplier nation, Customer nation, and year (all ascending)
result_df = result_df.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Write to CSV
result_df.to_csv('query_output.csv', index=False)

print('Query executed and output saved to query_output.csv')

