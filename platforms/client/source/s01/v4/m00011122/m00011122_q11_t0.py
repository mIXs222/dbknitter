import pymysql
import pymongo
import pandas as pd
import csv

# Connect to mysql
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4')

# Retrieve nation data from mysql
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_NAME = 'GERMANY'")
    nations = cursor.fetchall()

# Mapping of N_NATIONKEY to N_NAME for 'GERMANY'
nation_germany_key = {nation[0]: nation[1] for nation in nations}

# Connect to mongodb
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']

# Retrieve supplier and partsupp data from mongodb
supplier_docs = mongo_db['supplier'].find({"S_NATIONKEY": {"$in": list(nation_germany_key.keys())}})
partsupp_docs = mongo_db['partsupp'].find()

# Convert mongo docs to dataframes
supplier_df = pd.DataFrame(list(supplier_docs))
partsupp_df = pd.DataFrame(list(partsupp_docs))

# Merge supplier with partsupp on S_SUPPKEY = PS_SUPPKEY
merged_df = partsupp_df.merge(supplier_df, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Filter only German suppliers
german_suppliers_df = merged_df[merged_df['S_NATIONKEY'].isin(nation_germany_key)]

# Calculate the sum of PS_SUPPLYCOST * PS_AVAILQTY for Germany
total_value_for_germany = german_suppliers_df.eval('PS_SUPPLYCOST * PS_AVAILQTY').sum() * 0.0001000000

# Calculate each part's total value
german_suppliers_df['VALUE'] = german_suppliers_df.eval('PS_SUPPLYCOST * PS_AVAILQTY')

# Group by part key and summing the values for suppliers in GERMANY
result_df = german_suppliers_df.groupby('PS_PARTKEY').agg(TOTAL_VALUE=pd.NamedAgg(column='VALUE', aggfunc='sum'))

# Filter out the groups having total value greater than the calculated total value for Germany
final_result_df = result_df[result_df['TOTAL_VALUE'] > total_value_for_germany].sort_values(by='TOTAL_VALUE', ascending=False).reset_index()

# Save results to CSV
final_result_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
