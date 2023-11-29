import pymysql
import pymongo
import pandas as pd
import csv
from datetime import datetime

# Establish connection to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Select necessary fields from MySQL tables
with mysql_conn.cursor() as cursor:
    nation_query = "SELECT N_NATIONKEY, N_NAME FROM nation"
    part_query = "SELECT P_PARTKEY, P_NAME, P_MFGR, P_TYPE FROM part WHERE P_NAME LIKE '%dim%'"
    supplier_query = "SELECT S_SUPPKEY, S_NATIONKEY FROM supplier"

    cursor.execute(nation_query)
    nations = {n_nationkey: n_name for n_nationkey, n_name in cursor.fetchall()}
    
    cursor.execute(part_query)
    parts = cursor.fetchall()

    cursor.execute(supplier_query)
    suppliers = cursor.fetchall()

# Close connection to MySQL
mysql_conn.close()

# Establish connection to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Get collections from MongoDB
partsupp_col = mongodb_db['partsupp']
orders_col = mongodb_db['orders']
lineitem_col = mongodb_db['lineitem']

# Use Pandas to process data

# Convert parts and suppliers to DataFrame
parts_df = pd.DataFrame(parts, columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_TYPE'])
suppliers_df = pd.DataFrame(suppliers, columns=['S_SUPPKEY', 'S_NATIONKEY'])

# Get lineitem and partsupp data from MongoDB
partsupp_df = pd.DataFrame(list(partsupp_col.find()))
lineitems = list(lineitem_col.find({"L_PARTKEY": {"$in": parts_df["P_PARTKEY"].values.tolist()}}))
lineitem_df = pd.DataFrame(lineitems)

# Combine dataframes
parts_df.rename(columns={'P_PARTKEY': 'L_PARTKEY'}, inplace=True)
result_df = lineitem_df.merge(parts_df, on='L_PARTKEY')
result_df = result_df.merge(partsupp_df, left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
result_df = result_df.merge(suppliers_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Calculate profit
result_df['YEAR'] = result_df['L_SHIPDATE'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").year)
result_df['PROFIT'] = (result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])) - (result_df['PS_SUPPLYCOST'] * result_df['L_QUANTITY'])

# Aggregate profit by nation and year
profit_df = result_df.groupby(['S_NATIONKEY', 'YEAR']).agg({'PROFIT': 'sum'}).reset_index()

# Map nation names and sort
profit_df['N_NAME'] = profit_df['S_NATIONKEY'].map(nations)
profit_df.sort_values(by=['N_NAME', 'YEAR'], ascending=[True, False], inplace=True)

# Write to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['NATION', 'YEAR', 'PROFIT'])
    
    for index, row in profit_df.iterrows():
        csvwriter.writerow([row['N_NAME'], row['YEAR'], row['PROFIT']])
