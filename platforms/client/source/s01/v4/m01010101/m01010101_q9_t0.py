import pymysql
import pymongo
import pandas as pd
import datetime

# Function to convert string date to year
def extract_year(date_str):
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").year
    except ValueError:
        return None

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Execute MySQL query
mysql_query = """
SELECT
    N_NAME AS NATION,
    O_ORDERDATE AS O_ORDERDATE,
    P_PARTKEY, P_NAME,
    PS_SUPPLYCOST,
    L_PARTKEY, L_SUPPKEY,
    O_ORDERKEY, 
    L_EXTENDEDPRICE, L_DISCOUNT, L_QUANTITY
FROM
    part, partsupp, orders, nation
WHERE
    P_PARTKEY = PS_PARTKEY
    AND O_ORDERKEY IN (SELECT L_ORDERKEY FROM lineitem)
    AND S_NATIONKEY = N_NATIONKEY
    AND P_NAME LIKE '%dim%'
"""
mysql_cursor.execute(mysql_query)
mysql_result = mysql_cursor.fetchall()

# Columns from MySQL query
mysql_columns = [
    'NATION', 'O_ORDERDATE', 'P_PARTKEY', 'P_NAME',
    'PS_SUPPLYCOST', 'L_PARTKEY', 'L_SUPPKEY',
    'O_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_QUANTITY']

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# Get supplier data from MongoDB
suppliers = list(mongo_db.supplier.find({}, {'_id': 0, 'S_SUPPKEY': 1, 'S_NATIONKEY': 1}))
supplier_df = pd.DataFrame(suppliers)

# Get lineitem data from MongoDB
lineitems = list(mongo_db.lineitem.find(
    {},
    {'_id': 0, 'L_ORDERKEY': 1, 'L_PARTKEY': 1, 'L_SUPPKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1, 'L_QUANTITY': 1}
))
lineitem_df = pd.DataFrame(lineitems)

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Convert MySQL result to DataFrame
mysql_df = pd.DataFrame(mysql_result, columns=mysql_columns)

# Merge dataframes
merged_df = (
    mysql_df
    .merge(lineitem_df, on=['L_PARTKEY', 'L_SUPPKEY', 'O_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_QUANTITY'])
    .merge(supplier_df, on=['L_SUPPKEY'])
)

# Calculate amount and extract year
merged_df['O_YEAR'] = merged_df['O_ORDERDATE'].apply(extract_year)
merged_df['AMOUNT'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT']) - merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY']
profit_df = merged_df[['NATION', 'O_YEAR', 'AMOUNT']]

# Perform group by
final_df = profit_df.groupby(['NATION', 'O_YEAR']).sum().reset_index()

# Sort results
final_df.sort_values(by=['NATION', 'O_YEAR'], ascending=[True, False], inplace=True)

# Write to CSV
final_df.to_csv('query_output.csv', index=False)
