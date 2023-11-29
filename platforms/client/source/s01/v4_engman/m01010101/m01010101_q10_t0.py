import pymysql
import pymongo
import pandas as pd
from decimal import Decimal

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# MySQL query to get relevant nation and orders data
mysql_query = """
SELECT 
    n.N_NATIONKEY, n.N_NAME, n.N_COMMENT, o.O_ORDERKEY, o.O_CUSTKEY, o.O_TOTALPRICE 
FROM 
    nation n 
JOIN 
    orders o ON n.N_NATIONKEY = o.O_CUSTKEY 
WHERE 
    o.O_ORDERDATE BETWEEN '1993-10-01' AND '1994-01-01';
"""
mysql_cursor.execute(mysql_query)
nation_orders = mysql_cursor.fetchall()
nation_orders_df = pd.DataFrame(nation_orders, columns=['N_NATIONKEY', 'N_NAME', 'N_COMMENT', 'O_ORDERKEY', 'O_CUSTKEY', 'O_TOTALPRICE'])

# MongoDB query to get relevant customer and lineitem data
customers = mongodb_db['customer'].find({}, {
    "C_CUSTKEY": 1, "C_NAME": 1, "C_ACCTBAL": 1, "C_ADDRESS": 1, "C_PHONE": 1, "C_COMMENT": 1
})
lineitems = mongodb_db['lineitem'].find({
    "L_RETURNFLAG": "R", "L_SHIPDATE": {"$gte": "1993-10-01", "$lt": "1994-01-01"}
}, {
    "L_ORDERKEY": 1, "L_EXTENDEDPRICE": 1, "L_DISCOUNT": 1
})

lineitems_df = pd.DataFrame(list(lineitems))
lineitems_df['REVENUE_LOST'] = lineitems_df.apply(
    lambda row: Decimal(row['L_EXTENDEDPRICE']) * (1 - Decimal(row['L_DISCOUNT'])), axis=1
)
lineitems_grouped = lineitems_df.groupby('L_ORDERKEY').agg({'REVENUE_LOST': 'sum'}).reset_index()
customers_df = pd.DataFrame(list(customers))

# Merge all the dataframes into one
merged_df = nation_orders_df.merge(customers_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = merged_df.merge(lineitems_grouped, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Selecting the required columns and sorting
output_df = merged_df[['C_CUSTKEY', 'C_NAME', 'REVENUE_LOST', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']]
output_df = output_df.sort_values(by=['REVENUE_LOST', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[True, True, True, False])

# Write the final result to a CSV file
output_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
