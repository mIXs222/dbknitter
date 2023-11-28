# suppliers_analysis.py
import pandas as pd
import pymysql
from direct_redis import DirectRedis

# MySQL connection
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Create a pandas DataFrame for the 'supplier' and 'nation' tables from MySQL
with mysql_connection.cursor() as cursor:
    cursor.execute("SELECT * FROM supplier")
    suppliers = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

    cursor.execute("SELECT * FROM nation")
    nations = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

    cursor.execute("SELECT * FROM orders WHERE O_ORDERSTATUS = 'F'")
    orders = pd.DataFrame(cursor.fetchall(), columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'])

mysql_connection.close()

# Redis connection
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Create a pandas DataFrame for the 'lineitem' table from Redis
lineitem_df_json = redis_connection.get('lineitem')
lineitem = pd.read_json(lineitem_df_json)

# Filtering line items where the receipt date is later than commit date
lineitem_filtered = lineitem[lineitem['L_RECEIPTDATE'] > lineitem['L_COMMITDATE']]

# Filtering suppliers located in Saudi Arabia
suppliers_in_saudi = suppliers.merge(nations[nations['N_NAME'] == 'SAUDI ARABIA'], left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Merge line items with suppliers in Saudi and with orders
merged_data = lineitem_filtered.merge(suppliers_in_saudi, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_data = merged_data.merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Check for the conditions described using EXISTS subqueries equivalent
final_data = merged_data.groupby('S_NAME').agg(NUMWAIT=pd.NamedAgg(column='L_ORDERKEY', aggfunc='count'))
final_data = final_data.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Save the results to CSV
final_data.to_csv('query_output.csv')

print("Finished writing to query_output.csv")
