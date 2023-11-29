import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Fetch nation and supplier data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT n.N_NATIONKEY, s.S_SUPPKEY, s.S_NAME
        FROM nation n
        JOIN supplier s ON n.N_NATIONKEY = s.S_NATIONKEY
        WHERE n.N_NAME = 'SAUDI ARABIA'
    """)
    
    supplier_nation_data = cursor.fetchall()

# Convert fetched data to Pandas DataFrame
supplier_nation_df = pd.DataFrame(supplier_nation_data, columns=['N_NATIONKEY', 'S_SUPPKEY', 'S_NAME'])

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get lineitem data from Redis
lineitem_df = pd.DataFrame(eval(r.get('lineitem')))  # Assuming the data is stored in a way that r.get returns a string representation of a list of dictionaries

# Close the MySQL connection
mysql_conn.close()

# Process the query
# a. Keep only lineitems with L_RETURNFLAG 'F'
lineitem_df_filtered = lineitem_df[lineitem_df['L_RETURNFLAG'] == 'F']

# b. Identify multi-supplier orders
multi_supplier_orders = lineitem_df_filtered.groupby('L_ORDERKEY').filter(lambda x: x['L_SUPPKEY'].nunique() > 1)

# c. Identify suppliers who failed to meet the committed delivery date
failed_suppliers = multi_supplier_orders[multi_supplier_orders['L_COMMITDATE'] < multi_supplier_orders['L_RECEIPTDATE']]

# Analyze suppliers for multi-supplier orders
result = (
    failed_suppliers.merge(supplier_nation_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .groupby(['S_SUPPKEY', 'S_NAME'])
    .size()
    .reset_index(name='NUMWAIT')
    .sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])
)

# Write output to CSV file
result.to_csv('query_output.csv', index=False)
