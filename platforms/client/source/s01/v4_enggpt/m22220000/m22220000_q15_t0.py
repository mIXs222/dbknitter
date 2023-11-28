# The Python code (execute_query.py)
import pymysql
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Calculate revenue0 from MySQL lineitem table
mysql_cursor.execute("""
    SELECT L_SUPPKEY as SUPPLIER_NO, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as TOTAL_REVENUE
    FROM lineitem
    WHERE L_SHIPDATE BETWEEN '1996-01-01' AND '1996-03-31'
    GROUP BY L_SUPPKEY;
""")
revenue0 = pd.DataFrame(mysql_cursor.fetchall(), columns=['SUPPLIER_NO', 'TOTAL_REVENUE'])

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch supplier data and convert to DataFrame
supplier_data = redis_conn.get('supplier')
supplier_df = pd.read_json(supplier_data)

# Merge revenue0 with supplier information
merged_data = pd.merge(supplier_df, revenue0, left_on='S_SUPPKEY', right_on='SUPPLIER_NO')

# Identify supplier with the maximum total revenue
max_rev_supplier = merged_data.loc[merged_data['TOTAL_REVENUE'].idxmax()]

# Save the result to CSV, considering only the supplier with max revenue
max_rev_supplier.to_frame().transpose().to_csv('query_output.csv', index=False)

mysql_cursor.close()
mysql_conn.close()
