import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to mysql
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Execute query to get nations for Germany
mysql_cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'GERMANY'")
germany_nationkey = mysql_cursor.fetchone()[0]

# Execute query to get partsupp for Germany
mysql_cursor.execute(f"SELECT PS_PARTKEY, PS_AVAILQTY * PS_SUPPLYCOST AS value FROM partsupp, supplier WHERE PS_SUPPKEY = S_SUPPKEY AND S_NATIONKEY = {germany_nationkey}")
partsupp_data = mysql_cursor.fetchall()

# Calculate total value
total_value = sum([row[1] for row in partsupp_data])

# Find the important stock
important_stock = [(row[0], row[1]) for row in partsupp_data if row[1] > total_value * 0.0001]

# Sort important stock by value in descending order
important_stock.sort(key=lambda x: x[1], reverse=True)

# Close mysql connection
mysql_conn.close()

# Connect to redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
supplier_data = pd.read_json(redis_client.get('supplier').decode('utf-8'))

# Merge the supplier data for Germany
supplier_data_germany = supplier_data[supplier_data['S_NATIONKEY'] == germany_nationkey]

# Combine mysql and redis data
combined_data = pd.DataFrame(important_stock, columns=['PS_PARTKEY', 'value'])
combined_data = combined_data.merge(supplier_data_germany, left_on=['PS_PARTKEY'], right_on=['S_SUPPKEY'], how='inner')

# Output to CSV
combined_data.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC, columns=['PS_PARTKEY', 'value'])
