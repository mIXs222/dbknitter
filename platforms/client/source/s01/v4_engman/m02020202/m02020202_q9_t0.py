import pymysql
import pandas as pd
from direct_redis import DirectRedis
import csv
from datetime import datetime

# Connect to mysql
mysql_connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

# Prepare and execute query for mysql to fetch data from nation, part, partsupp, orders
mysql_query = '''
SELECT n.N_NATIONKEY as nationkey, n.N_NAME as nation,
YEAR(o.O_ORDERDATE) as order_year, 
(p.P_RETAILPRICE * (1)) - (ps.PS_SUPPLYCOST * l_quantity) as profit
FROM nation n
JOIN supplier s ON n.N_NATIONKEY = s.S_NATIONKEY
JOIN partsupp ps ON s.S_SUPPKEY = ps.PS_SUPPKEY
JOIN lineitem l ON ps.PS_PARTKEY = l.P_PARTKEY AND ps.PS_SUPPKEY = l.L_SUPPKEY
JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
JOIN part p ON l.L_PARTKEY = p.P_PARTKEY
WHERE p.P_NAME LIKE '%dim%'
ORDER BY n.N_NAME ASC, order_year DESC
'''

with mysql_connection.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_results = cursor.fetchall()

# Close the mysql connection
mysql_connection.close()

# Convert mysql data to pandas DataFrame
mysql_df = pd.DataFrame(mysql_results, columns=['nationkey', 'nation', 'order_year', 'profit'])

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Fetch supplier and lineitem data from Redis and convert to pandas DataFrame 
supplier_df = pd.read_json(redis.get('supplier'))
lineitem_df = pd.read_json(redis.get('lineitem'))

# Filter supplier data for nationkey
supplier_df = supplier_df[['S_SUPPKEY', 'S_NATIONKEY']]

# Create the combined dataset from the lineitem and supplier data considering the part 'dim' condition
combined_df = lineitem_df.merge(supplier_df, how='left', left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Calculate profit for the combined dataset
combined_df['profit'] = (combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])) - (combined_df['PS_SUPPLYCOST'] * combined_df['L_QUANTITY'])

# Include only necessary columns and group by nation and year
result_df = combined_df.groupby(['S_NATIONKEY', 'L_QUANTITY'])['profit'].sum().reset_index()

# Merge with the MySQL DataFrame to include the nation's name and sort as required
final_df = result_df.merge(mysql_df, how='left', left_on='S_NATIONKEY', right_on='nationkey')\
                   .sort_values(by=['nation', 'order_year'], ascending=[True, False])

# Write the query's output to the file
final_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_MINIMAL)
