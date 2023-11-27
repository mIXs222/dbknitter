# top_supplier_query.py
import pymysql.cursors
import pandas as pd
from direct_redis import DirectRedis
import datetime

# Connect to mysql
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Get lineitem data from MySQL within the specified timeframe
start_date = datetime.date(1996, 1, 1)
end_date = datetime.date(1996, 4, 1)
query = """
SELECT L_SUPPKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue
FROM lineitem
WHERE L_SHIPDATE BETWEEN %s AND %s
GROUP BY L_SUPPKEY
ORDER BY total_revenue DESC, L_SUPPKEY ASC
"""

with mysql_connection.cursor() as cursor:
    cursor.execute(query, (start_date, end_date))
    lineitem_revenue = cursor.fetchall()

# Formatting result as a DataFrame
lineitem_df = pd.DataFrame(list(lineitem_revenue), columns=['L_SUPPKEY', 'total_revenue'])

# Connect with Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Get supplier data from Redis
supplier_data = redis.get('supplier')
supplier_df = pd.read_json(supplier_data)

# Merging the dataframes on supplier key
result_df = pd.merge(supplier_df, lineitem_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY')

# Take the top supplier(s) based on revenue
max_revenue = result_df['total_revenue'].max()
top_suppliers = result_df[result_df['total_revenue'] == max_revenue]

# Write the query output to a CSV file
top_suppliers.to_csv('query_output.csv', index=False)

# Close the MySQL connection
mysql_connection.close()
