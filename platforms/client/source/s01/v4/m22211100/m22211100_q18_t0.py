import pymysql
import pymongo
import pandas as pd
import csv

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Connect to the MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# MySQL queries
mysql_query_orders_lineitem = """
SELECT O_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE, L_ORDERKEY
FROM orders
JOIN lineitem ON O_ORDERKEY = L_ORDERKEY
GROUP BY O_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE, L_ORDERKEY
HAVING SUM(L_QUANTITY) > 300
"""

with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query_orders_lineitem)
    mysql_results = cursor.fetchall()

mysql_df = pd.DataFrame(mysql_results, columns=['O_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_ORDERKEY'])

# MongoDB queries
mongo_results = mongodb['customer'].find({}, {'_id': 0, 'C_CUSTKEY': 1, 'C_NAME': 1})
mongo_df = pd.DataFrame(list(mongo_results))

# Combine MySQL and MongoDB results
combined_df = pd.merge(mysql_df, mongo_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Final aggregated results
final_df = combined_df.groupby(by=['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']) \
    .size() \
    .reset_index(name='SUM_L_QUANTITY')

final_df = final_df.sort_values(['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write to CSV file
final_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Close the connections
mysql_conn.close()
mongo_client.close()
