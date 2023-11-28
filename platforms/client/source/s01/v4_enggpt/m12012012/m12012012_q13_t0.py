import pymysql
import pymongo
import pandas as pd
import csv

# Connect to mysql
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# Connect to mongodb
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Query MySQL
mysql_query = """
SELECT customer.C_CUSTKEY, COUNT(orders.O_ORDERKEY) as O_COUNT
FROM customer
LEFT OUTER JOIN (
    SELECT * FROM orders
    WHERE NOT (O_COMMENT LIKE '%pending%' OR O_COMMENT LIKE '%deposits%')
) AS orders
ON customer.C_CUSTKEY = orders.O_CUSTKEY
GROUP BY customer.C_CUSTKEY
"""

# Execute MySQL Query
with mysql_connection.cursor() as cursor:
    cursor.execute(mysql_query)
    mysql_results = cursor.fetchall()

# Convert mysql results to dataframe
mysql_df = pd.DataFrame(mysql_results, columns=['C_CUSTKEY', 'O_COUNT'])

# Query MongoDB
mongodb_orders = mongodb_db['orders'].find({
    '$nor': [
        {'O_COMMENT': {'$regex': 'pending'}},
        {'O_COMMENT': {'$regex': 'deposits'}}
    ]
})

# Convert MongoDB results to dataframe
mongodb_df = pd.DataFrame(list(mongodb_orders))
mongodb_df = mongodb_df.rename(columns={'O_CUSTKEY': 'C_CUSTKEY'})

# Merge dataframes on C_CUSTKEY
merged_df = pd.merge(mysql_df, mongodb_df.groupby('C_CUSTKEY').size().reset_index(name='O_COUNT'), how='left', on='C_CUSTKEY')
merged_df = merged_df.fillna(0)
merged_df['O_COUNT'] = merged_df.sum(axis=1) - merged_df['C_CUSTKEY']
merged_df = merged_df[['C_CUSTKEY', 'O_COUNT']]

# Group by O_COUNT
output_df = merged_df.groupby('O_COUNT').size().reset_index(name='CUSTDIST')

# Sort resulting dataframe
output_df = output_df.sort_values(by=['CUSTDIST', 'O_COUNT'], ascending=[False, False])

# Write output to CSV
output_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
