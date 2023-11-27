# import necessary libraries
import pymysql
import pymongo
import pandas as pd
from datetime import datetime

# Connecting to the MySQL database
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   database='tpch')

# Fetch 'part' table where P_TYPE like 'PROMO%'
mysql_query = "SELECT P_PARTKEY, P_TYPE FROM part WHERE P_TYPE LIKE 'PROMO%'"
with mysql_connection.cursor() as cursor:
    cursor.execute(mysql_query)
    part_result = cursor.fetchall()
df_part = pd.DataFrame(part_result, columns=['P_PARTKEY', 'P_TYPE'])

# Close the MySQL connection
mysql_connection.close()

# Connect to MongoDB database
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_collection = mongo_db['lineitem']

# Fetch 'lineitem' data with the specified L_SHIPDATE
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 10, 1)
lineitem_cursor = mongo_collection.find({'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}},
                                        {'_id': 0, 'L_PARTKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1})
df_lineitem = pd.DataFrame(list(lineitem_cursor))

# Combine data from both databases
df_combined = pd.merge(df_lineitem, df_part, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Compute the PROMO_REVENUE
promo_revenue = 100.00 * df_combined.apply(
    lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']) if row['P_TYPE'].startswith('PROMO')
    else 0, axis=1).sum() / df_combined['L_EXTENDEDPRICE'].apply(
    lambda x: x * (1 - df_combined['L_DISCOUNT'])).sum()

# Write to CSV
df_output = pd.DataFrame([{'PROMO_REVENUE': promo_revenue}])
df_output.to_csv('query_output.csv', index=False)
