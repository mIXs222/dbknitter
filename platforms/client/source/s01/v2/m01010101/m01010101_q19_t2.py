import mysql.connector
import pandas as pd
from pymongo import MongoClient
import csv

mysql_db_connection = mysql.connector.connect(
  host="mysql",
  user="root",
  passwd="my-secret-pw",
  database="tpch"
)
mysql_cursor = mysql_db_connection.cursor()

mysql_query = "SELECT * FROM part WHERE P_BRAND IN ('Brand#12', 'Brand#23', 'Brand#34') AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG', 'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK', 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15"

mysql_cursor.execute(mysql_query)

column_names = [i[0] for i in mysql_cursor.description]
rows = mysql_cursor.fetchall()
part_df = pd.DataFrame(rows, columns=column_names)


mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client.tpch
lineitem_coll = mongodb.lineitem

lineitem_df = pd.DataFrame(list(lineitem_coll.find({})))

combined_df = pd.merge(lineitem_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Apply Filters
combined_df = combined_df[
    (
        (combined_df['P_BRAND'] == 'Brand#12') &
        (combined_df['L_QUANTITY'].between(1, 11)) &
        (combined_df['P_SIZE'].between(1, 5)) &
        (combined_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
    ) | (
        (combined_df['P_BRAND'] == 'Brand#23') & 
        (combined_df['L_QUANTITY'].between(10, 20)) & 
        (combined_df['P_SIZE'].between(1, 10))
    ) | (
        (combined_df['P_BRAND'] == 'Brand#34') & 
        (combined_df['L_QUANTITY'].between(20, 30)) & 
        (combined_df['P_SIZE'].between(1, 15))
    )
]

combined_df['REVENUE'] = combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])
combined_df.to_csv('query_output.csv', index=False)

mysql_db_connection.close()
mongo_client.close()
