import pymysql
import pymongo
import pandas as pd
from pymongo import MongoClient

# Connect to MySQL
mysql_db = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    cursorclass=pymysql.cursors.Cursor
)

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

try:
    with mysql_db.cursor() as cursor:
        # Query parts from MySQL
        mysql_query = """
        SELECT
            P_PARTKEY, P_RETAILPRICE
        FROM
            part
        WHERE
            P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG';
        """
        cursor.execute(mysql_query)
        parts_result = cursor.fetchall()

    # Get lineitem data from MongoDB
    lineitem_result = mongodb.lineitem.find({
        'L_PARTKEY': {'$in': [row[0] for row in parts_result]}
    })

    # Convert MySQL and MongoDB results to DataFrame
    parts_df = pd.DataFrame(parts_result, columns=['P_PARTKEY', 'P_RETAILPRICE'])
    lineitem_df = pd.DataFrame(list(lineitem_result))

    # Calculate the average quantity for each part
    avg_qty_per_part = lineitem_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()

    # Merge the average quantity with lineitem dataframe and filter
    merge_df = lineitem_df.merge(avg_qty_per_part, how='inner', on='L_PARTKEY', suffixes=('', '_AVG'))
    merge_df = merge_df[merge_df['L_QUANTITY'] < merge_df['L_QUANTITY_AVG'] * 0.2]

    # Merge with parts_df to get relevant parts data
    final_df = merge_df.merge(parts_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

    # Perform calculation of average yearly extended price
    final_df['AVG_YEARLY_EXTENDED_PRICE'] = final_df['L_EXTENDEDPRICE'] / 7.0

    # Select relevant columns for output
    output_df = final_df[['P_PARTKEY', 'AVG_YEARLY_EXTENDED_PRICE']]

    # Write to CSV
    output_df.to_csv('query_output.csv', index=False)

finally:
    mysql_db.close()
    mongo_client.close()
