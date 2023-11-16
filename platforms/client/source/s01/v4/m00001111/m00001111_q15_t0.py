import pymysql
import pymongo
import csv
import pandas as pd

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']
lineitem_collection = mongodb_db['lineitem']

# Retrieve lineitem data within the specified date range from MongoDB
query_date_range = {'L_SHIPDATE': {'$gte': '1996-01-01', '$lt': '1996-04-01'}}
projection_fields = {'L_SUPPKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1, '_id': 0}

revenue_with_suppkey = [
    {
        '$match': query_date_range
    },
    {
        '$project': projection_fields
    },
    {
        '$group': {
            '_id': '$L_SUPPKEY',
            'TOTAL_REVENUE': {
                '$sum': {
                    '$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]
                }
            }
        }
    }
]

lineitem_agg = list(lineitem_collection.aggregate(revenue_with_suppkey))

# Convert aggregation result to a DataFrame for easier processing
lineitem_df = pd.DataFrame(lineitem_agg)
lineitem_df.rename(columns={'_id': 'SUPPLIER_NO'}, inplace=True)

# Find the max TOTAL_REVENUE
max_total_revenue = lineitem_df['TOTAL_REVENUE'].max()

# Select suppliers with matching S_SUPPKEY from MySQL
mysql_query = "SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE FROM supplier"
mysql_cursor.execute(mysql_query)
suppliers = mysql_cursor.fetchall()

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Convert supplier results to DataFrame
suppliers_df = pd.DataFrame(suppliers, columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE'])

# Merge DataFrames and filter by max TOTAL_REVENUE
result_df = pd.merge(lineitem_df[lineitem_df['TOTAL_REVENUE'] == max_total_revenue], suppliers_df, left_on='SUPPLIER_NO', right_on='S_SUPPKEY')

# Select and reorder the necessary columns
output_df = result_df[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']]

# Write output to csv file
output_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
