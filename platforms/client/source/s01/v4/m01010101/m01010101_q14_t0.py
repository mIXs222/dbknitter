import pymysql
import pymongo
import pandas as pd
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4')

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Retrieve 'part' data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT P_PARTKEY, P_TYPE 
        FROM part 
        WHERE P_TYPE LIKE 'PROMO%%'
        """)
    part_data = cursor.fetchall()
    part_df = pd.DataFrame(part_data, columns=['P_PARTKEY', 'P_TYPE'])

mysql_conn.close()

# Convert part keys to a set for quick lookup
promo_part_keys = set(part_df['P_PARTKEY'].tolist())

# Retrieve 'lineitem' data from MongoDB
lineitem_collection = mongo_db['lineitem']
lineitem_query = {
    'L_SHIPDATE': {
        '$gte': datetime(1995, 9, 1),
        '$lt': datetime(1995, 10, 1)
    }
}
lineitem_data = list(lineitem_collection.find(lineitem_query))
lineitem_df = pd.DataFrame(lineitem_data)

# Filter lineitem DataFrame for only promo parts
lineitem_df = lineitem_df[lineitem_df['L_PARTKEY'].isin(promo_part_keys)]

# Calculate PROMO_REVENUE
lineitem_df['DISCOUNT_PRICE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
promo_revenue = 100.00 * sum(lineitem_df['DISCOUNT_PRICE']) / sum(lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT']))

# Output result to CSV
result_df = pd.DataFrame([{'PROMO_REVENUE': promo_revenue}])
result_df.to_csv('query_output.csv', index=False)
