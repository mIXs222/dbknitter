import pymysql
import pymongo
import pandas as pd
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Retrieve data from MySQL (lineitem table)
query = """
SELECT L_PARTKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE
FROM lineitem
WHERE L_SHIPDATE BETWEEN '1995-09-01' AND '1995-09-30';
"""
lineitem_df = pd.read_sql(query, mysql_conn)

# Retrieve data from MongoDB (part table)
part_col = mongodb_db['part']
part_data = [
    {
        "P_PARTKEY": doc["P_PARTKEY"],
        "P_TYPE": doc["P_TYPE"]
    }
    for doc in part_col.find()
]
part_df = pd.DataFrame(part_data)

# Merge the dataframes on P_PARTKEY and L_PARTKEY
merged_df = pd.merge(lineitem_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate discount prices
merged_df['DISCOUNT_PRICE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Filter promotional lines
promo_df = merged_df[merged_df['P_TYPE'].str.startswith('PROMO')]
promo_revenue = promo_df['DISCOUNT_PRICE'].sum()

# Calculate total revenue
total_revenue = merged_df['DISCOUNT_PRICE'].sum()

# Calculate promotional revenue percentage
promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Write results to csv
results_df = pd.DataFrame({
    'Promotional_Revenue': [promo_revenue],
    'Total_Revenue': [total_revenue],
    'Promotional_Revenue_Percentage': [promo_revenue_percentage]
})
results_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
mongodb_client.close()
