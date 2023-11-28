import pymysql
import pymongo
import pandas as pd
from pymongo import MongoClient

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Get necessary data from MySQL
with mysql_conn.cursor() as cursor:
    # Query to select German suppliers
    cursor.execute("""
        SELECT supplier.S_SUPPKEY, supplier.S_NAME
        FROM supplier
        INNER JOIN nation ON supplier.S_NATIONKEY = nation.N_NATIONKEY
        WHERE nation.N_NAME = 'GERMANY'
    """)
    suppliers = cursor.fetchall()
    
# Convert suppliers data to Pandas DataFrame
suppliers_df = pd.DataFrame(suppliers, columns=['S_SUPPKEY', 'S_NAME'])

# Get necessary data from MongoDB
nation_collection = mongo_db['nation']
partsupp_collection = mongo_db['partsupp']

# Select all documents from the nation collection where N_NAME is "GERMANY"
german_nations = list(nation_collection.find({'N_NAME': 'GERMANY'}))
german_nation_keys = [nation['N_NATIONKEY'] for nation in german_nations]

# Select all documents from the partsupp collection where PS_SUPPKEY is in German suppliers
partsupp = list(partsupp_collection.find({'PS_SUPPKEY': {'$in': suppliers_df['S_SUPPKEY'].tolist()}}))

# Convert partsupp data to Pandas DataFrame
partsupp_df = pd.DataFrame(partsupp)

# Calculate the total value for each part and filter by the specified conditions
partsupp_df['TOTAL_VALUE'] = partsupp_df['PS_SUPPLYCOST'] * partsupp_df['PS_AVAILQTY']

# Group by PS_PARTKEY and filter groups by the threshold condition
threshold = 0.5 * partsupp_df['TOTAL_VALUE'].sum()  # Example threshold
grouped = partsupp_df.groupby('PS_PARTKEY').filter(lambda x: (x['PS_SUPPLYCOST'] * x['PS_AVAILQTY']).sum() > threshold)

# Sort the result
result = grouped.sort_values(by='TOTAL_VALUE', ascending=False)

# Write the result to a file
result.to_csv('query_output.csv', index=False)

# Close the MySQL connection
mysql_conn.close()
