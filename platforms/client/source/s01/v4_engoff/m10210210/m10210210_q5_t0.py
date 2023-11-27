import pymysql
import pymongo
from direct_redis import DirectRedis
import pandas as pd

# Connect to MySQL Server
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

# Connect to MongoDB Server
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis Server
redis_db = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT R_REGIONKEY, R_NAME
        FROM region
        WHERE R_NAME = 'ASIA';
    """)
    asia_region = cursor.fetchall()
    region_dict = {region[0]: region[1] for region in asia_region}

# Retrieve data from MongoDB
nation_coll = mongo_db['nation']
supplier_coll = mongo_db['supplier']
orders_coll = mongo_db['orders']

# Retrieve customers from Redis and convert to DataFrame
customer_data_json = redis_db.get('customer')
customers_df = pd.read_json(customer_data_json, orient='records')

# Query MongoDB for nations and suppliers in ASIA
asia_nations = list(nation_coll.find({'N_REGIONKEY': {'$in': list(region_dict.keys())}}))
asia_nation_keys = [nation['N_NATIONKEY'] for nation in asia_nations]
asia_suppliers = list(supplier_coll.find({'S_NATIONKEY': {'$in': asia_nation_keys}}))
asia_supplier_keys = [supplier['S_SUPPKEY'] for supplier in asia_suppliers]

# Query MySQL for lineitem as pandas DataFrame
sql_query = """
    SELECT 
        L_SUPPKEY, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
    FROM 
        lineitem 
    WHERE 
        L_SHIPDATE BETWEEN '1990-01-01' AND '1995-01-01'
    GROUP BY 
        L_SUPPKEY;
"""
lineitem_df = pd.read_sql_query(sql_query, mysql_conn)

# Filter data for ASIA suppliers
asia_lineitem_df = lineitem_df[lineitem_df['L_SUPPKEY'].isin(asia_supplier_keys)]

# Merge data to get the nation key
suppliers_nations_df = pd.DataFrame(asia_suppliers)[['S_SUPPKEY', 'S_NATIONKEY']]
final_df = pd.merge(asia_lineitem_df, suppliers_nations_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Calculate total revenue per nation
final_df = final_df.groupby('S_NATIONKEY')['revenue'].sum().reset_index()
final_df.rename(columns={'S_NATIONKEY': 'N_NATIONKEY'}, inplace=True)

# Merge with nations to get nation names
nations_df = pd.DataFrame(asia_nations)[['N_NATIONKEY', 'N_NAME']]
result_df = pd.merge(final_df, nations_df, on='N_NATIONKEY').sort_values(by='revenue', ascending=False)

# Select the required columns and write to CSV
output_df = result_df[['N_NAME', 'revenue']]
output_df.to_csv('query_output.csv', index=False)

# Close all connections
mysql_conn.close()
mongo_client.close()
