import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MongoDB Connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_nation = mongo_db["nation"]

# Redis Connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetch nation data from MongoDB
nation_data = pd.DataFrame(list(mongo_nation.find({}, {'_id': 0})))

# Fetch data from Redis and convert to Pandas DataFrame
supplier_data = pd.read_json(redis_conn.get('supplier'), orient='records')
customer_data = pd.read_json(redis_conn.get('customer'), orient='records')

# Filter nations for India and Japan
nation_filter = nation_data[(nation_data['N_NAME'] == 'INDIA') | (nation_data['N_NAME'] == 'JAPAN')]
suppliers_filtered = supplier_data[supplier_data['S_NATIONKEY'].isin(nation_filter['N_NATIONKEY'])]
customers_filtered = customer_data[customer_data['C_NATIONKEY'].isin(nation_filter['N_NATIONKEY'])]

# MySQL queries to get lineitem and orders data
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT
            L_ORDERKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE, O_ORDERKEY,
            O_CUSTKEY
        FROM lineitem JOIN orders ON L_ORDERKEY = O_ORDERKEY
        WHERE
            L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31';
    """)
    lineitem_orders_data = pd.DataFrame(cursor.fetchall(), columns=[
        'L_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_SHIPDATE',
        'O_ORDERKEY', 'O_CUSTKEY'
    ])

mysql_conn.close()

# Merge data frames: lineitem-orders with customers and suppliers
result = lineitem_orders_data.merge(customers_filtered, left_on='O_CUSTKEY', right_on='C_CUSTKEY') \
    .merge(suppliers_filtered, left_on='O_CUSTKEY', right_on='S_SUPPKEY')

# Add Year and Revenue columns
result['Year'] = pd.to_datetime(result['L_SHIPDATE']).dt.year
result['Revenue'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])

# Filter India-Japan shipments
result = result[((result['S_NATIONKEY'] == nation_filter.iloc[0]['N_NATIONKEY']) 
                & (result['C_NATIONKEY'] == nation_filter.iloc[1]['N_NATIONKEY'])) 
                | ((result['S_NATIONKEY'] == nation_filter.iloc[1]['N_NATIONKEY']) 
                & (result['C_NATIONKEY'] == nation_filter.iloc[0]['N_NATIONKEY']))]

# Select required columns
final_result = result[['S_NATIONKEY', 'C_NATIONKEY', 'Year', 'Revenue']]

# Order by Supplier nation, Customer nation, and Year
final_result.sort_values(by=['S_NATIONKEY', 'C_NATIONKEY', 'Year'], inplace=True)

# Write to CSV
final_result.to_csv('query_output.csv', index=False)
