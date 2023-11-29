import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch')

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Load Redis nation table into pandas DataFrame
nation_df = pd.read_json(redis_conn.get('nation'))

# MySQL queries for Indian and Japanese supplier and customer information
supplier_query = """
SELECT S_SUPPKEY, S_NATIONKEY FROM supplier
WHERE S_NATIONKEY IN (
    SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'INDIA' OR N_NAME = 'JAPAN'
);"""

customer_query = """
SELECT C_CUSTKEY, C_NATIONKEY FROM customer
WHERE C_NATIONKEY IN (
    SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'INDIA' OR N_NAME = 'JAPAN'
);"""

with mysql_conn.cursor() as cursor:
    # Execute supplier query
    cursor.execute(supplier_query)
    suppliers = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_NATIONKEY'])

    # Execute customer query
    cursor.execute(customer_query)
    customers = pd.DataFrame(cursor.fetchall(), columns=['C_CUSTKEY', 'C_NATIONKEY'])

# MongoDB query for lineitem and orders
lineitem_collection = mongo_db['lineitem']
orders_collection = mongo_db['orders']

# Lineitem and order pipeline
pipeline = [
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'L_ORDERKEY',
            'foreignField': 'O_ORDERKEY',
            'as': 'orders'
        }
    },
    {'$unwind': '$orders'},
    {
        '$match': {
            'orders.O_ORDERDATE': {'$gte': pd.Timestamp('1995-01-01'), '$lte': pd.Timestamp('1996-12-31')}
        }
    }
]

lineitems_data = list(lineitem_collection.aggregate(pipeline))
lineitems_df = pd.DataFrame(lineitems_data)

# Join dataframes
joined_df = lineitems_df.merge(suppliers, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
joined_df = joined_df.merge(customers, how='inner', left_on='orders.O_CUSTKEY', right_on='C_CUSTKEY')

# Calculate gross discounted revenues and year
joined_df['REVENUE'] = joined_df['L_EXTENDEDPRICE'] * (1 - joined_df['L_DISCOUNT'])
joined_df['L_YEAR'] = joined_df['orders.O_ORDERDATE'].dt.year

# Filtering nations
joined_df = joined_df[(joined_df['S_NATIONKEY'] != joined_df['C_NATIONKEY']) &
                      ((joined_df['S_NATIONKEY'].isin(nation_df[nation_df['N_NAME'] == 'INDIA']['N_NATIONKEY'])) |
                       (joined_df['S_NATIONKEY'].isin(nation_df[nation_df['N_NAME'] == 'JAPAN']['N_NATIONKEY'])))]

# Rename columns to match the specified output
joined_df.rename(columns={'C_NATIONKEY': 'CUST_NATION', 'S_NATIONKEY': 'SUPP_NATION'}, inplace=True)

# Select specific columns and sort
output_df = joined_df[['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION']]
output_df = output_df.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Write to output file
output_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
