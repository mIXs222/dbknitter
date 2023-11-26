# query.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Function to connect to MySQL and get data
def get_mysql_data():
    connection = pymysql.connect(
        host='mysql', user='root', password='my-secret-pw', db='tpch'
    )
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT N_NATIONKEY, N_NAME, N_REGIONKEY FROM nation;"
        )
        nation = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY'])
        
        cursor.execute(
            "SELECT S_SUPPKEY, S_NATIONKEY FROM supplier;"
        )
        supplier = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_NATIONKEY'])
        
        cursor.execute(
            """
            SELECT O_ORDERKEY, O_CUSTKEY
            FROM orders
            WHERE O_ORDERDATE >= '1990-01-01' AND O_ORDERDATE < '1995-01-01';
            """
        )
        orders = pd.DataFrame(cursor.fetchall(), columns=['O_ORDERKEY', 'O_CUSTKEY'])
    return nation, supplier, orders

# Function to connect to MongoDB and get data
def get_mongodb_data():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    
    region = pd.DataFrame(list(db.region.find({'R_NAME': 'ASIA'}, {'_id': 0})))
    lineitem = pd.DataFrame(list(db.lineitem.find({}, {'_id': 0})))
    return region, lineitem

# Function to get data from Redis
def get_redis_data():
    client = DirectRedis(host='redis', port=6379, db=0)
    customer_df = pd.read_dataframe(client.get('customer'))
    return customer_df

# Get data from MySQL
nation, supplier, orders = get_mysql_data()

# Get data from MongoDB
region, lineitem = get_mongodb_data()

# Get data from Redis
customer = get_redis_data()

# Data processing and merging
merged_data = (
    customer.merge(orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    .merge(lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(supplier, left_on=['C_NATIONKEY', 'L_SUPPKEY'], right_on=['S_NATIONKEY', 'S_SUPPKEY'])
    .merge(nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
    .merge(region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
)

# Calculating results as per the SQL query
result = merged_data.groupby('N_NAME').apply(
    lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])).sum()
).reset_index(name='REVENUE')

# Sorting the results
result = result.sort_values(by='REVENUE', ascending=False)

# Writing the output to a CSV file
result.to_csv('query_output.csv', index=False)
