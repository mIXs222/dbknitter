# query_code.py

import pandas as pd
import pymysql
import pymongo
from direct_redis import DirectRedis
import csv

# Function to connect to MySQL and fetch supplier and partsupp data
def get_mysql_data():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT s.S_SUPPKEY, s.S_NAME, ps.PS_PARTKEY, ps.PS_AVAILQTY
                FROM supplier s
                JOIN partsupp ps ON s.S_SUPPKEY = ps.PS_SUPPKEY
            """)
            supplier_partsupp = cursor.fetchall()
            
            # Create a DataFrame from the tuple of tuples
            df_supplier_partsupp = pd.DataFrame(list(supplier_partsupp), columns=['S_SUPPKEY', 'S_NAME', 'PS_PARTKEY', 'PS_AVAILQTY'])
            
    return df_supplier_partsupp

# Function to connect to MongoDB and fetch nation data 
def get_mongodb_data():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client['tpch']
    
    # Convert MongoDB collections to DataFrames
    df_nation = pd.DataFrame(list(db['nation'].find()))
    df_part = pd.DataFrame(list(db['part'].find()))
    
    return df_nation, df_part

# Function to connect to Redis and fetch lineitem data
def get_redis_data():
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    
    # Fetch lineitem data as a Pandas DataFrame
    df_lineitem = pd.DataFrame(eval(redis_client.get('lineitem')))
    
    return df_lineitem

# Fetch data from respective databases
df_supplier_partsupp = get_mysql_data()
df_nation, df_part = get_mongodb_data()
df_lineitem = get_redis_data()

# Perform operations to match the English query
forest_parts = df_part[df_part['P_NAME'].str.contains('forest')]

# Filter lineitem for the specified date range and location (CANADA)
filtered_lineitem = df_lineitem[
    (df_lineitem['L_SHIPDATE'] >= '1994-01-01') &
    (df_lineitem['L_SHIPDATE'] < '1995-01-01') &
    (df_lineitem['L_SUPPKEY'].isin(df_nation[df_nation['N_NAME'] == 'CANADA']['N_NATIONKEY']))
]

# Calculate the quantity shipped by supplier for forest parts
supplier_forest_parts_qty = filtered_lineitem[
    filtered_lineitem['L_PARTKEY'].isin(forest_parts['P_PARTKEY'])
].groupby('L_SUPPKEY')[['L_QUANTITY']].sum().reset_index()

supplier_forest_parts_qty.rename(columns={'L_SUPPKEY': 'S_SUPPKEY', 'L_QUANTITY': 'FOREST_SHIPPED_QTY'}, inplace=True)

# Identify suppliers with more than 50% of shipped parts being forest parts
result = df_supplier_partsupp.merge(supplier_forest_parts_qty, on='S_SUPPKEY')
result['EXCESS'] = result['FOREST_SHIPPED_QTY'] > 0.5 * result['PS_AVAILQTY']

# Filter suppliers with excess
suppliers_with_excess = result[result['EXCESS']]

# Write final data to CSV
suppliers_with_excess.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
