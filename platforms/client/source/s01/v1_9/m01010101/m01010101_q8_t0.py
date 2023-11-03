import mysql.connector
import pandas as pd
import numpy as np
from pymongo import MongoClient


# Define MongoDB function
def read_mongo(db, collection, query={}, host='mongodb', port=27017):
    """ Read from Mongo and Store into DataFrame """
    # Connect to MongoDB
    client = MongoClient(host, port)
    # Make a query to the specific DB and Collection
    cursor = client[db][collection].find(query)
    # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(cursor))
    # Close connection
    client.close()
    return df


# Define MySQL function
def read_mysql(db_name, query, username='root', password='my-secret-pw', host='mysql'):
    cnx = mysql.connector.connect(user=username, password=password, host=host, database=db_name)
    df = pd.read_sql(query, cnx)
    cnx.close()
    return df


# Define queries to execute
query_nation = "SELECT * FROM NATION"
query_part = "SELECT * FROM PART WHERE P_TYPE = 'SMALL PLATED COPPER'"
query_partsupp = "SELECT * FROM PARTSUPP"
query_orders = "SELECT * FROM ORDERS WHERE O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'"

# Fetch data from MySQL
nation_df = read_mysql('tpch', query_nation)
part_df = read_mysql('tpch', query_part)
partsupp_df = read_mysql('tpch', query_partsupp)
orders_df = read_mysql('tpch', query_orders)

# Fetch data from MongoDB
region_df = read_mongo('tpch', 'region')
supplier_df = read_mongo('tpch', 'supplier')
customer_df = read_mongo('tpch', 'customer')
lineitem_df = read_mongo('tpch', 'lineitem')

# Perform joins and calculations as per the given SQL query
result_df = (part_df
             .merge(partsupp_df, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')
             .merge(supplier_df, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
             .merge(nation_df.rename(columns={'N_NATIONKEY': 'S_NATIONKEY', 'N_NAME': 'NATION'}), how='inner', left_on='S_NATIONKEY', right_on='S_NATIONKEY')
             .merge(lineitem_df, how='inner', on=['PS_PARTKEY', 'S_SUPPKEY'])
             .merge(orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
             .merge(customer_df.rename(columns={'C_NATIONKEY': 'NATION'}), how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
             .merge(nation_df.rename(columns={'N_NATIONKEY': 'NATION'}), how='inner', on='NATION')
             .merge(region_df, how='inner', left_on='N_REGIONKEY', right_on='R_REGIONKEY')
             .assign(O_YEAR=lambda x: pd.DatetimeIndex(x['O_ORDERDATE']).year)
             .assign(VOLUME=lambda x: x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']))
             .groupby('O_YEAR')
             .apply(lambda x: pd.Series({
                 'MKT_SHARE': (x[x['NATION']=='INDIA']['VOLUME'].sum()) / (x['VOLUME'].sum())
             }))
             .reset_index()
             .sort_values('O_YEAR'))

result_df.to_csv('query_output.csv', index=False)
