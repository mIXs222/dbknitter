import pymysql
import pymongo
import pandas as pd
from datetime import datetime

# Function to connect to MySQL and return a DataFrame for the given query
def get_data_from_mysql(query):
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    try:
        return pd.read_sql_query(query, connection)
    finally:
        connection.close()

# Function to connect to MongoDB and return a DataFrame for the given collection
def get_data_from_mongodb(collection_name):
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    collection = db[collection_name]
    data = list(collection.find({}))
    return pd.DataFrame(data)

# Getting the data from MySQL
mysql_tables = ['nation', 'region', 'part', 'supplier']
mysql_data = {}
for table in mysql_tables:
    mysql_query = f"SELECT * FROM {table}"
    mysql_data[table] = get_data_from_mysql(mysql_query)

# Getting the data from MongoDB
mongodb_tables = ['customer', 'orders', 'lineitem']
mongodb_data = {}
for collection in mongodb_tables:
    mongodb_data[collection] = get_data_from_mongodb(collection)

# Merge MySQL and MongoDB data into a single DataFrame
merged_data = (
    mongodb_data['lineitem']
    .merge(
        mysql_data['part'],
        left_on='L_PARTKEY',
        right_on='P_PARTKEY'
    )
    .merge(
        mysql_data['supplier'],
        left_on='L_SUPPKEY',
        right_on='S_SUPPKEY'
    )
    .merge(
        mongodb_data['orders'],
        left_on='L_ORDERKEY',
        right_on='O_ORDERKEY'
    )
    .merge(
        mongodb_data['customer'],
        left_on='O_CUSTKEY',
        right_on='C_CUSTKEY'
    )
    .merge(
        mysql_data['nation'],
        left_on='S_NATIONKEY',
        right_on='N_NATIONKEY'
    )
    .merge(
        mysql_data['region'],
        left_on='N_REGIONKEY',
        right_on='R_REGIONKEY'
    )
)

# Apply the query conditions and calculations
filtered_data = merged_data.query(
    "R_NAME == 'ASIA' and P_TYPE == 'SMALL PLATED COPPER' and "
    "'1995-01-01' <= O_ORDERDATE <= '1996-12-31'"
)
filtered_data['VOLUME'] = filtered_data['L_EXTENDEDPRICE'] * (1 - filtered_data['L_DISCOUNT'])
filtered_data['O_YEAR'] = filtered_data['O_ORDERDATE'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").strftime('%Y'))

# Group by and calculate market share
result = (
    filtered_data.groupby('O_YEAR')
    .apply(lambda x: pd.Series({
        'MKT_SHARE': (x[x['N_NAME'] == 'INDIA']['VOLUME'].sum() / x['VOLUME'].sum())
}))
    .reset_index()
    .sort_values('O_YEAR')
)

# Writing results to a CSV file
result.to_csv('query_output.csv', index=False)
