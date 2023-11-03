import pandas as pd
import pymongo
from pymongo import MongoClient
import mysql.connector
from mysql.connector import Error

def create_mysql_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL connection is successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def read_mongo_data(host_name, port, db_name, collection_name):
    client = MongoClient(host=host_name, port=port)
    db = client[db_name]
    collection = db[collection_name]
    data = pd.DataFrame(list(collection.find()))
    return data

def query_data(partsupp, part, supplier):
    partsupp_part = pd.merge(partsupp, part, left_on="PS_PARTKEY", right_on="P_PARTKEY")
    partsupp_part = partsupp_part[partsupp_part['P_BRAND'] != 'Brand#45']
    partsupp_part = partsupp_part[~partsupp_part['P_TYPE'].str.startswith("MEDIUM POLISHED")]
    partsupp_part = partsupp_part[partsupp_part['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9])]
    supplier = supplier[supplier['S_COMMENT'].str.contains("Customer Complaints")]
    result = partsupp_part[~partsupp_part['PS_SUPPKEY'].isin(supplier['S_SUPPKEY'])]
    result = result.groupby(['P_BRAND','P_TYPE','P_SIZE'])['PS_SUPPKEY'].nunique().reset_index(name="SUPPLIER_CNT")
    result = result.sort_values(['SUPPLIER_CNT','P_BRAND','P_TYPE','P_SIZE'], ascending=[False,True,True,True])
    result.to_csv('query_output.csv', index=False)

# Create MySQL connection
connection = create_mysql_connection("mysql", "root", "my-secret-pw", "tpch")

# Read data from MySQL
part = pd.read_sql_query("SELECT * FROM PART", connection)
partsupp = pd.read_sql_query("SELECT * FROM PARTSUPP", connection)

# Read data from MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
supplier = read_mongo_data("mongodb", 27017, "tpch", "supplier")

# Execute query
query_data(partsupp, part, supplier)
