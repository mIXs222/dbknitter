import pandas as pd
from pymongo import MongoClient
import mysql.connector
from mysql.connector import Error
import os

class DataFetcher:
    def __init__(self,mongo_uri,mongo_db,mysql_host,mysql_db,mysql_user,mysql_pw):
        self.mongo_client = MongoClient(mongo_uri)
        self.mongo_db = self.mongo_client[mongo_db]

        self.mysql_conn = mysql.connector.connect(host=mysql_host,
                                                  database=mysql_db,
                                                  user=mysql_user,
                                                  password=mysql_pw)
        self.mysql_cursor = self.mysql_conn.cursor()

    def fetch_data_from_mongo(self,collection):
        cursor = self.mongo_db[collection].find()
        df =  pd.DataFrame(list(cursor))
        return df

    def fetch_data_from_mysql(self,table):
        self.mysql_cursor.execute(f"SELECT * FROM {table}")
        df = pd.DataFrame(self.mysql_cursor.fetchall())
        df.columns = [i[0] for i in self.mysql_cursor.description]
        return df

    def close_connections(self):
        self.mongo_client.close()
        self.mysql_conn.close()


df = DataFetcher("mongodb://mongodb:27017",
                 "tpch",
                 "mysql",
                 "tpch",
                 "root",
                 "my-secret-pw")

supplier = df.fetch_data_from_mysql('SUPPLIER')
nation = df.fetch_data_from_mysql('NATION')

partsupp = df.fetch_data_from_mongo('partsupp')
customer = df.fetch_data_from_mongo('customer')
orders = df.fetch_data_from_mongo('orders')
lineitem = df.fetch_data_from_mongo('lineitem')

#close connections
df.close_connections()

# Proceed with your pandas data manipulations and filtering queries as Sequelize code provided above, example pandas code would go something like below - replace this with the sequelize code converted pandas functions:
# pd.merge(supplier, nation, ...)
# pd.merge(result, partsupp, ...)
#...
# write the final dataframe to csv
final_df.to_csv("./query_output.csv", index=False)
