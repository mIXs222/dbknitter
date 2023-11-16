# Importing all necessary libraries
import pandas as pd
import pymysql
from pymongo import MongoClient

# Function to connect and query MySQL database
def query_mysql():
    conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    cursor = conn.cursor()
    query = """
            SELECT
                S_SUPPKEY as SUPP_KEY,
                S_NATIONKEY as NAT_KEY
            FROM
                supplier,
                nation
            WHERE
                S_NATIONKEY = N_NATIONKEY
                AND N_NAME = 'GERMANY'
            """
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return pd.DataFrame(list(result))

# Function to connect and query MongoDB
def query_mongodb():
    client = MongoClient("mongodb", 27017)
    db = client['tpch']
    partsupp = db.partsupp
    query = {}
    result = partsupp.find(query)
    return pd.DataFrame(list(result))

def main():
    mysql_data = query_mysql()
    mongodb_data = query_mongodb()
    merged_data = pd.merge(mongodb_data, mysql_data, left_on='PS_SUPPKEY', right_on='SUPP_KEY')
    
    merged_data['VALUE'] = merged_data['PS_SUPPLYCOST'] * merged_data['PS_AVAILQTY']
    output_data = merged_data.groupby(['PS_PARTKEY'])['VALUE'].sum().reset_index()
    output_data.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
