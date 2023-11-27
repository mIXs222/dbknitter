from pymongo import MongoClient
import pandas as pd
import mysql.connector

def fetch_data_from_mongodb(hostname, port, db_name, collection_name):
    client = MongoClient(hostname, port)
    db = client[db_name]
    cursor = db[collection_name].find()
    data =  pd.DataFrame(list(cursor))
    return data

def main():
    # Fetch data from MongoDB
    customer_data = fetch_data_from_mongodb("mongodb", 27017, "tpch", "customer")
    orders_data = fetch_data_from_mongodb("mongodb", 27017, "tpch", "orders")
    lineitem_data = fetch_data_from_mongodb("mongodb", 27017, "tpch", "lineitem")
    
    # Merge and Query the data
    merged_data = pd.merge(customer_data, orders_data, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    merged_data = pd.merge(merged_data, lineitem_data, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    
    queried_data = merged_data.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']).sum()
    queried_data = queried_data[queried_data['L_QUANTITY'] > 300]
    queried_data = queried_data.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])
    
    # Write to CSV
    queried_data.to_csv('query_output.csv')

if __name__ == "__main__":
    main()
