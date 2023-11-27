import pymysql
import pymongo
import pandas as pd
import csv
from datetime import datetime
from direct_redis import DirectRedis

# MySQL connection and query
def get_mysql_data():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
    with connection:
        cursor = connection.cursor()
        query = """
            SELECT O_CUSTKEY, L_SUPPKEY, L_EXTENDEDPRICE, L_DISCOUNT 
            FROM orders JOIN lineitem ON O_ORDERKEY = L_ORDERKEY 
            WHERE O_ORDERDATE >= '1990-01-01' AND O_ORDERDATE < '1995-01-01'
        """
        cursor.execute(query)
        orders_lineitems = cursor.fetchall()
    return orders_lineitems

# MongoDB connection and query
def get_mongodb_data():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client.tpch
    supplier = list(db.supplier.find({}, {'_id': 0, 'S_SUPPKEY': 1, 'S_NATIONKEY': 1}))
    customer = list(db.customer.find({}, {'_id': 0, 'C_CUSTKEY': 1, 'C_NATIONKEY': 1}))
    return supplier, customer

# Redis connection and query
def get_redis_data():
    redis = DirectRedis(host="redis", port=6379, db=0)
    nations = pd.read_json(redis.get('nation').decode('utf-8'))
    regions = pd.read_json(redis.get('region').decode('utf-8'))
    asia_nations = regions[regions['R_NAME'] == 'ASIA'].merge(nations, left_on='R_REGIONKEY', right_on='N_REGIONKEY')['N_NATIONKEY']
    return asia_nations.tolist()

# Main script
def main():
    # Get data from databases
    orders_lineitems = get_mysql_data()
    suppliers, customers = get_mongodb_data()
    asia_nations = get_redis_data()

    # Convert to DataFrames
    df_ol = pd.DataFrame(orders_lineitems, columns=['O_CUSTKEY', 'L_SUPPKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT'])
    df_supplier = pd.DataFrame(suppliers)
    df_customer = pd.DataFrame(customers)

    # Compute revenue
    df_ol['REVENUE'] = df_ol['L_EXTENDEDPRICE'] * (1 - df_ol['L_DISCOUNT'])

    # Filter by ASIA suppliers and customers
    df_supplier_asia = df_supplier[df_supplier['S_NATIONKEY'].isin(asia_nations)]
    df_customer_asia = df_customer[df_customer['C_NATIONKEY'].isin(asia_nations)]

    # Join datasets
    df_asia = (
        df_ol.merge(df_supplier_asia, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
             .merge(df_customer_asia, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    )

    # Sum revenue by nation
    results = (
        df_asia.groupby(df_customer_asia['C_NATIONKEY'], as_index=False)['REVENUE']
        .sum()
        .sort_values(by='REVENUE', ascending=False)
    )

    # Save results to a CSV
    results.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

if __name__ == "__main__":
    main()
