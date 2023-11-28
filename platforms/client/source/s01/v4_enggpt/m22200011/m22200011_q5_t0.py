# File: query_data.py

import pymysql
import pymongo
import pandas as pd
import direct_redis

# Connecting to the MySQL database and retrieve relevant data
def get_mysql_data():
    connection = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        database='tpch',
        cursorclass=pymysql.cursors.Cursor
    )
    try:
        with connection.cursor() as cursor:
            # Retrieve customers from the 'ASIA' region
            query_customer = """
            SELECT c.C_CUSTKEY, c.C_NATIONKEY
            FROM customer c
            INNER JOIN nation n ON c.C_NATIONKEY = n.N_NATIONKEY
            INNER JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
            WHERE r.R_NAME = 'ASIA';
            """
            cursor.execute(query_customer)
            customers = cursor.fetchall()
            customers_df = pd.DataFrame(customers, columns=['C_CUSTKEY', 'C_NATIONKEY'])
            
            # Retrieve suppliers and their nations
            query_supplier_nation = """
            SELECT s.S_SUPPKEY, n.N_NAME
            FROM supplier s
            INNER JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
            """
            cursor.execute(query_supplier_nation)
            supplier_nations = cursor.fetchall()
            supplier_nations_df = pd.DataFrame(supplier_nations, columns=['S_SUPPKEY', 'N_NAME'])
            
            return customers_df, supplier_nations_df
    finally:
        connection.close()

# Connecting to the MongoDB database and retrieve relevant data
def get_mongodb_data(customers_df):
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    
    # Convert the customer keys from SQL part to list for querying MongoDB
    customer_keys = customers_df['C_CUSTKEY'].tolist()
    
    # Retrieve orders by customers from 'ASIA'
    orders = list(db.orders.find({
        'O_CUSTKEY': {'$in': customer_keys},
        'O_ORDERDATE': {'$gte': '1990-01-01', '$lte': '1994-12-31'}
    }, {'_id': 0}))
    orders_df = pd.DataFrame(orders)
    
    # Retrieve lineitems corresponding to the orders
    order_keys = orders_df['O_ORDERKEY'].tolist()
    lineitems = list(db.lineitem.find({
        'L_ORDERKEY': {'$in': order_keys}
    }, {'_id': 0}))
    lineitems_df = pd.DataFrame(lineitems)
    
    return orders_df, lineitems_df

# Connecting to the Redis database and retrieve relevant data
def get_redis_data():
    client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    # Retrieve 'nation' and 'region' tables
    nation_df = pd.read_json(client.get('nation'))
    region_df = pd.read_json(client.get('region'))
    
    # Filter out the nations from 'ASIA' region
    asia_region_key = region_df[region_df['R_NAME'] == 'ASIA']['R_REGIONKEY'].iloc[0]
    nations_in_asia_df = nation_df[nation_df['N_REGIONKEY'] == asia_region_key]
    
    return nations_in_asia_df

# Main function to gather data from different databases and process it
def main():
    # Retrieve data from MySQL
    customers_df, supplier_nations_df = get_mysql_data()
    # Retrieve data from MongoDB
    orders_df, lineitems_df = get_mongodb_data(customers_df)
    # Retrieve data from Redis
    nations_in_asia_df = get_redis_data()
    
    # Merge the dataframes to compose the final result
    result_df = (
        lineitems_df
        .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
        .merge(supplier_nations_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
        .merge(nations_in_asia_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    )
    
    # Calculate the revenue (extended price * (1 - discount))
    result_df['REVENUE'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])
    
    # Group by nation name and order by total revenue in descending order
    final_result = (
        result_df
        .groupby('N_NAME')['REVENUE']
        .sum()
        .reset_index()
        .rename(columns={'N_NAME': 'NATION_NAME'})
        .sort_values(by='REVENUE', ascending=False)
    )
    
    # Write to CSV file
    final_result.to_csv('query_output.csv', index=False)
    
if __name__ == '__main__':
    main()
