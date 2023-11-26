# query_exec.py
import pandas as pd
import pymysql
import pymongo
import direct_redis
from datetime import datetime

# Function to connect to MySQL database and retrieve supplier and customer data
def get_mysql_data():
    conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')
    supplier_query = "SELECT * FROM supplier"
    customer_query = "SELECT * FROM customer"

    df_supplier = pd.read_sql(supplier_query, conn)
    df_customer = pd.read_sql(customer_query, conn)
    conn.close()

    return df_supplier, df_customer

# Function to connect to MongoDB database and retrieve nation data
def get_mongodb_data():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    df_nation = pd.DataFrame(list(db.nation.find({})))
    client.close()
    return df_nation

# Function to connect to Redis database and retrieve orders and lineitem data
def get_redis_data():
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    df_orders = pd.read_json(r.get('orders'))
    df_lineitem = pd.read_json(r.get('lineitem'))
    return df_orders, df_lineitem

# Main execution function
def main():
    # Get data from different databases
    df_supplier, df_customer = get_mysql_data()
    df_nation = get_mongodb_data()
    df_orders, df_lineitem = get_redis_data()

    # Merge dataframes to simulate the SQL join operations
    df_supplier_nation = df_supplier.merge(df_nation.rename(columns={'N_NAME': 'SUPP_NATION', 'N_NATIONKEY': 'S_NATIONKEY'}), on='S_NATIONKEY')
    df_customer_nation = df_customer.merge(df_nation.rename(columns={'N_NAME': 'CUST_NATION', 'N_NATIONKEY': 'C_NATIONKEY'}), on='C_NATIONKEY')
    df_merged = (
        df_lineitem
        .merge(df_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
        .merge(df_supplier_nation, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
        .merge(df_customer_nation, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    )

    # Filter based on the SQL WHERE clause conditions
    df_filtered = df_merged.loc[
        ((df_merged['SUPP_NATION'] == 'JAPAN') & (df_merged['CUST_NATION'] == 'INDIA')) |
        ((df_merged['SUPP_NATION'] == 'INDIA') & (df_merged['CUST_NATION'] == 'JAPAN')) &
        (df_merged['L_SHIPDATE'] >= datetime(1995, 1, 1)) &
        (df_merged['L_SHIPDATE'] <= datetime(1996, 12, 31))
    ]

    # Calculate volume and year
    df_filtered['VOLUME'] = df_filtered['L_EXTENDEDPRICE'] * (1 - df_filtered['L_DISCOUNT'])
    df_filtered['L_YEAR'] = df_filtered['L_SHIPDATE'].dt.year

    # Group by the required fields
    result = df_filtered.groupby(['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])['VOLUME'].sum().reset_index()
    result = result.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

    # Write the output to CSV
    result.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
