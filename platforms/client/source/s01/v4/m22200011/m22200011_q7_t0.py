# query.py
import pymysql
import pymongo
import pandas as pd
import direct_redis
from datetime import datetime

# Function to connect to MySQL and return data
def get_mysql_data():
    conn = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch'
    )
    sql = """
    SELECT
        S_SUPPKEY,
        S_NATIONKEY
    FROM
        supplier
    UNION ALL
    SELECT
        C_CUSTKEY AS S_SUPPKEY,
        C_NATIONKEY
    FROM
        customer
    """
    supplier_customer_df = pd.read_sql(sql, conn)
    conn.close()
    return supplier_customer_df

# Function to connect to MongoDB and return data
def get_mongodb_data():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    orders = pd.DataFrame(list(db.orders.find(
        {'O_ORDERDATE': {'$gte': datetime(1995, 1, 1), '$lte': datetime(1996, 12, 31)}},
        {'_id': 0, 'O_ORDERKEY': 1, 'O_CUSTKEY': 1}
    )))
    lineitem = pd.DataFrame(list(db.lineitem.find(
        {'L_SHIPDATE': {'$gte': datetime(1995, 1, 1), '$lte': datetime(1996, 12, 31)}},
        {'_id': 0, 'L_ORDERKEY': 1, 'L_SUPPKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1, 'L_SHIPDATE': 1}
    )))
    client.close()
    return orders, lineitem

# Function to connect to Redis and return data
def get_redis_data():
    client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    nation_df = pd.read_msgpack(client.get('nation'))
    return nation_df

# Retrieve data from databases
supplier_customer_df = get_mysql_data()
orders_df, lineitem_df = get_mongodb_data()
nation_df = get_redis_data()

# Perform the operations to get the expected result
# Merge the dataframes with appropriate operations to emulate SQL joins and other operations
nation_df.rename(columns={'N_NATIONKEY': 'S_NATIONKEY'}, inplace=True)
sc_nation = pd.merge(supplier_customer_df, nation_df, on='S_NATIONKEY')
orders_df.rename(columns={'O_CUSTKEY': 'S_SUPPKEY'}, inplace=True)
so_lineitem = pd.merge(pd.merge(lineitem_df, orders_df, on='O_ORDERKEY'), sc_nation, on='S_SUPPKEY')

# Filter for Japan and India
so_lineitem = so_lineitem[
    ((so_lineitem['N_NAME'] == 'JAPAN') & (so_lineitem['C_NATIONKEY'] == nation_df[nation_df['N_NAME'] == 'INDIA'].iloc[0]['S_NATIONKEY'])) |
    ((so_lineitem['N_NAME'] == 'INDIA') & (so_lineitem['C_NATIONKEY'] == nation_df[nation_df['N_NAME'] == 'JAPAN'].iloc[0]['S_NATIONKEY']))
]

# Calculate VOLUME and L_YEAR
so_lineitem['VOLUME'] = so_lineitem['L_EXTENDEDPRICE'] * (1 - so_lineitem['L_DISCOUNT'])
so_lineitem['L_YEAR'] = so_lineitem['L_SHIPDATE'].dt.year

# Group by SUPP_NATION, CUST_NATION, L_YEAR
result_df = so_lineitem.groupby(['N_NAME', 'C_NATIONKEY', 'L_YEAR'])['VOLUME'].sum().reset_index()
result_df.rename(columns={'N_NAME': 'SUPP_NATION', 'C_NATIONKEY': 'CUST_NATION'}, inplace=True)

# Merge to get the nation names for customers
result_df = pd.merge(result_df, nation_df.rename(columns={'S_NATIONKEY': 'CUST_NATION'}), on='CUST_NATION')
result_df.rename(columns={'N_NAME': 'CUST_NATION'}, inplace=True)

# Final select and order by
result_df = result_df[['SUPP_NATION', 'CUST_NATION', 'L_YEAR', 'VOLUME']]
result_df.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'], inplace=True)

# Write the result to a CSV file
result_df.to_csv('query_output.csv', index=False)
