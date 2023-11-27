# Import necessary libraries
import pandas as pd
import pymysql
from pymongo import MongoClient
import direct_redis

# Function to connect to MySQL and fetch customer data from the tpch database
def fetch_mysql_data():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    try:
        with connection.cursor() as cursor:
            query = "SELECT C_CUSTKEY, C_NATIONKEY FROM customer"
            cursor.execute(query)
            results = cursor.fetchall()
    finally:
        connection.close()
    return pd.DataFrame(results, columns=['C_CUSTKEY', 'C_NATIONKEY'])

# Function to connect to MongoDB and fetch region and lineitem data from the tpch database
def fetch_mongodb_data():
    client = MongoClient('mongodb', 27017)
    db = client['tpch']

    # Fetching region data where R_NAME is 'ASIA'
    region_data = list(db.region.find({'R_NAME': 'ASIA'}, {'_id': False}))

    # Query for fetching lineitem data
    query = {
        'L_SHIPDATE': {'$gte': '1990-01-01', '$lte': '1995-01-01'}
    }
    lineitem_data = list(db.lineitem.find(query, {'_id': False}))

    client.close()
    return pd.DataFrame(region_data), pd.DataFrame(lineitem_data)

# Function to connect to Redis and fetch nation data
def fetch_redis_data():
    client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    
    nation_data = client.get('nation')
    if nation_data is not None:
        nation_df = pd.read_json(nation_data)
    else:
        nation_df = pd.DataFrame()
    client.close()
    return nation_df


def main():
    # Fetch data from MySQL, MongoDB and Redis
    customer_df = fetch_mysql_data()
    region_df, lineitem_df = fetch_mongodb_data()
    nation_df = fetch_redis_data()
    
    # Merge dataframes based on keys and filter for nations in ASIA
    asia_nation_keys = region_df.merge(nation_df, left_on='R_REGIONKEY', right_on='N_REGIONKEY')['N_NATIONKEY']
    asia_customers = customer_df[customer_df['C_NATIONKEY'].isin(asia_nation_keys)]
    
    # Calculate revenue and merge dataframes
    lineitem_df['revenue'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
    qualifying_lineitems = lineitem_df.merge(asia_customers, left_on='L_SUPPKEY', right_on='C_CUSTKEY')
    
    # Aggregate revenue by nation
    revenue_by_nation = qualifying_lineitems.groupby('C_NATIONKEY')['revenue'].sum().reset_index()
    revenue_by_nation = revenue_by_nation.sort_values(by='revenue', ascending=False)
    
    # Merge with nation names and select the required columns
    final_result = revenue_by_nation.merge(nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
    final_result = final_result[['N_NAME', 'revenue']]
    
    # Write the results to a CSV file
    final_result.to_csv('query_output.csv', index=False)


if __name__ == "__main__":
    main()
