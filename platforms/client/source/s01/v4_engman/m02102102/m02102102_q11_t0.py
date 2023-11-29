# important_stock_identification.py

import pymysql
import pandas as pd
import direct_redis

def main():
    # Connect to MySQL
    mysql_connection = pymysql.connect(host='mysql',
                                       user='root',
                                       password='my-secret-pw',
                                       database='tpch')
    
    # Retrieve data from MySQL tables
    query_nation = "SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'GERMANY';"
    german_nation_keys = pd.read_sql(query_nation, mysql_connection)

    query_supplier = "SELECT S_SUPPKEY, S_NATIONKEY FROM supplier;"
    suppliers = pd.read_sql(query_supplier, mysql_connection)

    # Filter german suppliers
    german_suppliers = suppliers[suppliers['S_NATIONKEY'].isin(german_nation_keys['N_NATIONKEY'])]
    mysql_connection.close()

    # Connect to Redis
    redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

    # Retrieve data from Redis
    partsupp_df = redis_connection.get('partsupp')
    if partsupp_df is not None:
        partsupp = pd.read_json(partsupp_df, orient='split')
    else:
        print("Error retrieving data from Redis.")
        return

    # Join German suppliers with their parts supply
    german_supplier_parts = partsupp[partsupp['PS_SUPPKEY'].isin(german_suppliers['S_SUPPKEY'])]

    # Calculate the total value for each part
    german_supplier_parts['TOTAL_VALUE'] = german_supplier_parts['PS_AVAILQTY'] * german_supplier_parts['PS_SUPPLYCOST']

    # Calculate the significance threshold
    significance_threshold = german_supplier_parts['TOTAL_VALUE'].sum() * 0.0001

    # Filter parts that represent a significant percentage of the total value
    important_parts = german_supplier_parts[german_supplier_parts['TOTAL_VALUE'] > significance_threshold]

    # Select part number and value, ordered by the value in descending order
    important_parts = important_parts[['PS_PARTKEY', 'TOTAL_VALUE']].sort_values(by='TOTAL_VALUE', ascending=False)

    # Write results to CSV
    important_parts.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
