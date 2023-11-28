# mysql_redis_query.py

import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Global variables for the database connections
mysql_connection_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.Cursor
}

redis_connection_info = {
    'host': 'redis',
    'port': 6379,
}

# Connect to MySQL and query the necessary tables
def query_mysql():
    connection = pymysql.connect(**mysql_connection_info)
    try:
        with connection.cursor() as cursor:
            supplier_query = """
            SELECT *
            FROM supplier
            """
            cursor.execute(supplier_query)
            supplier_data = cursor.fetchall()

            partsupp_query = """
            SELECT *
            FROM partsupp
            """
            cursor.execute(partsupp_query)
            partsupp_data = cursor.fetchall()

        # Turn query results into a pandas DataFrame
        supplier_df = pd.DataFrame(list(supplier_data), columns=[
            'S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])
        partsupp_df = pd.DataFrame(list(partsupp_data), columns=[
            'PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'])
    finally:
        connection.close()
    
    return supplier_df, partsupp_df

# Connect to Redis and get the Nation data
def query_redis():
    client = DirectRedis(**redis_connection_info)
    nation_data = client.get('nation')
    nation_df = pd.read_json(nation_data)
    return nation_df

def process_data(supplier_df, partsupp_df, nation_df):
    # Filter for German suppliers
    german_nations = nation_df[nation_df['N_NAME'] == 'GERMANY']
    german_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(german_nations['N_NATIONKEY'])]

    # Merge German suppliers with partsupp data
    merged_df = pd.merge(partsupp_df, german_suppliers, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

    # Calculate total value for each part
    merged_df['TOTAL_VALUE'] = merged_df['PS_SUPPLYCOST'] * merged_df['PS_AVAILQTY']
    
    # Calculate the threshold value from the subquery
    threshold_value = merged_df['TOTAL_VALUE'].sum() * 0.01  # Assuming a threshold of 1% of the overall value

    # Group by part key and apply filtering condition
    result = merged_df.groupby('PS_PARTKEY').filter(lambda x: x['TOTAL_VALUE'].sum() > threshold_value)

    # Sort the result
    result = result.sort_values(by='TOTAL_VALUE', ascending=False)

    # Selecting the relevant columns for output
    result = result[['PS_PARTKEY', 'TOTAL_VALUE']]

    return result

# Main Function to execute the overall process
def main():
    supplier_df, partsupp_df = query_mysql()
    nation_df = query_redis()
    processed_result = process_data(supplier_df, partsupp_df, nation_df)
    processed_result.to_csv('query_output.csv', index=False)

# Execute the script
if __name__ == '__main__':
    main()
