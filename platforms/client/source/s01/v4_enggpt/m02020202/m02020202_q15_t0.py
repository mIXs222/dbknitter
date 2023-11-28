import pandas as pd
from datetime import datetime
import direct_redis

# Replace these values with correct connection parameters if necessary
REDIS_HOSTNAME = 'redis'
REDIS_PORT = 6379
REDIS_DB_NAME = '0'

# Function to read data from Redis
def read_from_redis(table_name):
    dr = direct_redis.DirectRedis(host=REDIS_HOSTNAME, port=REDIS_PORT, db=int(REDIS_DB_NAME))
    data = dr.get(table_name)
    return pd.read_json(data)

# Function to transform and process the data
def process_data():
    # Load data from Redis
    df_supplier = read_from_redis('supplier')
    df_lineitem = read_from_redis('lineitem')

    # Convert SHIPDATE to datetime and filter date range
    df_lineitem['L_SHIPDATE'] = pd.to_datetime(df_lineitem['L_SHIPDATE'])
    df_lineitem = df_lineitem[(df_lineitem['L_SHIPDATE'] >= datetime(1996, 1, 1)) & 
                              (df_lineitem['L_SHIPDATE'] <= datetime(1996, 3, 31))]

    # Calculate revenue for each line item
    df_lineitem['REVENUE'] = df_lineitem['L_EXTENDEDPRICE'] * (1 - df_lineitem['L_DISCOUNT'])

    # Create a CTE-like DataFrame for total revenue per supplier
    df_revenue0 = df_lineitem.groupby('L_SUPPKEY')['REVENUE'].sum().reset_index()
    df_revenue0.rename(columns={'L_SUPPKEY': 'SUPPLIER_NO', 'REVENUE': 'TOTAL_REVENUE'}, inplace=True)

    # Join supplier with revenue DataFrame to get supplier details
    df_result = df_supplier.merge(df_revenue0, left_on='S_SUPPKEY', right_on='SUPPLIER_NO')

    # Find the supplier with the maximum total revenue
    max_revenue_supplier = df_result[df_result['TOTAL_REVENUE'] == df_result['TOTAL_REVENUE'].max()]

    # Order the final results
    final_result = max_revenue_supplier.sort_values(by='S_SUPPKEY')

    # Write the results to CSV
    final_result.to_csv('query_output.csv', index=False)

def main():
    process_data()

if __name__ == '__main__':
    main()
