# query.py
import pandas as pd
import direct_redis
from datetime import datetime

def perform_query():
    # Connection to Redis
    redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

    # Load tables from Redis
    nation = pd.read_json(redis_client.get('nation'))
    supplier = pd.read_json(redis_client.get('supplier'))
    customer = pd.read_json(redis_client.get('customer'))
    orders = pd.read_json(redis_client.get('orders'))
    lineitem = pd.read_json(redis_client.get('lineitem'))

    # Filtering nations 'JAPAN' and 'INDIA'
    nations_of_interest = nation[nation['N_NAME'].isin(['JAPAN', 'INDIA'])]

    # Joining tables
    sup_nation = supplier.merge(nations_of_interest, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    cust_nation = customer.merge(nations_of_interest, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
    order_lineitem = orders.merge(lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

    # Combining all the tables with relevant information
    combined = sup_nation.merge(order_lineitem, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
    combined = combined.merge(cust_nation, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

    # Filtering date range between 1995 and 1996
    combined['L_SHIPDATE'] = pd.to_datetime(combined['L_SHIPDATE'])
    date_filtered = combined[(combined['L_SHIPDATE'] >= datetime(1995, 1, 1)) & (combined['L_SHIPDATE'] <= datetime(1996, 12, 31))]

    # Supplier from JAPAN and Customer from INDIA and Vice Versa
    japan_india = date_filtered[(date_filtered['N_NAME_x'] == 'JAPAN') & (date_filtered['N_NAME_y'] == 'INDIA')]
    india_japan = date_filtered[(date_filtered['N_NAME_x'] == 'INDIA') & (date_filtered['N_NAME_y'] == 'JAPAN')]
    final_data = pd.concat([japan_india, india_japan])

    # Calculate revenue
    final_data['REVENUE'] = final_data['L_EXTENDEDPRICE'] * (1 - final_data['L_DISCOUNT'])

    # Group by supplier nation, customer nation, year of shipping
    grouped_data = final_data.groupby([final_data['N_NAME_x'], final_data['N_NAME_y'], final_data['L_SHIPDATE'].dt.year]).agg({'REVENUE': 'sum'}).reset_index()

    # Sort the results
    sorted_data = grouped_data.sort_values(by=['N_NAME_x', 'N_NAME_y', 'L_SHIPDATE'])

    # Write to csv
    sorted_data.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    perform_query()
