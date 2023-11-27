import pandas as pd
import direct_redis

def execute_query():
    # Connect to Redis
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    
    # Get all tables as Pandas DataFrames
    nation = pd.DataFrame(r.get('nation'))
    supplier = pd.DataFrame(r.get('supplier'))
    customer = pd.DataFrame(r.get('customer'))
    orders = pd.DataFrame(r.get('orders'))
    lineitem = pd.DataFrame(r.get('lineitem'))
    
    # Filter nations for 'INDIA' and 'JAPAN'
    nations_filtered = nation[nation['N_NAME'].isin(['INDIA', 'JAPAN'])]
    
    # Join supplier with nations_filtered to get suppliers from INDIA and JAPAN
    supplier_nation = supplier.merge(nations_filtered, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    
    # Join customer with nations_filtered to get customers from INDIA and JAPAN
    customer_nation = customer.merge(nations_filtered, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
    
    # Orders from the years 1995 and 1996
    orders_filtered = orders[(orders['O_ORDERDATE'] >= '1995-01-01') & (orders['O_ORDERDATE'] <= '1996-12-31')]
    
    # Join lineitem with orders_filtered
    lineitem_orders = lineitem.merge(orders_filtered, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    
    # Calculate the revenue
    lineitem_orders['revenue'] = lineitem_orders['L_EXTENDEDPRICE'] * (1 - lineitem_orders['L_DISCOUNT'])
    
    # Join lineitem_orders with supplier_nation and customer_nation
    final_query = lineitem_orders.merge(supplier_nation, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    final_query = final_query.merge(customer_nation, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    
    # Filter for supplier and customer from different nations
    cross_nation_sales = final_query[(final_query['N_NAME_x'] != final_query['N_NAME_y']) &
                                     (final_query['N_NAME_x'].isin(['INDIA', 'JAPAN'])) &
                                     (final_query['N_NAME_y'].isin(['INDIA', 'JAPAN']))]
    
    # Extract year from date
    cross_nation_sales['year'] = pd.to_datetime(cross_nation_sales['O_ORDERDATE']).dt.year
    
    # Required columns for output
    result = cross_nation_sales.groupby(['N_NAME_x', 'N_NAME_y', 'year']).agg({'revenue': 'sum'}).reset_index()
    result = result.rename(columns={'N_NAME_x':'supplier_nation', 'N_NAME_y': 'customer_nation'})
    result = result.sort_values(by=['supplier_nation', 'customer_nation', 'year'])
    
    # Write to CSV
    result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    execute_query()
