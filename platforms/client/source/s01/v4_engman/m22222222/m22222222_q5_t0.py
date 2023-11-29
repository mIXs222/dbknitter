import pandas as pd
from direct_redis import DirectRedis

def query_data():
    # Establish a connection to the Redis database
    redis_con = DirectRedis(host='redis', port=6379, db=0)

    # Read the tables as pandas DataFrames using the DirectRedis library
    nation = pd.read_json(redis_con.get('nation').decode('utf-8'))
    region = pd.read_json(redis_con.get('region').decode('utf-8'))
    supplier = pd.read_json(redis_con.get('supplier').decode('utf-8'))
    customer = pd.read_json(redis_con.get('customer').decode('utf-8'))
    orders = pd.read_json(redis_con.get('orders').decode('utf-8'))
    lineitem = pd.read_json(redis_con.get('lineitem').decode('utf-8'))

    # Filter regions to get only ASIA region
    asia_region = region[region['R_NAME'] == 'ASIA']

    # Merge the nation and region tables on the region key
    asia_nations = pd.merge(nation, asia_region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

    # Filter the suppliers and customers by the nation keys for ASIA
    asia_suppliers = supplier[supplier['S_NATIONKEY'].isin(asia_nations['N_NATIONKEY'])]
    asia_customers = customer[customer['C_NATIONKEY'].isin(asia_nations['N_NATIONKEY'])]

    # Filter orders by order date and join with ASIA customers
    orders = orders[(orders['O_ORDERDATE'] >= '1990-01-01') & (orders['O_ORDERDATE'] < '1995-01-01')]
    asia_orders = pd.merge(orders, asia_customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

    # Merge line items with ASIA suppliers
    lineitems_asia_suppliers = pd.merge(lineitem, asia_suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

    # Finally, join the orders with the line items to calculate the revenue
    final_data = pd.merge(asia_orders, lineitems_asia_suppliers, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

    # Calculate revenue
    final_data['REVENUE'] = final_data['L_EXTENDEDPRICE'] * (1 - final_data['L_DISCOUNT'])

    # Group by the nation name and sum the revenue
    result = final_data.groupby('N_NAME')['REVENUE'].sum().reset_index().sort_values(by='REVENUE', ascending=False)

    # Write the result to query_output.csv
    result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    query_data()
