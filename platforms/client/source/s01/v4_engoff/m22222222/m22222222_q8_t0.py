import pandas as pd
from direct_redis import DirectRedis

def run_query():
    # Create a DirectRedis instance to connect to the Redis database
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    
    # Load tables from the Redis database
    nation = pd.read_json(redis_client.get('nation'), orient='records')
    region = pd.read_json(redis_client.get('region'), orient='records')
    supplier = pd.read_json(redis_client.get('supplier'), orient='records')
    part = pd.read_json(redis_client.get('part'), orient='records')
    lineitem = pd.read_json(redis_client.get('lineitem'), orient='records')
    orders = pd.read_json(redis_client.get('orders'), orient='records')
    
    # Filters and conditions
    asiaregion = region[region.R_NAME == 'ASIA']
    indiasuppliers = supplier[(supplier.S_NATIONKEY.isin(nation[nation.N_NAME == 'INDIA'].N_NATIONKEY))]
    target_parts = part[(part.P_SIZE == 'SMALL') & (part.P_TYPE == 'PLATED COPPER')]
    years = ['1995', '1996']

    # Merging Dataframes
    part_lineitem = pd.merge(target_parts, lineitem, left_on='P_PARTKEY', right_on='L_PARTKEY')
    ind_lnsup = pd.merge(indiasuppliers, part_lineitem, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
    nation_orders = pd.merge(nation, orders, left_on='N_NATIONKEY', right_on='O_CUSTKEY')
    asia_nation = pd.merge(asiaregion, nation_orders, left_on='R_REGIONKEY', right_on='N_REGIONKEY')
    
    # Final merge and calculation of market share
    result_merge = pd.merge(asia_nation, ind_lnsup, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    result_merge['YEAR'] = pd.to_datetime(result_merge['O_ORDERDATE']).dt.year
    result_merge['REVENUE'] = result_merge['L_EXTENDEDPRICE'] * (1 - result_merge['L_DISCOUNT'])

    # Calculate market share for 1995 and 1996
    result = []
    for year in years:
        total_revenue = result_merge[result_merge['YEAR'] == int(year)]['REVENUE'].sum()
        india_revenue = result_merge[(result_merge['YEAR'] == int(year)) & (result_merge['N_NAME'] == 'INDIA')]['REVENUE'].sum()
        market_share = india_revenue / total_revenue if total_revenue else 0
        result.append([market_share, int(year)])

    # Create and save DataFrame of results
    result_df = pd.DataFrame(result, columns=['MARKET_SHARE', 'YEAR']).sort_values('YEAR')
    result_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    run_query()
