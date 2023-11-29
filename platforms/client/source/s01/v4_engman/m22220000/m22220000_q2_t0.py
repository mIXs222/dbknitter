import pymysql
import pandas as pd
import json
from direct_redis import DirectRedis

def get_mysql_data():
    conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    query = """
    SELECT partsupp.PS_PARTKEY, partsupp.PS_SUPPKEY, partsupp.PS_SUPPLYCOST
    FROM partsupp
    """
    partsupp_df = pd.read_sql(query, conn)
    conn.close()
    return partsupp_df

def get_redis_data():
    redis_client = DirectRedis(host='redis', port=6379, db=0)    
    region_df = pd.DataFrame(json.loads(redis_client.get('region')))
    nation_df = pd.DataFrame(json.loads(redis_client.get('nation')))
    part_df = pd.DataFrame(json.loads(redis_client.get('part')))
    supplier_df = pd.DataFrame(json.loads(redis_client.get('supplier')))
    
    return region_df, nation_df, part_df, supplier_df

def combine_data(partsupp_df, region_df, nation_df, part_df, supplier_df):
    # Filter Europe region
    europe_region = region_df[region_df['R_NAME'] == 'EUROPE']
    europe_nations = pd.merge(nation_df, europe_region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
    
    # Filter part
    brass_parts = part_df[(part_df['P_TYPE'] == 'BRASS') & (part_df['P_SIZE'] == 15)]
    
    # Join tables
    combined_df = partsupp_df.merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
    combined_df = combined_df.merge(brass_parts, left_on='PS_PARTKEY', right_on='P_PARTKEY')
    combined_df = combined_df.merge(europe_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    
    # Filter minimum cost suppliers
    combined_df['MIN_PS_SUPPLYCOST'] = combined_df.groupby('P_PARTKEY')['PS_SUPPLYCOST'].transform(min)
    min_cost_df = combined_df[combined_df['PS_SUPPLYCOST'] == combined_df['MIN_PS_SUPPLYCOST']]
    
    return min_cost_df

def sort_and_select(min_cost_df):
    # Sorting
    min_cost_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'],
                            ascending=[False, True, True, True], inplace=True)
    
    # Selecting columns
    result_df = min_cost_df[['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']]
    
    # Output to CSV
    result_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    partsupp_df = get_mysql_data()

    # Get DataFrames from Redis
    region_df, nation_df, part_df, supplier_df = get_redis_data()

    # Combine DataFrames
    min_cost_df = combine_data(partsupp_df, region_df, nation_df, part_df, supplier_df)

    # Sort, select columns, and write to CSV
    sort_and_select(min_cost_df)
