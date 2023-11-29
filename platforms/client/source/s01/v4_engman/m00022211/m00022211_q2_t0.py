# query.py
import pymysql
import pandas as pd
import direct_redis

def get_connection(database_name, username, password, hostname):
    return pymysql.connect(host=hostname, user=username, passwd=password, db=database_name)

def fetch_from_mysql(query, connection):
    return pd.read_sql(query, connection)

def main():
    mysql_connection = get_connection('tpch', 'root', 'my-secret-pw', 'mysql')

    # Fetch required data from mysql
    region_query = "SELECT R_REGIONKEY FROM region WHERE R_NAME='EUROPE';"
    europe_region = fetch_from_mysql(region_query, mysql_connection)
    if europe_region.empty:
        return
    
    eu_region_key = europe_region.iloc[0]['R_REGIONKEY']
    
    nation_query = f"SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_REGIONKEY={eu_region_key};"
    nations = fetch_from_mysql(nation_query, mysql_connection)

    part_query = "SELECT P_PARTKEY, P_MFGR FROM part WHERE P_TYPE='BRASS' AND P_SIZE=15;"
    parts = fetch_from_mysql(part_query, mysql_connection)

    # Close MySQL connection
    mysql_connection.close()

    # Connect to Redis
    redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    
    # Fetch data from Redis
    supplier_df = pd.DataFrame(redis_connection.get('supplier'))
    partsupp_df = pd.DataFrame(redis_connection.get('partsupp'))

    # Start the merging and filtering process
    nations.columns = ['S_NATIONKEY', 'N_NAME']
    suppliers_in_europe = pd.merge(supplier_df, nations, on='S_NATIONKEY', how='inner')

    suppliers_parts_europe = pd.merge(partsupp_df, suppliers_in_europe, on='S_SUPPKEY', how='inner')
    suppliers_parts_europe = pd.merge(suppliers_parts_europe, parts, on='P_PARTKEY', how='inner')

    # Find the minimum cost for each part and then merge to get the details of suppliers
    parts_min_cost = suppliers_parts_europe.loc[suppliers_parts_europe.groupby('P_PARTKEY')['PS_SUPPLYCOST'].idxmin()]
    result = parts_min_cost.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

    # Selecting the required columns
    final_result = result[['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']]

    # Write the result to CSV
    final_result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
