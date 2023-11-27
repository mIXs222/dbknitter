# Python code to execute the hybrid query across MySQL and Redis databases.
import pymysql
import pandas as pd
import direct_redis

def get_mysql_connection():
    return pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch'
    )

def get_redis_connection():
    return direct_redis.DirectRedis(host='redis', port=6379, db=0)

def fetch_partsupp_from_mysql(conn):
    query = """
    SELECT
        PS_PARTKEY,
        PS_SUPPKEY,
        PS_AVAILQTY,
        PS_SUPPLYCOST,
        PS_COMMENT
    FROM
        partsupp
    """
    return pd.read_sql(query, conn)

def fetch_excluded_suppkeys_from_redis(r):
    supplier_data = r.get('supplier')
    suppliers_df = pd.DataFrame(supplier_data)
    excluded_supp_keys = suppliers_df[suppliers_df['S_COMMENT'].str.contains('Customer.*Complaints')]['S_SUPPKEY'].unique()
    return excluded_supp_keys

def main():
    # Connect to MySQL and Redis
    mysql_conn = get_mysql_connection()
    redis_conn = get_redis_connection()
    
    # Fetch the partsupp table from MySQL
    partsupp_df = fetch_partsupp_from_mysql(mysql_conn)
    
    # Fetch part and supplier data from Redis
    part_data = redis_conn.get('part')
    part_df = pd.DataFrame(part_data)
    
    # Convert necessary columns to numeric type to match SQL types
    partsupp_df['PS_SUPPKEY'] = partsupp_df['PS_SUPPKEY'].astype(int)
    partsupp_df['PS_PARTKEY'] = partsupp_df['PS_PARTKEY'].astype(int)
    part_df['P_PARTKEY'] = part_df['P_PARTKEY'].astype(int)
    
    # Fetch the excluded supplier keys from Redis
    excluded_suppkeys = fetch_excluded_suppkeys_from_redis(redis_conn)
    
    # Filtering out the parts and suppliers according to the conditions
    filtered_parts = part_df[
        (part_df['P_BRAND'] != 'Brand#45') &
        (~part_df['P_TYPE'].str.startswith('MEDIUM POLISHED')) &
        (part_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))
    ]
    
    # Merge the parts and partsupp dataframes on partkey
    merged_data = filtered_parts.merge(partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')
    
    # Exclude suppliers with complaints
    merged_data = merged_data[~merged_data['PS_SUPPKEY'].isin(excluded_suppkeys)]
    
    # Perform GROUP BY operation and count distinct suppliers
    result = merged_data.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']) \
                        .agg(SUPPLIER_CNT=('PS_SUPPKEY', pd.Series.nunique)) \
                        .reset_index()
    
    # Order by SUPPLIER_CNT DESC, P_BRAND, P_TYPE, P_SIZE
    result = result.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])
    
    # Write the result to 'query_output.csv'
    result.to_csv('query_output.csv', index=False)
    
    # Close connections
    mysql_conn.close()
    redis_conn.close()

if __name__ == '__main__':
    main()
