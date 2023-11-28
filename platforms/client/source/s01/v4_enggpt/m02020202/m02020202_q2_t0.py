import pymysql
import pandas as pd
import direct_redis

def connect_mysql():
    return pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch'
    )

def get_parts_and_nations(mysql_conn):
    query = """
    SELECT
        p.P_PARTKEY,
        p.P_MFGR,
        p.P_SIZE,
        n.N_NATIONKEY,
        n.N_NAME
    FROM part p
    INNER JOIN nation n ON p.P_SIZE = 15 AND p.P_TYPE LIKE '%BRASS%'
    """
    return pd.read_sql(query, mysql_conn)

def get_partsuppliers(mysql_conn):
    query = "SELECT PS_PARTKEY, PS_SUPPKEY, MIN(PS_SUPPLYCOST) AS MIN_SUPPLY_COST FROM partsupp GROUP BY PS_PARTKEY, PS_SUPPKEY"
    return pd.read_sql(query, mysql_conn)

def get_redis_data():
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    region_df = r.get('region')
    supplier_df = r.get('supplier')
    return region_df, supplier_df

def main():
    # MySQL connection
    mysql_conn = connect_mysql()

    try:
        # Get data from MySQL
        parts_nations_df = get_parts_and_nations(mysql_conn)
        partsuppliers_df = get_partsuppliers(mysql_conn)

        # Get data from Redis
        region_df, supplier_df = get_redis_data()

        # Filter the 'EUROPE' region
        europe_region_keys = region_df[region_df['R_NAME'] == 'EUROPE']['R_REGIONKEY'].tolist()
        europe_nations_df = parts_nations_df[parts_nations_df['N_REGIONKEY'].isin(europe_region_keys)]

        # Join on nation and region
        nations_suppliers_df = pd.merge(
            left=europe_nations_df,
            right=supplier_df,
            how='inner',
            left_on='N_NATIONKEY',
            right_on='S_NATIONKEY'
        )

        # Join on part and supplier
        final_df = pd.merge(
            left=nations_suppliers_df,
            right=partsuppliers_df,
            how='inner',
            left_on=['P_PARTKEY', 'S_SUPPKEY'],
            right_on=['PS_PARTKEY', 'PS_SUPPKEY']
        )

        # Order by S_ACCTBAL descending, N_NAME, S_NAME, P_PARTKEY ascending
        final_df.sort_values(
            by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'],
            ascending=[False, True, True, True],
            inplace=True
        )

        # Selecting the required columns
        result_df = final_df[[
            'S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT',
            'P_PARTKEY', 'P_MFGR', 'P_SIZE'
        ]].copy()

        # Write to CSV
        result_df.to_csv('query_output.csv', index=False)
    finally:
        mysql_conn.close()

if __name__ == "__main__":
    main()
