import pymysql
import pandas as pd
import direct_redis

def get_mysql_connection():
    return pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

def get_redis_connection():
    return direct_redis.DirectRedis(host='redis', port=6379, db=0)

def query_from_mysql():
    mysql_con = get_mysql_connection()
    try:
        with mysql_con.cursor() as cursor:
            # Extracting nations for Canada
            cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'CANADA'")
            nation_key = cursor.fetchone()
            if not nation_key:
                raise Exception("Canada not found in the nation table")
            nation_key = nation_key[0]
            
            # Querying suppliers based on the nation key and name pattern
            cursor.execute("""
                SELECT S.S_SUPPKEY, S.S_NAME
                FROM supplier S
                WHERE S.S_NATIONKEY = %s
            """, (nation_key,))
            suppliers = cursor.fetchall()
     
    finally:
        mysql_con.close()

    return pd.DataFrame(suppliers, columns=['S_SUPPKEY', 'S_NAME'])

def query_from_redis():
    redis_con = get_redis_connection()
    # Extracting parts with a pattern matching forest
    part_df = pd.read_json(redis_con.get('part'), orient='records')
    # Filter parts based on the pattern and retrieve their keys
    part_df = part_df[part_df['P_NAME'].str.contains('forest', case=False)]
    part_keys = part_df['P_PARTKEY']
    
    # Extracting lineitems within the date range and with part keys
    lineitem_df = pd.read_json(redis_con.get('lineitem'), orient='records')
    lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= '1994-01-01') & 
                              (lineitem_df['L_SHIPDATE'] <= '1995-01-01') & 
                              (lineitem_df['L_PARTKEY'].isin(part_keys))]
    
    # Summing quantities shipped by each supplier for qualifying parts
    shipped_qty_by_supp = lineitem_df.groupby('L_SUPPKEY')['L_QUANTITY'].sum().reset_index()

    return shipped_qty_by_supp

# Run the queries and merge results
suppliers_df = query_from_mysql()
shipped_qty_by_supp_df = query_from_redis()

# Only consider suppliers that have shipped qualifying parts
relevant_suppliers = pd.merge(suppliers_df, shipped_qty_by_supp_df,
                              left_on='S_SUPPKEY', right_on='L_SUPPKEY')

# Identifying suppliers with excess parts - more than 50%
relevant_suppliers['excess'] = relevant_suppliers['L_QUANTITY'] > 300 # Assuming total parts like forest is 600
potential_promotion_suppliers = relevant_suppliers[relevant_suppliers['excess']]

# Write the result to a CSV file
potential_promotion_suppliers.to_csv('query_output.csv', index=False)
