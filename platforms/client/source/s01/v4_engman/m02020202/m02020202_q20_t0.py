# potential_part_promotion.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

def get_mysql_data():
    conn = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch',
        charset='utf8mb4'
    )

    part_query = """
        SELECT P_PARTKEY, P_NAME
        FROM part
        WHERE P_NAME LIKE '%forest%'
    """
    partsupp_query = """
        SELECT PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY
        FROM partsupp
    """

    with conn.cursor() as cur:
        cur.execute(part_query)
        part_data = pd.DataFrame(cur.fetchall(), columns=['P_PARTKEY', 'P_NAME'])

        cur.execute(partsupp_query)
        partsupp_data = pd.DataFrame(cur.fetchall(), columns=['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY'])

    conn.close()
    return part_data, partsupp_data

def get_redis_data():
    redis = DirectRedis(host='redis', port=6379, db=0)
    
    supplier_data = pd.read_json(redis.get('supplier'))
    lineitem_data = pd.read_json(redis.get('lineitem'))

    return supplier_data, lineitem_data

def main():
    part_data, partsupp_data = get_mysql_data()
    supplier_data, lineitem_data = get_redis_data()

    # Filter lineitem for the given date range and CANADA nation
    lineitem_data = lineitem_data[((lineitem_data['L_SHIPDATE'] >= '1994-01-01') & 
                                   (lineitem_data['L_SHIPDATE'] < '1995-01-01'))]
    canada_suppliers = supplier_data[supplier_data['S_NATIONKEY'] == 3] # Assuming CANADA has NATIONKEY 3

    # Merge frames to find suppliers with part availability in CANADA
    parts_in_canada = lineitem_data.merge(canada_suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    parts_in_canada = part_data.merge(parts_in_canada, left_on='P_PARTKEY', right_on='L_PARTKEY')
    excess_parts = partsupp_data.merge(parts_in_canada, on='PS_PARTKEY')

    # Group by SUPPKEY to find excess in parts
    excess_parts['excess'] = excess_parts['PS_AVAILQTY'] > 0.5 * excess_parts['L_QUANTITY']
    results = excess_parts[excess_parts['excess']]
    
    # Final result
    final_results = results[['S_SUPPKEY', 'S_NAME']].drop_duplicates()
    final_results.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
