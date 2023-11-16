# query.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

def query_mysql():
    mysql_conn = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch',
    )

    part_query = """
        SELECT P_PARTKEY, P_TYPE
        FROM part
        WHERE P_TYPE LIKE 'PROMO%'
    """

    with mysql_conn.cursor() as cursor:
        cursor.execute(part_query)
        part_data = cursor.fetchall()

    mysql_conn.close()

    return pd.DataFrame(part_data, columns=['P_PARTKEY', 'P_TYPE'])

def query_redis():
    redis_conn = DirectRedis(host='redis', port=6379, db=0)
    
    lineitem_df = pd.read_msgpack(redis_conn.get('lineitem'))
    
    redis_conn.close()
    
    lineitem_df = lineitem_df[
        (lineitem_df['L_SHIPDATE'] >= '1995-09-01') &
        (lineitem_df['L_SHIPDATE'] < '1995-10-01')
    ]

    return lineitem_df

def main():
    part_df = query_mysql()
    lineitem_df = query_redis()

    merged_df = pd.merge(
        lineitem_df,
        part_df,
        how='inner',
        left_on='L_PARTKEY',
        right_on='P_PARTKEY'
    )

    merged_df['EXT_PRICE_DISC'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

    promo_revenue = (
        (100.0 * merged_df[merged_df['P_TYPE'].str.startswith('PROMO')]['EXT_PRICE_DISC'].sum()) /
        merged_df['EXT_PRICE_DISC'].sum()
    )

    output = pd.DataFrame({'PROMO_REVENUE': [promo_revenue]})

    output.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
