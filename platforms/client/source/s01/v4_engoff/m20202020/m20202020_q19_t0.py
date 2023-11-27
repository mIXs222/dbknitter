# discounted_revenue_query.py

import pymysql
import pandas as pd
import direct_redis

def connect_mysql():
    connection = pymysql.connect(
        host="mysql",
        user="root",
        password="my-secret-pw",
        database="tpch",
    )
    return connection

def fetch_lineitem_data(connection):
    query = """
    SELECT
        L_ORDERKEY,
        L_EXTENDEDPRICE,
        L_DISCOUNT,
        L_SHIPMODE,
        L_QUANTITY
    FROM
        lineitem
    WHERE 
        L_SHIPMODE IN ('AIR', 'AIR REG')
        AND ( 
            (L_QUANTITY >= 1  AND L_QUANTITY <= 11) OR 
            (L_QUANTITY >= 10 AND L_QUANTITY <= 20) OR
            (L_QUANTITY >= 20 AND L_QUANTITY <= 30)
        );
    """
    return pd.read_sql(query, connection)

def fetch_part_data(redis_client):
    part_data = redis_client.get('part')
    part_df = pd.read_json(part_data)
    return part_df

def generate_discounted_revenue(lineitems, parts):
    merge_condition = 'L_PARTKEY == P_PARTKEY'
    filtered_parts = parts[
        ((parts['P_BRAND'] == 'Brand#12') & 
         (parts['P_CONTAINER'].isin(['SM CASE', 'SM BOX','SM PACK','SM PKG'])) & 
         (parts['P_SIZE'] >= 1) & (parts['P_SIZE'] <= 5)
        ) |
        ((parts['P_BRAND'] == 'Brand#23') & 
         (parts['P_CONTAINER'].isin(['MED BAG','MED BOX','MED PKG','MED PACK'])) & 
         (parts['P_SIZE'] >= 1) & (parts['P_SIZE'] <= 10)
        ) |
        ((parts['P_BRAND'] == 'Brand#34') & 
         (parts['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) &
         (parts['P_SIZE'] >= 1) & (parts['P_SIZE'] <= 15)
        )
    ]
    combined_df = lineitems.merge(filtered_parts, left_on='L_PARTKEY', right_on='P_PARTKEY', how='inner')
    combined_df['DISCOUNTED_PRICE'] = combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])
    results = combined_df.groupby('L_ORDERKEY')['DISCOUNTED_PRICE'].sum().reset_index()

    return results

def main():
    mysql_connection = connect_mysql()
    lineitems = fetch_lineitem_data(mysql_connection)
    mysql_connection.close()

    redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    parts = fetch_part_data(redis_client)

    discounted_revenue = generate_discounted_revenue(lineitems, parts)

    discounted_revenue.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
