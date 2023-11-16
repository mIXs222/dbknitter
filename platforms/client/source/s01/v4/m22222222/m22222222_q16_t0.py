# query.py
import pandas as pd
import direct_redis

def execute_query_and_save_to_csv():
    # Initialize connection to Redis database
    d_redis = direct_redis.DirectRedis(host='redis', port=6379, db=0)

    # Load tables from Redis
    df_part = pd.DataFrame(eval(d_redis.get('part')))
    df_supplier = pd.DataFrame(eval(d_redis.get('supplier')))
    df_partsupp = pd.DataFrame(eval(d_redis.get('partsupp')))

    # Perform the query
    # Filtering for P_BRAND and P_TYPE
    df_part_filtered = df_part[
        (df_part['P_BRAND'] != 'Brand#45') &
        (~df_part['P_TYPE'].str.startswith('MEDIUM POLISHED')) &
        (df_part['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))
    ]

    # Filter suppliers not in the subquery
    df_filtered_suppliers = df_supplier[~df_supplier['S_COMMENT'].str.contains('Customer%Complaints')]
    df_partsupp_filtered = df_partsupp[df_partsupp['PS_SUPPKEY'].isin(df_filtered_suppliers['S_SUPPKEY'])]

    # Join and group by as per the SQL query
    result = pd.merge(df_part_filtered, df_partsupp_filtered, left_on='P_PARTKEY', right_on='PS_PARTKEY')
    result = result.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg(SUPPLIER_CNT=('PS_SUPPKEY', 'nunique')).reset_index()

    # Sorting results as per SQL query
    result = result.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

    # Save result to CSV
    result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    execute_query_and_save_to_csv()
