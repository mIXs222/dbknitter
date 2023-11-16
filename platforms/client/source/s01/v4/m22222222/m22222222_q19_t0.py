import pandas as pd
from direct_redis import DirectRedis

def main():
    # Connect to Redis
    redis_client = DirectRedis(host='redis', port=6379, db=0)

    # Get data from Redis as Pandas DataFrames
    df_part = pd.read_json(redis_client.get('part'))
    df_lineitem = pd.read_json(redis_client.get('lineitem'))

    # Apply SQL filters and calculation within Pandas environment
    conditions = (
        # First condition block
        (
            (df_part['P_PARTKEY'] == df_lineitem['L_PARTKEY']) &
            (df_part['P_BRAND'] == 'Brand#12') &
            (df_part['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) &
            (df_lineitem['L_QUANTITY'] >= 1) & (df_lineitem['L_QUANTITY'] <= 11) &
            (df_part['P_SIZE'] >= 1) & (df_part['P_SIZE'] <= 5) &
            (df_lineitem['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
            (df_lineitem['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
        ) |
        # Second condition block
        (
            (df_part['P_PARTKEY'] == df_lineitem['L_PARTKEY']) &
            (df_part['P_BRAND'] == 'Brand#23') &
            (df_part['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) &
            (df_lineitem['L_QUANTITY'] >= 10) & (df_lineitem['L_QUANTITY'] <= 20) &
            (df_part['P_SIZE'] >= 1) & (df_part['P_SIZE'] <= 10) &
            (df_lineitem['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
            (df_lineitem['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
        ) |
        # Third condition block
        (
            (df_part['P_PARTKEY'] == df_lineitem['L_PARTKEY']) &
            (df_part['P_BRAND'] == 'Brand#34') &
            (df_part['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) &
            (df_lineitem['L_QUANTITY'] >= 20) & (df_lineitem['L_QUANTITY'] <= 30) &
            (df_part['P_SIZE'] >= 1) & (df_part['P_SIZE'] <= 15) &
            (df_lineitem['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
            (df_lineitem['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
        )
    )

    merged_df = df_lineitem.merge(df_part, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
    filtered_df = merged_df[conditions]
    # Perform the revenue calculation
    filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

    # Group by empty set (to perform aggregate on whole dataset), and compute SUM of revenue
    result_df = pd.DataFrame({'REVENUE': [filtered_df['REVENUE'].sum()]})

    # Write the result_df to a CSV file
    result_df.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
