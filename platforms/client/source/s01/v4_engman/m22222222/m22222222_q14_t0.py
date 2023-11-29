import pandas as pd
from direct_redis import DirectRedis

def query_promotion_effect():
    # Connecting to the Redis database
    redis_db = DirectRedis(host='redis', port=6379, db=0)

    # Getting the 'part' and 'lineitem' tables from Redis
    part_df = pd.read_msgpack(redis_db.get('part'))
    lineitem_df = pd.read_msgpack(redis_db.get('lineitem'))

    # Filtering the date for the range provided
    date_start = '1995-09-01'
    date_end = '1995-10-01'
    lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
    filtered_lineitem = lineitem_df[(lineitem_df['L_SHIPDATE'] >= date_start) &
                                    (lineitem_df['L_SHIPDATE'] <= date_end)]

    # Merge the two dataframes based on part key
    merged_df = filtered_lineitem.merge(part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

    # Calculate Revenue
    merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

    # Summarize revenue and compute the percentage
    total_revenue = merged_df['REVENUE'].sum()
    promo_revenue = merged_df[merged_df['P_TYPE'].str.contains('PROMO')]['REVENUE'].sum()
    promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

    # Create the output dataframe
    output_df = pd.DataFrame({
        'TOTAL_REVENUE': [total_revenue],
        'PROMO_REVENUE': [promo_revenue],
        'PROMO_REVENUE_PERCENTAGE': [promo_revenue_percentage]
    })

    # Write results to a CSV file
    output_df.to_csv('query_output.csv', index=False)

# Execute the query
query_promotion_effect()
