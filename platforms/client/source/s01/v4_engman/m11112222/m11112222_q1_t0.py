# pricing_summary_report.py

import pandas as pd
from direct_redis import DirectRedis

def main():
    # Connect to the Redis database
    redis_db = DirectRedis(host='redis', port=6379, db=0)

    # Read in the 'lineitem' DataFrame from Redis
    df = pd.read_json(redis_db.get('lineitem'), orient='records')

    # Filter for lineitems shipped before 1998-09-02
    df['L_SHIPDATE'] = pd.to_datetime(df['L_SHIPDATE'])
    df_filtered = df[df['L_SHIPDATE'] < '1998-09-02']

    # Calculate the required aggregates and group by 'RETURNFLAG' and 'LINESTATUS'
    aggregates = {
        'L_QUANTITY': ['sum', 'mean'],
        'L_EXTENDEDPRICE': ['sum', 'mean'],
        'L_DISCOUNT': 'mean',
        'L_EXTENDEDPRICE_DISCOUNTED': 'sum',
        'AVERAGE_DISCOUNTPRICE_PLUS_TAX': 'mean'
    }

    # Calculate discounted extended price and discounted extended price plus tax
    df_filtered['L_EXTENDEDPRICE_DISCOUNTED'] = df_filtered['L_EXTENDEDPRICE'] * (1 - df_filtered['L_DISCOUNT'])
    df_filtered['L_EXTENDEDPRICE_PLUS_TAX'] = df_filtered['L_EXTENDEDPRICE_DISCOUNTED'] * (1 + df_filtered['L_TAX'])
    
    # Perform group by and aggregation
    df_summary = df_filtered.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg({
        'L_QUANTITY': ['sum', 'mean'],
        'L_EXTENDEDPRICE': ['sum', 'mean'],
        'L_DISCOUNT': 'mean',
        'L_EXTENDEDPRICE_DISCOUNTED': 'sum',
        'L_EXTENDEDPRICE_PLUS_TAX': 'mean'
    })

    # Rename columns to match the output format
    df_summary.columns = [
        'SUM_QTY', 'AVG_QTY',
        'SUM_BASE_PRICE', 'AVG_BASE_PRICE',
        'AVG_DISCOUNT', 'SUM_DISC_PRICE', 'AVG_DISC_PRICE_PLUS_TAX'
    ]

    # Reset index to move 'RETURNFLAG' and 'LINESTATUS' out of the index
    df_summary = df_summary.reset_index()

    # Sort by 'RETURNFLAG' and 'LINESTATUS'
    df_summary = df_summary.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

    # Write the results to a CSV file
    df_summary.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
