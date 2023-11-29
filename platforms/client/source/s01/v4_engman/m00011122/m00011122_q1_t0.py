# pricing_summary_report.py

import pandas as pd
import direct_redis

def execute_pricing_summary_report():
    connection_details = {
        'database': '0',
        'port': 6379,
        'hostname': 'redis',
    }
    # Connect to Redis
    redis_db = direct_redis.DirectRedis(host=connection_details['hostname'], port=connection_details['port'], db=connection_details['database'])
    
    # Convert the Redis data to Pandas DataFrame.
    lineitem_df = pd.read_json(redis_db.get('lineitem'), orient='records')

    # Filter out the lineitems shipped before 1998-09-02
    lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
    filtered_df = lineitem_df[lineitem_df['L_SHIPDATE'] < '1998-09-02']

    # Compute the required aggregates
    summary_df = filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
        quantity_total=pd.NamedAgg(column='L_QUANTITY', aggfunc='sum'),
        extended_price_total=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='sum'),
        discounted_price_total=pd.NamedAgg(column=lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])).sum()),
        charge_total=pd.NamedAgg(column=lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']) * (1 + x['L_TAX'])).sum()),
        average_quantity=pd.NamedAgg(column='L_QUANTITY', aggfunc='mean'),
        average_extended_price=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='mean'),
        average_discount=pd.NamedAgg(column='L_DISCOUNT', aggfunc='mean'),
        count_order=pd.NamedAgg(column='L_ORDERKEY', aggfunc='count')
    ).reset_index()

    # Sort the results
    summary_df.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'], inplace=True)
    
    # Write the DataFrame to a CSV file
    summary_df.to_csv('query_output.csv', index=False)
    
if __name__ == '__main__':
    execute_pricing_summary_report()
