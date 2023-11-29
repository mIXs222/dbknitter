import pandas as pd
import direct_redis

def get_revenue_from_redis():
    redis_conn_details = {
        'host': 'redis',
        'port': 6379,
        'db': 0
    }
    
    # Connect to Redis
    redis_conn = direct_redis.DirectRedis(**redis_conn_details)
    
    # Retrieve data from Redis
    part_df = pd.read_json(redis_conn.get('part'))
    lineitem_df = pd.read_json(redis_conn.get('lineitem'))
    
    # Define the filters
    filters = [
        {'brand_id': 12, 'containers': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'], 'quantity': (1, 11), 'size': (1, 5)},
        {'brand_id': 23, 'containers': ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'], 'quantity': (10, 20), 'size': (1, 10)},
        {'brand_id': 34, 'containers': ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'], 'quantity': (20, 30), 'size': (1, 15)}
    ]
    
    # Process tables and calculate revenue
    total_revenue = 0
    for filter in filters:
        part_mask = part_df.P_BRAND.eq(filter['brand_id']) & part_df.P_CONTAINER.isin(filter['containers']) & part_df.P_SIZE.between(*filter['size'])
        selected_parts = part_df[part_mask]
        
        lineitem_mask = lineitem_df.L_SHIPMODE.isin(['AIR', 'AIR REG']) & lineitem_df.L_SHIPINSTRUCT.eq('DELIVER IN PERSON') & \
                        lineitem_df.L_QUANTITY.between(*filter['quantity'])
        filtered_lineitems = lineitem_df[lineitem_mask]
        
        # Join relevant line items with filtered parts based on P_PARTKEY
        joined_data = pd.merge(filtered_lineitems, selected_parts, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
        
        # Calculate the revenue
        joined_data['REVENUE'] = joined_data.L_EXTENDEDPRICE * (1 - joined_data.L_DISCOUNT)
        total_revenue += joined_data['REVENUE'].sum()
    
    # Save the result to a file
    result_df = pd.DataFrame({'REVENUE': [total_revenue]})
    result_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    get_revenue_from_redis()
