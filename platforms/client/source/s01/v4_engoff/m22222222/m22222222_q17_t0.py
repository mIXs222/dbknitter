import pandas as pd
import direct_redis

def get_data_from_redis(redis_host, redis_port, db_name):
    client = direct_redis.DirectRedis(host=redis_host, port=redis_port, db=db_name)
    part_df = pd.read_json(client.get('part').decode('utf-8'))
    lineitem_df = pd.read_json(client.get('lineitem').decode('utf-8'))
    return part_df, lineitem_df

def calculate_lost_revenue(part_df, lineitem_df):
    # Filter parts that are of brand 'Brand#23' with 'MED BAG' container
    filtered_parts = part_df[(part_df['P_BRAND'] == 'Brand#23') & (part_df['P_CONTAINER'] == 'MED BAG')]
    
    # Merge with lineitem table on part key
    merged_df = pd.merge(filtered_parts, lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')
    
    # Calculate the average quantity
    avg_quantity = merged_df['L_QUANTITY'].mean()
    
    # Calculate yearly lost revenue
    small_quantity_orders = merged_df[merged_df['L_QUANTITY'] < 0.2 * avg_quantity]
    lost_revenue = small_quantity_orders['L_EXTENDEDPRICE'].sum() / 7  # Assuming the 7-year database period
    
    return lost_revenue

if __name__ == '__main__':
    try:
        # Config
        redis_host = 'redis'
        redis_port = 6379
        redis_db_name = 0

        # Fetch data from Redis
        part_df, lineitem_df = get_data_from_redis(redis_host, redis_port, redis_db_name)

        # Calculate the lost revenue
        avg_lost_revenue = calculate_lost_revenue(part_df, lineitem_df)

        # Write to CSV
        with open('query_output.csv', 'w') as file:
            file.write(f"Average yearly gross loss in revenue,{avg_lost_revenue}\n")
            
        print('Query executed successfully and output saved to "query_output.csv"')
    
    except Exception as e:
        print(f"An error occurred: {e}")
