import pandas as pd
from direct_redis import DirectRedis

def calculate_loss():
    # Connect to Redis
    redis_client = DirectRedis(host='redis', port=6379, db=0)

    # Fetch data using the connection information provided
    part_table = pd.read_json(redis_client.get('part'))
    lineitem_table = pd.read_json(redis_client.get('lineitem'))

    # Filter parts of BRAND#23 and with MED BAG
    filtered_parts = part_table[(part_table['P_BRAND'] == 'BRAND#23') & (part_table['P_CONTAINER'] == 'MED BAG')]

    # Join the filtered parts with lineitem
    join_result = pd.merge(filtered_parts, lineitem_table, left_on='P_PARTKEY', right_on='L_PARTKEY')

    # Calculate average quantity of such parts ordered
    avg_quantity = join_result['L_QUANTITY'].mean()

    # Consider only the lineitems with quantity less than 20% of average quantity
    small_quantity_orders = join_result[join_result['L_QUANTITY'] < (0.20 * avg_quantity)]

    # Calculate the gross loss in revenue (undiscounted)
    gross_loss = small_quantity_orders['L_EXTENDEDPRICE'].sum()

    # Average yearly gross loss
    avg_yearly_gross_loss = gross_loss / 7 # as per the 7-year database

    # Save the result to a csv file
    pd.DataFrame({'avg_yearly_gross_loss': [avg_yearly_gross_loss]}).to_csv('query_output.csv', index=False)

# Execute the function
calculate_loss()
