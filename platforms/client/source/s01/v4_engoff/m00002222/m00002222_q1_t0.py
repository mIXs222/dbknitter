import pandas as pd
from direct_redis import DirectRedis

# Establish connection to the Redis instance
connection = DirectRedis(host='redis', port=6379, db=0)

# Get the lineitem table as DataFrame
lineitem = pd.DataFrame(connection.get('lineitem'))

# Convert the shipdate string to datetime objects and filter by date
lineitem['L_SHIPDATE'] = pd.to_datetime(lineitem['L_SHIPDATE'])
filtered_data = lineitem[lineitem['L_SHIPDATE'] < pd.Timestamp('1998-09-02')]

# Calculate the derived columns
filtered_data['disc_price'] = filtered_data['L_EXTENDEDPRICE'] * (1 - filtered_data['L_DISCOUNT'])
filtered_data['charge'] = filtered_data['disc_price'] * (1 + filtered_data['L_TAX'])

# Group by RETURNFLAG and LINESTATUS
result = filtered_data.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg({
    'L_QUANTITY': ['sum', 'mean'],
    'L_EXTENDEDPRICE': ['sum', 'mean'],
    'L_DISCOUNT': ['mean', 'count'],
    'disc_price': 'sum',
    'charge': 'sum'
}).reset_index()

# Renaming the columns to the required format
result.columns = [
    'L_RETURNFLAG',
    'L_LINESTATUS',
    'sum_qty',
    'avg_qty',
    'sum_base_price',
    'avg_price',
    'avg_discount',
    'count_order',
    'sum_disc_price',
    'sum_charge'
]

# Sort results according to the query requirements
result.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'], inplace=True)

# Write the result to a csv file
result.to_csv('query_output.csv', index=False)
