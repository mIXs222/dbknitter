import pandas as pd
from direct_redis import DirectRedis

# Establish connection to the Redis database
r = DirectRedis(host='redis', port=6379, db=0)

# Retrieve the 'lineitem' table
lineitem_data = r.get('lineitem')
lineitem_df = pd.read_json(lineitem_data)

# Apply the filters and group by requirements of the report
filtered_df = lineitem_df[lineitem_df['L_SHIPDATE'] < '1998-09-02']
aggregation = {
    'L_QUANTITY': ['sum', 'mean'],
    'L_EXTENDEDPRICE': ['sum', 'mean'],
    'L_DISCOUNT': 'mean',
    'L_DISCOUNTED_PRICE': lambda x: sum(x * (1 - filtered_df.loc[x.index, 'L_DISCOUNT'])),
    'L_DISCOUNTED_PRICE_PLUS_TAX': lambda x: sum(x * (1 - filtered_df.loc[x.index, 'L_DISCOUNT']) * (1 + filtered_df.loc[x.index, 'L_TAX'])),
    'L_RETURNFLAG': 'count'
}

# Adding computed columns for discounted price calculations
filtered_df['L_DISCOUNTED_PRICE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])
filtered_df['L_DISCOUNTED_PRICE_PLUS_TAX'] = filtered_df['L_DISCOUNTED_PRICE'] * (1 + filtered_df['L_TAX'])

grouped_df = filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(aggregation).reset_index()
grouped_df.columns = [ # Flatten the MultiIndex columns
    'L_RETURNFLAG',
    'L_LINESTATUS',
    'SUM_L_QUANTITY',
    'AVG_L_QUANTITY',
    'SUM_L_EXTENDEDPRICE',
    'AVG_L_EXTENDEDPRICE',
    'AVG_L_DISCOUNT',
    'SUM_DISCOUNTED_PRICE',
    'SUM_DISCOUNTED_PRICE_PLUS_TAX',
    'COUNT_ORDER'
]

# Sort the results as instructed
sorted_df = grouped_df.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

# Write the output to a CSV
sorted_df.to_csv('query_output.csv', index=False)
