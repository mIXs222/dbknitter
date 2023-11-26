# redis_query.py
import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis
hostname = 'redis'
port = 6379
database_name = '0'  # Redis doesn't use databases the same way SQL does
r = DirectRedis(host=hostname, port=port, db=database_name)

# Load the DataFrame using get()
lineitem = pd.read_json(r.get('lineitem'), orient='records')

# Perform the same operations as the SQL query using pandas
filtered_data = lineitem[lineitem['L_SHIPDATE'] <= '1998-09-02']
grouped_data = filtered_data.groupby(['L_RETURNFLAG', 'L_LINESTATUS'])

# Perform aggregation
result = grouped_data.agg(SUM_QTY=('L_QUANTITY', 'sum'),
                          SUM_BASE_PRICE=('L_EXTENDEDPRICE', 'sum'),
                          SUM_DISC_PRICE=('L_EXTENDEDPRICE', lambda x: (x * (1 - filtered_data.loc[x.index, 'L_DISCOUNT'])).sum()),
                          SUM_CHARGE=('L_EXTENDEDPRICE', lambda x: (x * (1 - filtered_data.loc[x.index, 'L_DISCOUNT']) * (1 + filtered_data.loc[x.index, 'L_TAX'])).sum()),
                          AVG_QTY=('L_QUANTITY', 'mean'),
                          AVG_PRICE=('L_EXTENDEDPRICE', 'mean'),
                          AVG_DISC=('L_DISCOUNT', 'mean'),
                          COUNT_ORDER=('L_QUANTITY', 'count'))

# Sort the result
sorted_result = result.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

# Write to CSV
sorted_result.to_csv('query_output.csv')
