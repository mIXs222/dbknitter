import pandas as pd
from direct_redis import DirectRedis

# Setting up the connection to the Redis
redis_hostname = 'redis'
redis_port = 6379
r = DirectRedis(host=redis_hostname, port=redis_port, db=0)

# Reading DataFrame from Redis
lineitem_df = r.get('lineitem')
lineitem = pd.read_msgpack(lineitem_df)

# Convert necessary columns to the correct datatype
lineitem['L_SHIPDATE'] = pd.to_datetime(lineitem['L_SHIPDATE'])
lineitem['L_EXTENDEDPRICE'] = lineitem['L_EXTENDEDPRICE'].astype(float)
lineitem['L_DISCOUNT'] = lineitem['L_DISCOUNT'].astype(float)
lineitem['L_QUANTITY'] = lineitem['L_QUANTITY'].astype(float)
lineitem['L_TAX'] = lineitem['L_TAX'].astype(float)

# Filtering line items by shipping date criterion
filtered_lineitem = lineitem[lineitem['L_SHIPDATE'] <= pd.Timestamp('1998-09-02')]

# Performing the aggregate operations
aggregation_functions = {
    'L_QUANTITY': ['sum', 'mean'],
    'L_EXTENDEDPRICE': ['sum', 'mean'],
    'L_DISCOUNT': 'mean',
    'L_ORDERKEY': 'count'
}
aggregated = filtered_lineitem.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(aggregation_functions)

# Renaming columns as per the requirements
aggregated.columns = [
    'SUM_QTY', 'AVG_QTY',
    'SUM_BASE_PRICE', 'AVG_PRICE',
    'AVG_DISC',
    'COUNT_ORDER'
]

# Computing the total discounted price and total charge
aggregated['SUM_DISC_PRICE'] = aggregated.apply(lambda x: x['SUM_BASE_PRICE'] * (1 - x['AVG_DISC']), axis=1)
aggregated['SUM_CHARGE'] = aggregated.apply(lambda x: (x['SUM_BASE_PRICE'] * (1 - x['AVG_DISC'])) * (1 + x.name[1]), axis=1)

# Sorting the results as per the requirement
sorted_aggregated = aggregated.sort_index(ascending=True)

# Writing the result to CSV
sorted_aggregated.to_csv('query_output.csv')
