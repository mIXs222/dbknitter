# query.py
import pandas as pd
import direct_redis
import datetime

# Connection information
conn_info = {
    'db': 0,
    'port': 6379,
    'host': 'redis'
}

# Connecting to Redis
r = direct_redis.DirectRedis(host=conn_info['host'], port=conn_info['port'], db=conn_info['db'])

# Retrieve lineitem table dataframe from Redis
lineitem_json = r.get('lineitem')
lineitem = pd.read_json(lineitem_json, orient='records')

# Perform the query on lineitem dataframe
cutoff_date = datetime.date(1998, 9, 2)
filtered_data = lineitem[
    (lineitem['L_SHIPDATE'] < pd.Timestamp(cutoff_date))
]

# Calculate aggregated columns
summary_report = filtered_data \
    .assign(disc_price=lambda x: x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']),
            charge=lambda x: x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']) * (1 + x['L_TAX'])) \
    .groupby(['L_RETURNFLAG', 'L_LINESTATUS']) \
    .agg(total_qty=pd.NamedAgg(column='L_QUANTITY', aggfunc='sum'),
         total_base_price=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='sum'),
         total_disc_price=pd.NamedAgg(column='disc_price', aggfunc='sum'),
         total_charge=pd.NamedAgg(column='charge', aggfunc='sum'),
         avg_qty=pd.NamedAgg(column='L_QUANTITY', aggfunc='mean'),
         avg_price=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='mean'),
         avg_disc=pd.NamedAgg(column='L_DISCOUNT', aggfunc='mean'),
         count_order=pd.NamedAgg(column='L_ORDERKEY', aggfunc='count')) \
    .reset_index() \
    .sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

# Write the result to query_output.csv
summary_report.to_csv('query_output.csv', index=False)
