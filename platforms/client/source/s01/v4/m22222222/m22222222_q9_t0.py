import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Function to extract and convert the Redis data into DataFrame
def get_dataframe_from_redis(table_name):
    data = db.get(table_name)
    df = pd.read_json(data)
    return df

# Function to calculate the SUM_PROFIT
def calculate_sum_profit(nation, part, supplier, partsupp, orders, lineitem):
    # Filters
    filters = (
        (supplier['S_SUPPKEY'] == lineitem['L_SUPPKEY']) &
        (partsupp['PS_SUPPKEY'] == lineitem['L_SUPPKEY']) &
        (partsupp['PS_PARTKEY'] == lineitem['L_PARTKEY']) &
        (part['P_PARTKEY'] == lineitem['L_PARTKEY']) &
        (orders['O_ORDERKEY'] == lineitem['O_ORDERKEY']) &
        (supplier['S_NATIONKEY'] == nation['N_NATIONKEY']) &
        (part['P_NAME'].str.contains('dim'))
    )

    # Merge dataframes on filters
    merged_df = pd.merge(lineitem[filters],
                         part,
                         left_on='L_PARTKEY',
                         right_on='P_PARTKEY')
    merged_df = pd.merge(merged_df,
                         supplier,
                         left_on='L_SUPPKEY',
                         right_on='S_SUPPKEY')
    merged_df = pd.merge(merged_df,
                         partsupp,
                         left_on=['L_PARTKEY', 'L_SUPPKEY'],
                         right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
    merged_df = pd.merge(merged_df,
                         orders,
                         left_on='L_ORDERKEY',
                         right_on='O_ORDERKEY')
    merged_df = pd.merge(merged_df,
                         nation,
                         left_on='S_NATIONKEY',
                         right_on='N_NATIONKEY')

    # Calculate amount and sum_profit
    merged_df['AMOUNT'] = (merged_df['L_EXTENDEDPRICE'] *
                           (1 - merged_df['L_DISCOUNT'])) -\
                            (merged_df['PS_SUPPLYCOST'] *
                             merged_df['L_QUANTITY'])
    merged_df['O_YEAR'] = merged_df['O_ORDERDATE'].apply(lambda x: datetime.fromisoformat(x).year)
    profit = merged_df.groupby(['N_NAME', 'O_YEAR'])['AMOUNT'].sum().reset_index()
    profit.columns = ['NATION', 'O_YEAR', 'SUM_PROFIT']

    return profit

# Connect to Redis
db = DirectRedis(db=0, port=6379, host='redis')

# Load tables from Redis
nation = get_dataframe_from_redis('nation')
part = get_dataframe_from_redis('part')
supplier = get_dataframe_from_redis('supplier')
partsupp = get_dataframe_from_redis('partsupp')
orders = get_dataframe_from_redis('orders')
lineitem = get_dataframe_from_redis('lineitem')

# Calculate SUM_PROFIT
sum_profit = calculate_sum_profit(nation, part, supplier, partsupp, orders, lineitem)

# Save results to CSV
sum_profit.to_csv('query_output.csv', index=False)
