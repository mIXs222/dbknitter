import pandas as pd
from direct_redis import DirectRedis

def main():
    # Connect to the Redis database
    redis_connection = DirectRedis(host='redis', port=6379, db=0)

    # Retrieve the lineitem DataFrame from Redis
    lineitem_data = pd.read_json(redis_connection.get('lineitem'), orient='records')

    # Convert 'L_SHIPDATE' to datetime and filter the records
    lineitem_data['L_SHIPDATE'] = pd.to_datetime(lineitem_data['L_SHIPDATE'])
    filtered_data = lineitem_data[lineitem_data['L_SHIPDATE'] <= '1998-09-02']

    # Perform the aggregation
    aggregate_functions = {
        'L_QUANTITY': ['sum', 'mean'],
        'L_EXTENDEDPRICE': ['sum', 'mean'],
        'L_DISCOUNT': 'mean',
        'L_TAX': 'mean',
        'L_ORDERKEY': 'count'
    }

    result = filtered_data.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(aggregate_functions)
    result.columns = ['SUM_QTY', 'AVG_QTY', 'SUM_BASE_PRICE', 'AVG_PRICE', 'AVG_DISC', 'AVG_TAX', 'COUNT_ORDER']

    # Calculate SUM_DISC_PRICE and SUM_CHARGE
    result['SUM_DISC_PRICE'] = filtered_data.apply(lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']), axis=1).groupby([filtered_data['L_RETURNFLAG'], filtered_data['L_LINESTATUS']]).sum()
    result['SUM_CHARGE'] = filtered_data.apply(lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']) * (1 + row['L_TAX']), axis=1).groupby([filtered_data['L_RETURNFLAG'], filtered_data['L_LINESTATUS']]).sum()

    # Sort the results
    result = result.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

    # Output the results to a CSV file
    result.to_csv('query_output.csv')

if __name__ == '__main__':
    main()
