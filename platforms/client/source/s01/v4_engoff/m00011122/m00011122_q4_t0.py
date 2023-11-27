# main.py
import pandas as pd
import direct_redis

def load_data_from_redis(redis_host, redis_port, redis_db, table_name):
    r = direct_redis.DirectRedis(host=redis_host, port=redis_port, db=redis_db)
    return pd.read_json(r.get(table_name))

def save_results_to_csv(results, filename):
    results.to_csv(filename, index=False)

def main():
    redis_host = 'redis'
    redis_port = 6379
    redis_db = 0
    
    # Load orders and lineitem data from Redis
    orders = load_data_from_redis(redis_host, redis_port, redis_db, 'orders')
    lineitem = load_data_from_redis(redis_host, redis_port, redis_db, 'lineitem')
    
    # Convert date columns to datetime for comparison
    orders['O_ORDERDATE'] = pd.to_datetime(orders['O_ORDERDATE'])
    lineitem['L_COMMITDATE'] = pd.to_datetime(lineitem['L_COMMITDATE'])
    lineitem['L_RECEIPTDATE'] = pd.to_datetime(lineitem['L_RECEIPTDATE'])
    
    # Filter orders and line items based on conditions
    filtered_orders = orders[(orders['O_ORDERDATE'] >= '1993-07-01') & (orders['O_ORDERDATE'] <= '1993-10-01')]
    late_lineitems = lineitem[lineitem['L_RECEIPTDATE'] > lineitem['L_COMMITDATE']]
    
    # Merge orders and line items on L_ORDERKEY == O_ORDERKEY
    merged_data = pd.merge(filtered_orders, late_lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')
    
    # Group by O_ORDERPRIORITY and count unique O_ORDERKEY
    result = merged_data.groupby('O_ORDERPRIORITY')['O_ORDERKEY'].nunique().reset_index()
    result.columns = ['O_ORDERPRIORITY', 'ORDER_COUNT']
    result.sort_values('O_ORDERPRIORITY', ascending=True, inplace=True)
    
    # Save results to CSV
    save_results_to_csv(result, 'query_output.csv')

if __name__ == "__main__":
    main()
