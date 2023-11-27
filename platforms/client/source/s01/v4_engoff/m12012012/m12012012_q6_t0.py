import pandas as pd
from direct_redis import DirectRedis

def connect_to_redis(host, port, db):
    return DirectRedis(host=host, port=port, db=db)

def get_redis_data(table_name, redis_connection):
    data = redis_connection.get(table_name)
    return pd.read_json(data, orient='split')

def calculate_revenue_increase(df_lineitem):
    # Convert dates to datetime objects for comparison
    df_lineitem['L_SHIPDATE'] = pd.to_datetime(df_lineitem['L_SHIPDATE'])
    
    # Filter the dataframe based on the given conditions
    filtered_df = df_lineitem[
        (df_lineitem['L_SHIPDATE'] >= pd.Timestamp('1994-01-01')) &
        (df_lineitem['L_SHIPDATE'] < pd.Timestamp('1995-01-01')) &
        (df_lineitem['L_DISCOUNT'] >= 0.06 - 0.01) &
        (df_lineitem['L_DISCOUNT'] <= 0.06 + 0.01) &
        (df_lineitem['L_QUANTITY'] < 24)
    ]
    
    # Calculate the potential revenue increase
    filtered_df['REVENUE_INCREASE'] = filtered_df['L_EXTENDEDPRICE'] * filtered_df['L_DISCOUNT']
    total_revenue_increase = filtered_df['REVENUE_INCREASE'].sum()
    
    return total_revenue_increase

def main():
    # Connect to Redis
    redis_conn = connect_to_redis(host='redis', port=6379, db=0)
    
    # Fetch the lineitem table from Redis
    df_lineitem = get_redis_data('lineitem', redis_conn)
    
    # Calculate revenue increase
    total_revenue_increase = calculate_revenue_increase(df_lineitem)
    
    # Output the result to a CSV file
    pd.DataFrame([{'TOTAL_REVENUE_INCREASE': total_revenue_increase}]).to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
