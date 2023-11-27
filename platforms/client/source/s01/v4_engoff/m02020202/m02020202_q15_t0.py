import pandas as pd
import direct_redis

def fetch_table_as_dataframe(redis_connection, table_name):
    table_json = redis_connection.get(table_name)
    if table_json:
        return pd.read_json(table_json, orient='index')
    else:
        return None

def main():
    # Establish Redis connection
    redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    
    # Fetch data
    supplier_df = fetch_table_as_dataframe(redis_conn, 'supplier')
    lineitem_df = fetch_table_as_dataframe(redis_conn, 'lineitem')
    
    # Filter lineitem for dates
    mask = (lineitem_df['L_SHIPDATE'] >= '1996-01-01') & (lineitem_df['L_SHIPDATE'] < '1996-04-01')
    filtered_lineitem_df = lineitem_df.loc[mask]
    
    # Calculate total revenue contribution per supplier
    revenue_per_supplier = (
        filtered_lineitem_df
        .groupby('L_SUPPKEY')['L_EXTENDEDPRICE']
        .sum()
        .reset_index()
    )
    
    # Merge with supplier information
    merged_df = pd.merge(supplier_df, revenue_per_supplier, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
    
    # Determine the top supplier(s)
    max_revenue = merged_df['L_EXTENDEDPRICE'].max()
    top_suppliers_df = merged_df[merged_df['L_EXTENDEDPRICE'] == max_revenue].sort_values(by='S_SUPPKEY')

    # Write to CSV
    top_suppliers_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
