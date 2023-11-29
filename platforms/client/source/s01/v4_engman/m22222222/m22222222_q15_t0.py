import pandas as pd
import direct_redis

def get_dataframe_from_redis(table_name):
    dr = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    df = pd.read_json(dr.get(table_name))
    return df

def main():
    # Get dataframes from Redis
    supplier_df = get_dataframe_from_redis('supplier')
    lineitem_df = get_dataframe_from_redis('lineitem')

    # Convert SHIPDATE to datetime and filter records
    lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
    filtered_lineitem = lineitem_df[(lineitem_df['L_SHIPDATE'] >= '1996-01-01') &
                                    (lineitem_df['L_SHIPDATE'] <= '1996-04-01')]

    # Calculate revenue for each line
    filtered_lineitem['REVENUE'] = filtered_lineitem['L_EXTENDEDPRICE'] * (1 - filtered_lineitem['L_DISCOUNT'])

    # Group by supplier key and calculate total revenue
    revenue_per_supplier = filtered_lineitem.groupby('L_SUPPKEY')['REVENUE'].sum().reset_index()
    
    # Merge supplier and revenue data
    result = supplier_df.merge(revenue_per_supplier, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
    
    # Get the maximum revenue
    max_revenue = result['REVENUE'].max()
    # Filter suppliers with the maximum revenue
    top_suppliers = result[result['REVENUE'] == max_revenue]
    
    # Sort by S_SUPPKEY and select columns
    top_suppliers_sorted = top_suppliers.sort_values('S_SUPPKEY')[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'REVENUE']]
    
    # Rename columns
    top_suppliers_sorted.rename(columns={'REVENUE': 'TOTAL_REVENUE'}, inplace=True)
    
    # Write to CSV
    top_suppliers_sorted.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
