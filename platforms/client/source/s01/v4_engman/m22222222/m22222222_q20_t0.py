import pandas as pd
import direct_redis

def main():
    # Initialize connection to Redis
    dr = direct_redis.DirectRedis(hostname='redis', port=6379, db=0)
    
    # Read tables from Redis
    df_nation = pd.read_json(dr.get('nation'))
    df_supplier = pd.read_json(dr.get('supplier'))
    df_partsupp = pd.read_json(dr.get('partsupp'))
    df_part = pd.read_json(dr.get('part'))
    df_lineitem = pd.read_json(dr.get('lineitem'))

    # Define the time range and nation name for the query
    start_date = pd.Timestamp('1994-01-01')
    end_date = pd.Timestamp('1995-01-01')
    nation_name = 'CANADA'
    
    # Filter records by date and naming convention
    df_lineitem_filtered = df_lineitem[
        (df_lineitem['L_SHIPDATE'] >= start_date) & (df_lineitem['L_SHIPDATE'] < end_date)
    ]
    
    df_part_filtered = df_part[df_part['P_NAME'].str.contains('forest', case=False)]
    
    # Join to locate the suppliers in the nation of interest
    df_nation_filtered = df_nation[df_nation['N_NAME'] == nation_name]
    supplier_nation = df_supplier.merge(df_nation_filtered, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    
    # Combine the filtered datasets to execute the query
    combined_data = (
        df_lineitem_filtered[['L_PARTKEY', 'L_SUPPKEY', 'L_QUANTITY']]
        .merge(df_part_filtered[['P_PARTKEY']], left_on='L_PARTKEY', right_on='P_PARTKEY')
        .merge(supplier_nation[['S_SUPPKEY']], on='L_SUPPKEY')
    )
    
    grouped_data = combined_data.groupby('L_SUPPKEY').agg(
        total_quantity=('L_QUANTITY', 'sum'),
        part_count=('L_PARTKEY', 'nunique')
    ).reset_index()
    
    # Apply the excess criteria
    result = grouped_data[grouped_data['total_quantity'] > 1.5 * grouped_data['part_count']]
    
    # Extract supplier details
    final_result = result.merge(df_supplier[['S_SUPPKEY', 'S_NAME']], on='S_SUPPKEY', how='left')

    # Write the query's output to a CSV file
    final_result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
