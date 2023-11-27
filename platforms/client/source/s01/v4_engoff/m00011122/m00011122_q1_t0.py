import pandas as pd
import direct_redis

# Function to calculate pricing summary
def calculate_pricing_summary(df):
    # Convert relevant columns to numeric type for calculations
    df['L_QUANTITY'] = pd.to_numeric(df['L_QUANTITY'])
    df['L_EXTENDEDPRICE'] = pd.to_numeric(df['L_EXTENDEDPRICE'])
    df['L_DISCOUNT'] = pd.to_numeric(df['L_DISCOUNT'])
    df['L_TAX'] = pd.to_numeric(df['L_TAX'])

    # Calculate discounted price and discounted price with tax
    df['disc_price'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])
    df['disc_price_tax'] = df['disc_price'] * (1 + df['L_TAX'])
    
    # Filter the data by shipdate
    df = df[pd.to_datetime(df['L_SHIPDATE']) < pd.Timestamp('1998-09-02')]

    # Perform the aggregation
    result = df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
        total_qty=pd.NamedAgg(column='L_QUANTITY', aggfunc='sum'),
        total_base_price=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='sum'),
        total_disc_price=pd.NamedAgg(column='disc_price', aggfunc='sum'),
        total_disc_price_tax=pd.NamedAgg(column='disc_price_tax', aggfunc='sum'),
        avg_qty=pd.NamedAgg(column='L_QUANTITY', aggfunc='mean'),
        avg_price=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='mean'),
        avg_disc=pd.NamedAgg(column='L_DISCOUNT', aggfunc='mean'),
        count_order=pd.NamedAgg(column='L_ORDERKEY', aggfunc='count'),
    ).reset_index()

    # Sort the results
    result.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'], inplace=True)
    return result

# Connect to Redis database
connection_info = {
    'host': 'redis',
    'port': 6379,
    'db': 0,
}
redis_client = direct_redis.DirectRedis(**connection_info)

# Get the lineitem table from Redis
lineitem_df = pd.read_json(redis_client.get('lineitem').decode('utf-8'))

# Calculate summary report
pricing_summary = calculate_pricing_summary(lineitem_df)

# Write the summary report to CSV
pricing_summary.to_csv('query_output.csv', index=False)
