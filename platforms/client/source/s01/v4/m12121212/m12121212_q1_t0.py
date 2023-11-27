# query_redis.py
import pandas as pd
import direct_redis

def query_redis():
    # Connection information for Redis
    hostname = 'redis'
    port = 6379
    database_name = 0

    # Connect to Redis
    redis_conn = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)

    # Read the data from Redis into a Pandas DataFrame
    lineitem_data = redis_conn.get('lineitem')
    df_lineitem = pd.read_json(lineitem_data)
    
    # Convert shipdate to datetime for comparison
    df_lineitem['L_SHIPDATE'] = pd.to_datetime(df_lineitem['L_SHIPDATE'])

    # Perform the query on the DataFrame
    result = (
        df_lineitem[df_lineitem['L_SHIPDATE'] <= pd.Timestamp('1998-09-02')]
        .groupby(['L_RETURNFLAG', 'L_LINESTATUS'])
        .agg(
            SUM_QTY=pd.NamedAgg(column='L_QUANTITY', aggfunc='sum'),
            SUM_BASE_PRICE=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='sum'),
            SUM_DISC_PRICE=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc=lambda x: (x * (1 - df_lineitem.loc[x.index, 'L_DISCOUNT'])).sum()),
            SUM_CHARGE=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc=lambda x: (x * (1 - df_lineitem.loc[x.index, 'L_DISCOUNT']) * (1 + df_lineitem.loc[x.index, 'L_TAX'])).sum()),
            AVG_QTY=pd.NamedAgg(column='L_QUANTITY', aggfunc='mean'),
            AVG_PRICE=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='mean'),
            AVG_DISC=pd.NamedAgg(column='L_DISCOUNT', aggfunc='mean'),
            COUNT_ORDER=pd.NamedAgg(column='L_ORDERKEY', aggfunc='count')
        )
        .reset_index()
        .sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])
    )

    # Output the result to a CSV file
    result.to_csv('query_output.csv', index=False)


if __name__ == "__main__":
    query_redis()
