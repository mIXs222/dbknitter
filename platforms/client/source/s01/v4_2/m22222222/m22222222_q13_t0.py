import pandas as pd
import redis
from io import StringIO

def connect_to_redis():
    r = redis.Redis(host='redis', port=6379, db=0)
    return r

def get_df_from_redis(r, table_name):
    data = r.get(table_name)
    df = pd.read_csv(StringIO(data.decode('utf-8')))
    return df

def execute_query():
    r = connect_to_redis()

    customer_df = get_df_from_redis(r, "customer")
    orders_df = get_df_from_redis(r, "orders")

    merged_df = pd.merge(customer_df, orders_df, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    merged_df['O_COMMENT_mod'] = merged_df['O_COMMENT'].apply(lambda x: 1 if 'pending' in x and 'deposits' in x else 0)

    c_orders_df = merged_df.groupby('C_CUSTKEY').agg(C_COUNT=('O_ORDERKEY', 'count')).reset_index()

    final_df = c_orders_df.groupby('C_COUNT').agg(CUSTDIST=('C_CUSTKEY', 'count')).reset_index()

    final_df.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False], inplace=True)
    
    final_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    execute_query()
