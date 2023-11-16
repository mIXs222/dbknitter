import pandas as pd
import direct_redis

def query_redis():
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    df_lineitem = pd.read_json(r.get('lineitem'))
    
    filtered_lineitem = df_lineitem[
        (df_lineitem['L_SHIPDATE'] >= '1994-01-01') &
        (df_lineitem['L_SHIPDATE'] < '1995-01-01') &
        (df_lineitem['L_DISCOUNT'] >= 0.06 - 0.01) &
        (df_lineitem['L_DISCOUNT'] <= 0.06 + 0.01) &
        (df_lineitem['L_QUANTITY'] < 24)
    ]

    result = pd.DataFrame({
        "REVENUE": [filtered_lineitem.eval('L_EXTENDEDPRICE * L_DISCOUNT').sum()]
    })

    result.to_csv('query_output.csv', index=False)

query_redis()
