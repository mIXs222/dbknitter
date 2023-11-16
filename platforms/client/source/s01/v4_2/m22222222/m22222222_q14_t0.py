import pandas as pd
import redis

def fetch_table_from_redis(tablename):
    r = redis.Redis(host='redis', port=6379, db=0)
    data = r.get(tablename)
    return pd.read_json(data)

def eval_query(part, lineitem):
    lineitem['L_SHIPDATE'] = pd.to_datetime(lineitem['L_SHIPDATE'])
    mask = (lineitem['L_SHIPDATE'] >= '1995-09-01') & (lineitem['L_SHIPDATE'] < '1995-10-01')
    lineitem = lineitem.loc[mask]
    combined = pd.merge(part, lineitem, left_on='P_PARTKEY', right_on='L_PARTKEY')    
    combined['DISCOUNT_PRICE'] = combined['L_EXTENDEDPRICE'] * (1 - combined['L_DISCOUNT'])
    promo = combined[combined['P_TYPE'].str.startswith('PROMO')]['DISCOUNT_PRICE'].sum()
    total = combined['DISCOUNT_PRICE'].sum()
    result = 100.00 * promo / total
    result = pd.DataFrame([result], columns=['PROMO_REVENUE'])
    result.to_csv('query_output.csv', index=False)

def main():
    part = fetch_table_from_redis('part')
    lineitem = fetch_table_from_redis('lineitem')
    eval_query(part, lineitem)

if __name__ == "__main__":
    main()
