# VolumeShippingQuery.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

def get_supplier_customer_from_mysql():
    conn = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch'
    )

    try:
        with conn.cursor() as cursor:
            query = """
            SELECT
                s.S_NATIONKEY AS SUPP_NATIONKEY,
                c.C_NATIONKEY AS CUST_NATIONKEY,
                s.S_SUPPKEY, c.C_CUSTKEY
            FROM
                supplier s, customer c
            WHERE
                s.S_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME='INDIA' OR N_NAME = 'JAPAN')
                AND c.C_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME='INDIA' OR N_NAME = 'JAPAN')
                AND s.S_NATIONKEY != c.C_NATIONKEY;
            """

            cursor.execute(query)
            result = cursor.fetchall()
            return pd.DataFrame(result, columns=["SUPP_NATIONKEY", "CUST_NATIONKEY", "S_SUPPKEY", "C_CUSTKEY"])
    finally:
        conn.close()

def get_nations_from_mongodb():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client.tpch
    nations = list(db.nation.find({'N_NAME': {'$in': ['INDIA', 'JAPAN']}}, {'_id': False}))
    client.close()
    return pd.DataFrame(nations)

def get_lineitem_from_redis():
    client = DirectRedis(host='redis', port=6379, db=0)
    df = pd.read_json(client.get('lineitem'))
    return df[df['L_SHIPDATE'].dt.year.isin([1995, 1996])]

def main():
    df_sup_cust = get_supplier_customer_from_mysql()
    df_nations = get_nations_from_mongodb()
    df_lineitem = get_lineitem_from_redis()

    nation_map = df_nations.set_index('N_NATIONKEY')['N_NAME'].to_dict()

    df_results = pd.merge(
        df_lineitem,
        df_sup_cust,
        how='inner',
        left_on='L_SUPPKEY',
        right_on='S_SUPPKEY'
    )

    df_results = df_results[df_results['L_ORDERKEY'].isin(df_sup_cust['C_CUSTKEY'])]

    df_results['REVENUE'] = df_results['L_EXTENDEDPRICE'] * (1 - df_results['L_DISCOUNT'])
    df_results['SUPP_NATION'] = df_results['SUPP_NATIONKEY'].map(nation_map)
    df_results['CUST_NATION'] = df_results['CUST_NATIONKEY'].map(nation_map)
    df_results['L_YEAR'] = df_results['L_SHIPDATE'].dt.year

    final_df = df_results.groupby(['SUPP_NATION', 'CUST_NATION', 'L_YEAR']).agg({'REVENUE': 'sum'}).reset_index()
    final_df = final_df.sort_values(['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])
    final_df[['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION']].to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
