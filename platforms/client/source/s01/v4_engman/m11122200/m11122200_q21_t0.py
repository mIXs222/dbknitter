import pandas as pd
import pymysql
import pymongo
import direct_redis

def get_mysql_data():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    try:
        orders_df = pd.read_sql("SELECT * FROM orders WHERE O_ORDERSTATUS='F'", connection)
        lineitem_df = pd.read_sql("SELECT * FROM lineitem", connection)
    finally:
        connection.close()
    return orders_df, lineitem_df

def get_mongodb_data():
    client = pymongo.MongoClient(host='mongodb', port=27017)
    db = client['tpch']
    nation_col = db['nation']
    nations_df = pd.DataFrame(list(nation_col.find({"N_NAME": "SAUDI ARABIA"})))
    return nations_df

def get_redis_data():
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    supplier_df = pd.DataFrame(eval(r.get('supplier')))
    return supplier_df

def execute_query(orders_df, lineitem_df, nations_df, supplier_df):
    # Filtering the lineitem dataframe to get the lineitems with failed commit date
    failed_lineitem_df = lineitem_df[lineitem_df['L_RETURNFLAG'] == 'F'].copy()
    
    # Getting only orders with multiple suppliers
    multi_supplier_orders = failed_lineitem_df.groupby('L_ORDERKEY').filter(lambda x: x['L_SUPPKEY'].nunique() > 1)
    
    # Identifying the single failed suppliers
    single_failed_supplier_df = multi_supplier_orders.groupby('L_ORDERKEY').filter(lambda x: (x['L_RECEIPTDATE'] > x['L_COMMITDATE']).sum() == 1)
    
    # Get the suppliers for these orders
    suppliers_in_multi_supplier_orders = single_failed_supplier_df[['L_ORDERKEY', 'L_SUPPKEY']].drop_duplicates()
    
    # Join with the suppliers data
    suppliers_in_multi_supplier_orders = suppliers_in_multi_supplier_orders.merge(supplier_df, how='left', left_on='L_SUPPKEY', right_on='S_SUPPKEY')

    # Only keep suppliers that are in the nation 'SAUDI ARABIA'
    saudi_suppliers = suppliers_in_multi_supplier_orders[suppliers_in_multi_supplier_orders['S_NATIONKEY'].isin(nations_df['N_NATIONKEY'])]
    
    # Count the number of waits per supplier
    numwait_df = saudi_suppliers.groupby(['S_NAME'])['L_ORDERKEY'].count().reset_index(name='NUMWAIT')
    
    # Sort the results as per the query
    sorted_result = numwait_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])
    
    # Output the results to a CSV
    sorted_result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    orders_df, lineitem_df = get_mysql_data()
    nations_df = get_mongodb_data()
    supplier_df = get_redis_data()
    execute_query(orders_df, lineitem_df, nations_df, supplier_df)
