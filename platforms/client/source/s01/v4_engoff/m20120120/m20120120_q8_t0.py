# market_share_analysis.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Function to get data from MySQL
def get_mysql_data():
    mysql_conn = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        database='tpch'
    )
    cursor = mysql_conn.cursor()
    
    supplier_sql = """
    SELECT S_SUPPKEY FROM supplier
    WHERE S_NATION = 'INDIA'
    """
    cursor.execute(supplier_sql)
    supplier_data = cursor.fetchall()
    
    lineitem_sql = """
    SELECT L_SUPPKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE
    FROM lineitem
    WHERE L_PARTKEY IN (
        SELECT P_PARTKEY FROM part
        WHERE P_TYPE = 'SMALL PLATED COPPER'
    )
    """
    cursor.execute(lineitem_sql)
    lineitem_data = cursor.fetchall()
    
    cursor.close()
    mysql_conn.close()
    return supplier_data, lineitem_data

# Function to get data from MongoDB
def get_mongodb_data():
    mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
    db = mongo_client["tpch"]
    
    part_query = {"P_TYPE": "SMALL PLATED COPPER"}
    part_data = pd.DataFrame(list(db.part.find(part_query)))
    
    return part_data

# Function to get data from Redis
def get_redis_data():
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    
    nation_data = pd.read_json(redis_client.get('nation'))
    nation_data = nation_data.loc[nation_data['N_NAME'].str.contains('ASIA')]
    
    return nation_data

# Combine data from all databases and calculate market share
def calculate_market_share(suppliers, lineitems, parts, nations):
    suppliers_df = pd.DataFrame(suppliers, columns=['SUPPKEY'])
    lineitems_df = pd.DataFrame(lineitems, columns=['SUPPKEY', 'EXTENDEDPRICE', 'DISCOUNT', 'SHIPDATE'])
    india_suppliers = suppliers_df['SUPPKEY'].unique()
    
    # Filter lineitems for India suppliers
    lineitems_df = lineitems_df[lineitems_df['SUPPKEY'].isin(india_suppliers)]
    
    # Only consider the years 1995 and 1996
    lineitems_df['SHIPDATE'] = pd.to_datetime(lineitems_df['SHIPDATE'])
    lineitems_df['YEAR'] = lineitems_df['SHIPDATE'].dt.year
    lineitems_df = lineitems_df[(lineitems_df['YEAR'] == 1995) | (lineitems_df['YEAR'] == 1996)]
    
    # Filter parts for SMALL PLATED COPPER
    parts_df = parts[parts['P_TYPE'] == 'SMALL PLATED COPPER']
    
    # Calculate revenue
    lineitems_df['REVENUE'] = lineitems_df['EXTENDEDPRICE'] * (1 - lineitems_df['DISCOUNT'])
    
    # Market share calculation
    market_share = lineitems_df.groupby('YEAR')['REVENUE'].sum() / lineitems_df['REVENUE'].sum()
    
    return market_share

# Main execution
if __name__ == "__main__":
    suppliers, lineitems = get_mysql_data()
    parts = get_mongodb_data()
    nations = get_redis_data()
    
    market_share = calculate_market_share(suppliers, lineitems, parts, nations)
    
    # Write to CSV file
    market_share.to_csv('query_output.csv', header=True)

