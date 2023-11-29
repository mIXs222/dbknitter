import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Define a function to execute the query on MySQL
def mysql_query():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    query = """
    SELECT s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS, s.S_PHONE, sum(ps.PS_AVAILQTY) as total_qty
    FROM supplier s
    JOIN partsupp ps ON s.S_SUPPKEY = ps.PS_SUPPKEY
    GROUP BY s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS, s.S_PHONE
    HAVING total_qty > (SELECT 0.5 * sum(PS_AVAILQTY) FROM partsupp WHERE PS_PARTKEY IN (
        SELECT P_PARTKEY FROM part WHERE P_NAME LIKE 'forest%'))
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
    connection.close()
    return result

# Define a function to execute the query on MongoDB
def mongodb_query():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client['tpch']
    # As per the given instruction, no specific query is to be run on MongoDB
    parts = pd.DataFrame(list(db.part.find({"P_NAME": {"$regex": "^forest"}}, {"_id": 0, "P_PARTKEY": 1})))
    part_keys = parts['P_PARTKEY'].tolist()
    return part_keys

# Connect to Redis and retrieve the data
def redis_query():
    client = DirectRedis(host='redis', port=6379, db=0)
    df_lineitem = client.get('lineitem')
    # Filter dates and calculate total quantities shipped for CANADA
    df_lineitem = df_lineitem[(df_lineitem['L_SHIPDATE'] >= '1994-01-01') & (df_lineitem['L_SHIPDATE'] < '1995-01-01')]
    supplier_qty = df_lineitem[df_lineitem['L_SUPPKEY'].isin(part_supplier_keys)]['L_SUPPKEY'].value_counts()
    return supplier_qty

# Join and merge data from all sources and write to the file
def main():
    suppliers = pd.DataFrame(mysql_query(), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'total_qty'])
    part_keys = mongodb_query()
    supplier_qty = redis_query()
    qualified_suppliers = suppliers[suppliers['S_SUPPKEY'].isin(supplier_qty.index)]

    # Save the result to a CSV file
    qualified_suppliers.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
