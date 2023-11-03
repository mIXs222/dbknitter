import pandas as pd
import mysql.connector
import pymongo

# MySql Connection
mysql_conn = mysql.connector.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MongoDB Connection
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = client["tpch"]

# Define output file
output_file = "query_output.csv"

def execute_query():
    mysql_cursor = mysql_conn.cursor(buffered=True)

    # Get data from MySQL
    mysql_cursor.execute("SELECT * FROM PART WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'")
    parts = pd.DataFrame(mysql_cursor.fetchall(), columns=['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])

    # Get data from MongoDB
    lineitem_data = mongodb["lineitem"].find({}, {"_id":0})
    lineitem = pd.DataFrame(list(lineitem_data))

    # Merger the lineitem and part data
    merge_df = pd.merge(lineitem, parts, left_on='L_PARTKEY', right_on='P_PARTKEY')

    # Compute avg quantity
    avg_quantity = merge_df.groupby('P_PARTKEY')['L_QUANTITY'].mean()*0.2
    avg_quantity.rename('AVG_QUANTITY', inplace=True)

    # Merge with quantity data
    merge_df = pd.merge(merge_df, avg_quantity, left_on='P_PARTKEY', right_index=True)

    # Condition for L_QUANTITY < 0.2 * AVG(L_QUANTITY)
    merge_df = merge_df[merge_df['L_QUANTITY'] < merge_df['AVG_QUANTITY']]

    # aggregate data
    final_result = merge_df['L_EXTENDEDPRICE'].sum() / 7.0

    # save the result
    pd.DataFrame([final_result], columns=['AVG_YEARLY']).to_csv(output_file, index=False)

if __name__ == '__main__':
    execute_query()
