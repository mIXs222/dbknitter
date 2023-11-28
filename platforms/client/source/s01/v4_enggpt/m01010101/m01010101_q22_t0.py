# Python code to execute the mixed database query.
import pymysql
import pymongo
import pandas as pd
from pymongo import MongoClient

# Function to perform analysis and write to CSV.
def analyze_and_write_to_csv():
    # Connect to MySQL
    mysql_conn = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 db='tpch')
    mysql_cur = mysql_conn.cursor()

    # Query MySQL to get customers without orders (`orders` table is in MySQL)
    mysql_query = """
    SELECT C_CUSTKEY, C_ACCTBAL, SUBSTR(C_PHONE, 1, 2) AS CNTRYCODE
    FROM customer AS c
    WHERE NOT EXISTS (
        SELECT * FROM orders WHERE O_CUSTKEY = c.C_CUSTKEY
    )
    """
    mysql_cur.execute(mysql_query)
    mysql_customers = mysql_cur.fetchall()

    mysql_cust_df = pd.DataFrame(mysql_customers, columns=['C_CUSTKEY', 'C_ACCTBAL', 'CNTRYCODE'])

    # Close MySQL connection
    mysql_cur.close()
    mysql_conn.close()
    
    # Connect to MongoDB
    client = MongoClient('mongodb', 27017)
    db = client.tpch
    customers_collection = db.customer

    # Filter MongoDB customers based on extracted CNTRYCODEs
    allowed_cntrycodes = ['20', '40', '22', '30', '39', '42', '21']
    mongo_customers = list(customers_collection.find({'C_PHONE': {'$regex': '^(20|40|22|30|39|42|21)'}}))
    
    # Create a DataFrame from MongoDB data
    mongo_cust_df = pd.DataFrame(mongo_customers)

    # Compute the average account balance for the allowed country codes.
    avg_balances = mongo_cust_df.loc[mongo_cust_df['CNTRYCODE'].isin(allowed_cntrycodes) & (mongo_cust_df['C_ACCTBAL'] > 0)].groupby('CNTRYCODE')['C_ACCTBAL'].mean()

    # Merge MySQL and MongoDB dataframes
    combined_df = pd.merge(mysql_cust_df, mongo_cust_df, how='inner', on='C_CUSTKEY')
    
    # Filter based on average account balance condition
    combined_df = combined_df[combined_df['C_ACCTBAL_x'] > combined_df['CNTRYCODE'].map(avg_balances)]
    
    # Group by country code to calculate summary
    result = combined_df.groupby('CNTRYCODE').agg(NUMCUST=pd.NamedAgg(column='C_CUSTKEY', aggfunc='count'), 
                                                   TOTACCTBAL=pd.NamedAgg(column='C_ACCTBAL_x', aggfunc='sum')).reset_index()

    # Write to CSV
    result.sort_values('CNTRYCODE').to_csv('query_output.csv', index=False)

analyze_and_write_to_csv()
