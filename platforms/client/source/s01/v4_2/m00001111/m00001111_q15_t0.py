import pymysql
import pymongo
import pandas as pd
from datetime import datetime, timedelta

# Connect to MySQL Database
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')
mysql_cursor = mysql_conn.cursor()

mysql_cursor.execute("SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE FROM supplier")
supplier = pd.DataFrame(mysql_cursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE'])

# Connect to MongoDB 
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem = mongo_db['lineitem']

start_date = datetime.strptime('1996-01-01', '%Y-%m-%d')
end_date = start_date + timedelta(3*365/12)

revenue0 = lineitem.aggregate([
    {
        '$match': {
            'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}
        }
    },
    {
        '$group': {
            '_id': '$L_SUPPKEY',
            'TOTAL_REVENUE': {
                '$sum': {
                    '$multiply': ["$L_EXTENDEDPRICE", {'$subtract': [1, "$L_DISCOUNT"]}]
                }
            }
        }
    }
])

revenue0_df = pd.DataFrame(list(revenue0))
revenue0_df.columns = ['SUPPLIER_NO', 'TOTAL_REVENUE']

max_revenue = revenue0_df['TOTAL_REVENUE'].max()
revenue0_filter = revenue0_df[revenue0_df['TOTAL_REVENUE'] == max_revenue]

results = pd.merge(supplier, revenue0_filter, how='inner', left_on='S_SUPPKEY', right_on='SUPPLIER_NO')
results.to_csv('query_output.csv', index=False)
