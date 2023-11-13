from pymongo import MongoClient
from pymysql import connect
from pandas import DataFrame

# connect to MySQL database
mysql_conn = connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
cur = mysql_conn.cursor()

# connect to MongoDB database
mongo_conn = MongoClient('mongodb', 27017)
mongo_db = mongo_conn['tpch']

# get data from orders table in MySQL
mysql_query = "SELECT O_ORDERKEY, O_ORDERPRIORITY FROM orders WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01'"
cur.execute(mysql_query)
mysql_data = cur.fetchall()

# get data from lineitem table in MongoDB
mongo_data = list(mongo_db['lineitem'].find())

# combine data
combined_data = []
for m_data in mysql_data:
    for lineitem in mongo_data:
        if m_data[0] == lineitem['L_ORDERKEY'] and lineitem['L_COMMITDATE'] < lineitem['L_RECEIPTDATE']:
            combined_data.append((m_data[1], m_data[0]))

# convert combined data into dataframe
df = DataFrame(combined_data, columns=['O_ORDERPRIORITY', 'COUNT'])
result = df.groupby(['O_ORDERPRIORITY']).size().reset_index(name='ORDER_COUNT')

# write result to csv file
result.to_csv('query_output.csv', index=False)

mysql_conn.close()
mongo_conn.close()
