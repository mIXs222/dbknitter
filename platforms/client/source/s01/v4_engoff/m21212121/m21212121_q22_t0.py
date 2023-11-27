# sales_opportunity.py
import pymongo
from bson.json_util import dumps
import pandas as pd
import direct_redis
from datetime import datetime, timedelta

# Connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_customers_collection = mongo_db['customer']

# Query MongoDB for customers data
query = { 
    'C_PHONE': {'$regex': '^(20|40|22|30|39|42|21)'},
    'C_ACCTBAL': {'$gt': 0.0},
}
mongo_customers_cursor = mongo_customers_collection.find(query, {'_id': 0})
customer_data = pd.DataFrame(list(mongo_customers_cursor))

# Connection to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query Redis for orders data
orders_data_json = r.get('orders')
orders_data = pd.read_json(orders_data_json, orient='split')

# Filter out orders older than 7 years
seven_years_ago = (datetime.now() - timedelta(days=7*365)).date()
orders_data = orders_data[orders_data['O_ORDERDATE'] > str(seven_years_ago)]

# Find customers with no orders in the last 7 years
customers_no_recent_orders = customer_data[~customer_data['C_CUSTKEY'].isin(orders_data['O_CUSTKEY'])]

# Group by the first two characters of c_phone and calculate mean account balance
result = (customers_no_recent_orders.groupby(customers_no_recent_orders['C_PHONE'].str[:2])['C_ACCTBAL']
          .agg(['count', 'mean'])
          .reset_index()
          .rename(columns={'C_PHONE': 'Country_Code', 'count': 'Customer_Count', 'mean': 'Average_AcctBal'}))

# Write query result to csv file
result.to_csv('query_output.csv', index=False)
