import pymongo
from pymongo import MongoClient
import csv
from datetime import datetime
from collections import defaultdict


def is_high_priority(order_priority):
    return order_priority in ['1-URGENT', '2-HIGH']


def is_not_high_priority(order_priority):
    return order_priority not in ['1-URGENT', '2-HIGH']


def date_in_range(date):
    return date >= datetime.strptime('1994-01-01', '%Y-%m-%d') and \
           date < datetime.strptime('1995-01-01', '%Y-%m-%d')


one_year_ago = datetime.now() - timedelta(days=365)
orders_dict = defaultdict(list)
client = MongoClient('mongodb', 27017)
tpch = client.tpch
orders = tpch['orders']
lineitem = tpch['lineitem']

orders_list = list(
    orders.find({}, {'_id': False, 'O_ORDERKEY': True, 'O_ORDERPRIORITY': True}))
lineitem_list = list(
    lineitem.find({'L_SHIPMODE': {'$in': ['MAIL', 'SHIP']}, 'L_COMMITDATE': {'$lt': 'L_RECEIPTDATE'},
                   'L_SHIPDATE': {'$lt': 'L_COMMITDATE'}, 'L_RECEIPTDATE': {'$gte': '1994-01-01', '$lt': '1995-01-01'}}))

for order in orders_list:
    orders_dict[order['O_ORDERKEY']].append(order['O_ORDERPRIORITY'])

query_result = []

for item in lineitem_list:
    if item['L_ORDERKEY'] in orders_dict:
        order_priority = orders_dict[item['L_ORDERKEY']][0]
        high_line_count = 1 if is_high_priority(order_priority) else 0
        low_line_count = 1 if is_not_high_priority(order_priority) else 0
        query_result.append(
            (item['L_SHIPMODE'], high_line_count, low_line_count))
query_result.sort()

with open('query_output.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['L_SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])
    writer.writerows(query_result)
