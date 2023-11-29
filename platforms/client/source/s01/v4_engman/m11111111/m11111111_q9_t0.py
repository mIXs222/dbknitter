from pymongo import MongoClient
from datetime import datetime
import csv

# Constants
MONGO_HOST = 'mongodb'
MONGO_PORT = 27017
MONGO_DB = 'tpch'
SPECIFIED_DIM = 'SPECIFIED_DIM'

# Establish MongoDB connection
client = MongoClient(MONGO_HOST, MONGO_PORT)
db = client[MONGO_DB]

# Function to perform the query
def product_type_profit_measure():
    # Fetch all required collections
    lineitems = db.lineitem.find()
    partsupp = db.partsupp.find()
    parts = db.part.find({'P_NAME': {'$regex': SPECIFIED_DIM}})
    suppliers = db.supplier.find()
    nations = db.nation.find()

    # Convert collections to lists for processing
    lineitem_list = list(lineitems)
    partsupp_list = list(partsupp)
    part_list = list(parts)
    supplier_list = list(suppliers)
    nation_list = list(nations)

    # Pre-process to create dictionaries for efficient lookup
    parts_dict = {part['P_PARTKEY']: part for part in part_list}
    suppliers_dict = {sup['S_SUPPKEY']: sup for sup in supplier_list}
    nations_dict = {nation['N_NATIONKEY']: nation for nation in nation_list}
    partsupp_dict = {(ps['PS_PARTKEY'], ps['PS_SUPPKEY']): ps for ps in partsupp_list}

    # Prepare data for the output
    output = []
    for item in lineitem_list:
        if (item['L_PARTKEY'], item['L_SUPPKEY']) in partsupp_dict and item['L_PARTKEY'] in parts_dict:
            supplier_nation_key = suppliers_dict[item['L_SUPPKEY']]['S_NATIONKEY']
            year = datetime.strptime(item['L_SHIPDATE'], '%Y-%m-%d').year
            profit = (item['L_EXTENDEDPRICE'] * (1 - item['L_DISCOUNT'])) - (partsupp_dict[(item['L_PARTKEY'], item['L_SUPPKEY'])]['PS_SUPPLYCOST'] * item['L_QUANTITY'])
            nation = nations_dict[supplier_nation_key]['N_NAME']
            output.append((nation, year, profit))

    # Sort the output as specified
    output.sort(key=lambda x: (x[0], -x[1]))

    # Write the output to CSV
    with open('query_output.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["nation", "year", "profit"])
        for data in output:
            writer.writerow(data)

# Execute the function
product_type_profit_measure()
