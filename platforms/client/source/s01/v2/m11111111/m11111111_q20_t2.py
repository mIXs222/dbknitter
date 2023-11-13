import csv
import pymongo
from pymongo import MongoClient
import pandas as pd

client = MongoClient('mongodb', 27017)
db = client['tpch']

nation = pd.DataFrame(list(db.nation.find({}, {'_id': 0})))
supplier = pd.DataFrame(list(db.supplier.find({}, {'_id': 0})))
part = pd.DataFrame(list(db.part.find({}, {'_id': 0})))
partsupp = pd.DataFrame(list(db.partsupp.find({}, {'_id': 0})))
lineitem = pd.DataFrame(list(db.lineitem.find({}, {'_id': 0})))

part = part[part['P_NAME'].str.startswith('forest')]

partsupp = partsupp[partsupp['PS_PARTKEY'].isin(part['P_PARTKEY'])]

half_quantity = lineitem[
    (lineitem['L_PARTKEY'].isin(partsupp['PS_PARTKEY'])) &
    (lineitem['L_SUPPKEY'].isin(partsupp['PS_SUPPKEY'])) &
    (lineitem['L_SHIPDATE'] >= '1994-01-01') &
    (lineitem['L_SHIPDATE'] < '1995-01-01')
]['L_QUANTITY'].sum() * 0.5

partsupp = partsupp[partsupp['PS_AVAILQTY'] > half_quantity]

supplier = supplier[
    supplier['S_SUPPKEY'].isin(partsupp['PS_SUPPKEY']) &
    (supplier['S_NATIONKEY'] == nation['N_NATIONKEY']) &
    (nation['N_NAME'] == 'CANADA')
][['S_NAME', 'S_ADDRESS']].sort_values('S_NAME')

supplier.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
