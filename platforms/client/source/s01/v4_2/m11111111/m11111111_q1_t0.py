from pymongo import MongoClient
import pandas as pd
import numpy as np
from datetime import datetime
import csv

# Establish the connection
client = MongoClient('mongodb', 27017)
db = client['tpch']
table = db['lineitem']

# Create an empty DataFrame
df = pd.DataFrame()

# Query documents from the table
for row in table.find({"L_SHIPDATE": {"$lte": datetime.strptime('1998-09-02', "%Y-%m-%d")}}):
    df = df.append(row, ignore_index=True)

# Convert the required columns to appropriate datatypes
df['L_QUANTITY'] = df['L_QUANTITY'].astype(float)
df['L_EXTENDEDPRICE'] = df['L_EXTENDEDPRICE'].astype(float)
df['L_DISCOUNT'] = df['L_DISCOUNT'].astype(float)
df['L_TAX'] = df['L_TAX'].astype(float)

# Apply the SQL operations using pandas functions
output = df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
    SUM_QTY = pd.NamedAgg(column='L_QUANTITY', aggfunc='sum'),
    SUM_BASE_PRICE = pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='sum'),
    SUM_DISC_PRICE = pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc=lambda x: sum(x * (1 - df.loc[x.index, 'L_DISCOUNT']))),
    SUM_CHARGE = pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc=lambda x: sum(x * (1 - df.loc[x.index, 'L_DISCOUNT']) * (1 + df.loc[x.index, 'L_TAX']))),
    AVG_QTY = pd.NamedAgg(column='L_QUANTITY', aggfunc='mean'),
    AVG_PRICE = pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='mean'),
    AVG_DISC = pd.NamedAgg(column='L_DISCOUNT', aggfunc='mean'),
    COUNT_ORDER = pd.NamedAgg(column='L_QUANTITY', aggfunc='count'),
).reset_index()

# Sort the results
output = output.sort_values(['L_RETURNFLAG', 'L_LINESTATUS'])

# Write the output to a csv file
output.to_csv('query_output.csv')
