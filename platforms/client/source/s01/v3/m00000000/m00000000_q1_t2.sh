#!/bin/bash

# Step 1) Install pip - if not installed
# Guides: https://pip.pypa.io/en/stable/installing/
# Uncomment the below lines to install pip via bash.

# curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
# python get-pip.py

# Step 2) Install python libraries 'mysql.connector' via pip
pip install mysql-connector-python

# Step 3) Run our Python script
python mysql_query.py
