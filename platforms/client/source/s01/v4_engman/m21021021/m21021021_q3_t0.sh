#!/bin/bash

# Install Python's package manager pip if not already installed
which pip || curl https://bootstrap.pypa.io/get-pip.py | python

# Install pymysql for MySQL connections
pip install pymysql

# Install pymongo for connecting to MongoDB
pip install pymongo

# Install pandas for DataFrame operations
pip install pandas

# Install direct_redis for Redis connections
pip install direct_redis
