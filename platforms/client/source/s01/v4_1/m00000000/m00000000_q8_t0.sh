#!/bin/bash

# Update Package List
apt-get update

# Install python
apt-get install python3.7

# install pip
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3 get-pip.py

# Install pymysql
pip install PyMySQL

# Install pymongo
pip install pymongo

#Install pandas
pip install pandas

#Install direct_redis
pip install direct_redis

