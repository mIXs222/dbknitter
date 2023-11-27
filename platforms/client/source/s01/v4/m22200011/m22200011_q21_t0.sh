#!/bin/bash

# Update the package list
apt-get update

# Install Python and Pip
apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pymysql
pip3 install pymongo
pip3 install pandas
pip3 install direct-redis
