#!/bin/bash

# Updating package lists
apt-get update

# Installing Python and PIP
apt-get install -y python3 python3-pip

# Installing necessary Python libraries
pip3 install pymysql pymongo
