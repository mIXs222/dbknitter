#!/bin/bash

# Updating package lists
apt-get update

# Install Python pip if not present
apt-get install -y python3-pip

# Install MySQL client
apt-get install -y default-mysql-client

# Install pymongo with pip
pip3 install pymongo

# Install PyMySQL with pip
pip3 install pymysql
