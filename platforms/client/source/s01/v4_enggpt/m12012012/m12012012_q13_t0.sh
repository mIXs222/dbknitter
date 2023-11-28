#!/bin/bash

# Update package lists
apt-get update

# Install python3 and pip
apt-get install -y python3 python3-pip

# Install the required python modules
pip3 install pymysql pymongo pandas
