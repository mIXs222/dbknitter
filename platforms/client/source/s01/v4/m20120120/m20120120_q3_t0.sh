#!/bin/bash

# Update package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pymysql==1.0.2
pip3 install pymongo==4.0
pip3 install pandas==1.4.1
pip3 install direct-redis==1.0.0
