#!/bin/bash

# Update package list and install Python and Pip
apt-get update
apt-get install -y python3 python3-pip

# Install Python dependencies
pip3 install pymysql pymongo pandas
