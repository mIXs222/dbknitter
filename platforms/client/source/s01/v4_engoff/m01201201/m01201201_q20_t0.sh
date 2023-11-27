#!/bin/bash

# Update package list
sudo apt-get update

# Install MySQL client
sudo apt-get install -y default-mysql-client

# Install Python3 and pip
sudo apt-get install -y python3 python3-pip 

# Install the required Python libraries
pip3 install pymysql pymongo pandas
