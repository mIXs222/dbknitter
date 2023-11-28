#!/bin/bash

# Update repository and Upgrade system
sudo apt-get update && sudo apt-get -y upgrade

# Install Python3 and pip3 if not present
sudo apt-get install -y python3 python3-pip

# Install the MySQL client
sudo apt-get install -y default-mysql-client

# Install Python MySQL connector library (pymysql)
pip3 install pymysql pandas
