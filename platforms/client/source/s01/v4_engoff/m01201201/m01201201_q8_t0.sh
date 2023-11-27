#!/bin/bash

# Update repository and install pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql
pip3 install pymongo
pip3 install pandas
pip3 install direct_redis
