#!/bin/bash

# Update system
sudo apt-get update

# Install python3 and pip3
sudo apt-get install python3.8
sudo apt-get install python3-pip

# Install needed python libraries
pip3 install pymongo
pip3 install pymysql
pip3 install pandas
pip3 install direct_redis
