#!/bin/bash

# bash script to install all dependencies

# Update repositories and install pip for Python 3
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql
pip3 install pymongo
pip3 install pandas
pip3 install direct-redis
