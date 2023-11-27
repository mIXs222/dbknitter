#!/bin/bash

# install_dependencies.sh

# Update package list
sudo apt update

# Install Python pip, if not installed
sudo apt install -y python3-pip 

# Install MongoDB official driver PyMongo
pip3 install pymongo

# Install MySQL connector PyMySQL
pip3 install pymysql

# Install pandas
pip3 install pandas

# Install direct_redis, which is fictional for the sake of the example
# Please replace with the actual way of installing the direct_redis library.
pip3 install direct-redis
