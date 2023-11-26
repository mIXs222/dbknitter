#!/bin/bash
# Update system and ensure that pip is installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install PyMySQL
pip3 install pymongo
pip3 install pandas
pip3 install sqlalchemy
pip3 install direct_redis
