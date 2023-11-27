#!/bin/bash

# Update package list and upgrade existing packages
sudo apt-get update
sudo apt-get -y upgrade

# Install python3 and pip3 if not already installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Install pymysql and pandas
pip3 install pymysql pandas

# Install direct_redis
pip3 install direct_redis
