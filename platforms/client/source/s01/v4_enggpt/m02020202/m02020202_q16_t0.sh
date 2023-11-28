#!/bin/bash

# Update the package index
sudo apt-get update

# Install pip for Python3
sudo apt-get install -y python3-pip

# Install pymysql and pandas
pip3 install pymysql pandas

# Install direct_redis (assuming this library or a similar one exists for the context)
pip3 install direct_redis
