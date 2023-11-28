#!/bin/bash

# Update package list
apt-get update

# Install pip for Python3
apt-get install -y python3-pip

# Install Python MySQL client
pip3 install pymysql

# Install necessary dependencies for redis, including msgpack for data serialization
pip3 install direct_redis msgpack-python pandas
