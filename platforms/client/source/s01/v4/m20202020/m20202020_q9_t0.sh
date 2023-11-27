#!/bin/bash

# Update package listings
apt-get update

# Install MySQL client and pandas dependencies
apt-get install -y default-libmysqlclient-dev build-essential python3-dev

# Install pip if not available
which pip3 > /dev/null || curl https://bootstrap.pypa.io/get-pip.py | python3

# Install Python dependencies
pip3 install pymysql pandas sqlalchemy direct-redis msgpack-python
