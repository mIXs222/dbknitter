#!/bin/bash
# install_dependencies.sh

# Update your package list
apt-get update

# Install Python and pip if not already installed
apt-get install -y python3 python3-pip

# Install Redis and MongoDB clients, as well as directly accessible Redis in Python
pip3 install pandas pymysql pymongo direct-redis
