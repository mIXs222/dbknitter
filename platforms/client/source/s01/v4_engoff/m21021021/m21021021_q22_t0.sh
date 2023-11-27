#!/bin/bash
# install_dependencies.sh

# Update package lists
apt-get update

# Install python3 and pip if they are not installed
apt-get install -y python3 python3-pip

# Install required python packages
pip3 install pymysql pandas direct-redis
