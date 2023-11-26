#!/bin/bash

# install_dependencies.sh

# Update package list and install pip if needed
apt-get update
apt-get install -y python3-pip

# Install python dependencies
pip install pymysql pandas redis direct_redis
