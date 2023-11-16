#!/bin/bash
# install_dependencies.sh

# Update the package list
apt-get update

# Install python pip if it's not already installed
apt-get install -y python3-pip

# Install the required Python libraries
pip3 install pymysql pandas direct_redis
