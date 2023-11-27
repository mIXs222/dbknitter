#!/bin/bash
# install_dependencies.sh

# Update the package list
apt-get update -y

# Install Python and Pip if they are not installed
apt-get install -y python3-pip

# Install the required Python packages
pip3 install pymysql pandas redis direct-redis
