#!/bin/bash

# install_dependencies.sh

# Update the package list
apt-get update

# Install Python and pip if they're not already present
apt-get install -y python3 python3-pip

# Install dependencies using pip
pip3 install pymysql pandas direct-redis
