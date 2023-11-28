#!/bin/bash

# install_dependencies.sh

# Update the package list
sudo apt-get update

# Install Python and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pandas redis direct-redis
