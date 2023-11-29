#!/bin/bash

# Update repositories
sudo apt-get update

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pymysql pandas
pip3 install direct-redis
