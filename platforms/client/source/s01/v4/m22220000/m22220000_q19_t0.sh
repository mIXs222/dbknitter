#!/bin/bash

# Update repository information
sudo apt-get update

# Install python3 and pip3
sudo apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pandas direct-redis
