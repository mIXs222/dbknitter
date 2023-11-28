#!/bin/bash

# Update package list and upgrade existing packages
sudo apt-get update && sudo apt-get upgrade -y

# Install Python and pip
sudo apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymysql pandas direct-redis
