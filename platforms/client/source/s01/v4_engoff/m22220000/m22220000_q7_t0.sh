#!/bin/bash

# Update package lists for upgrades and new package installs
sudo apt update -y

# Install pip and Python development packages if not already installed
sudo apt install python3-pip python3-dev -y

# Install Python packages
pip3 install pymysql pandas sqlalchemy

# Install redis-py and direct_redis (may need to be provided separately)
pip3 install redis direct_redis
