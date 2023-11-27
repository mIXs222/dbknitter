#!/bin/bash

# Update packages list
sudo apt update

# Install Python3 and pip
sudo apt install python3 python3-pip -y

# Install the required Python packages
pip3 install pymysql pandas direct-redis
