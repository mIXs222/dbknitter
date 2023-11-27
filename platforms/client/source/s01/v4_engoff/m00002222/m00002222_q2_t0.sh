#!/bin/bash

# Update package list and install pip for Python3
sudo apt update
sudo apt install python3-pip -y

# Install the required Python libraries
pip3 install pandas pymysql sqlalchemy direct-redis
