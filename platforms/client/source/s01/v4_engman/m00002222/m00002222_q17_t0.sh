#!/bin/bash

# Updating package lists
sudo apt-get update

# Install Python 3.x and Pip if it's not already installed.
sudo apt-get install -y python3 python3-pip

# Install Python MySQL driver
pip3 install pymysql

# Install Python Redis driver and pandas
pip3 install direct_redis pandas
