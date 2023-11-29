#!/bin/bash

# Install Python and Pip
sudo apt-get update
sudo apt-get install python3
sudo apt-get install python3-pip

# Install required Python packages
pip3 install pymysql
pip3 install pandas
pip3 install direct-redis
