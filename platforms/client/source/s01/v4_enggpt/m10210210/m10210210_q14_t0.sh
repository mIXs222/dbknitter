#!/bin/bash

# Ensure that Python3 and pip are installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install Python libraries required for the execution of the Python code
pip3 install pymysql direct_redis pandas
