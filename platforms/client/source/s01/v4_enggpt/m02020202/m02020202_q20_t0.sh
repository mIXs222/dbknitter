#!/bin/bash

# Install Python and PIP if not already installed
sudo apt-get update
sudo apt-get install python3
sudo apt-get install python3-pip

# Install required Python libraries
pip3 install pymysql pandas direct-redis
