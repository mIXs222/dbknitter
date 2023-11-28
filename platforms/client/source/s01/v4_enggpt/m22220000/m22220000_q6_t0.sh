#!/bin/bash
# Ensuring the system package list is updated and all basic dependencies are installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql
