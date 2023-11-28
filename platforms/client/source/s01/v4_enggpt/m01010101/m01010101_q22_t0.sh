#!/bin/bash

# Bash script to install dependencies

# Update system and install Python pip if not installed
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas
