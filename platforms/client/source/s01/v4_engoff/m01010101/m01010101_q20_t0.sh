#!/bin/bash

# Update package list and upgrade system packages
sudo apt update && sudo apt upgrade -y

# Install pip and Python development package if not already installed
sudo apt install python3-pip python3-dev -y

# Install pymysql and pymongo Python libraries
pip3 install pymysql pymongo
