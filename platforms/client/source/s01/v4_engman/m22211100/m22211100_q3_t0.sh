#!/bin/bash

# Update package list and install Python package manager pip
sudo apt update
sudo apt install python3-pip -y

# Install the necessary Python libraries
pip3 install pymysql
pip3 install pymongo
