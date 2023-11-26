#!/bin/bash

# Update package list and install Python3, pip and the required Python libraries
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymysql using pip
pip3 install pymysql
