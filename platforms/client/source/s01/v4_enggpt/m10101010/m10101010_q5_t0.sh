#!/bin/bash

# Update package lists
sudo apt update

# Install pip and necessary system libraries for Python and MySQL
sudo apt install -y python3-pip python3-dev default-libmysqlclient-dev build-essential

# Install pymongo and pymysql through pip
pip3 install pymongo pymysql
