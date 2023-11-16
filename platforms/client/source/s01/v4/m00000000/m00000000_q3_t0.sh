#!/bin/bash

# Update the package lists
sudo apt-get update

# Install Python and pip prerequisites
sudo apt-get install -y python3 python3-pip

# Install the 'pymysql' library
pip3 install pymysql
