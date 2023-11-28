#!/bin/bash

# install_dependencies.sh

# Install Python and pip if they are not installed
sudo apt-get update
sudo apt-get install python3
sudo apt-get install python3-pip

# Install Python libraries
pip3 install pandas pymysql redis direct-redis
