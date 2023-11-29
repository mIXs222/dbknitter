#!/bin/bash

# Update repositories and install Python and pip if needed
sudo apt-get update
sudo apt install -y python3 python3-pip

# Install Python libraries
pip3 install pymysql
pip3 install pymongo
pip3 install pandas
pip3 install direct-redis
