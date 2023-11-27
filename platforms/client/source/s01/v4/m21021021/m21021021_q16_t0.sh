#!/bin/bash

# Update and upgrade system packages
sudo apt-get update
sudo apt-get upgrade -y

# Install python3 and pip if they are not already installed
sudo apt-get install -y python3-pip

# Install the required Python libraries
pip3 install pandas pymysql pymongo direct-redis
