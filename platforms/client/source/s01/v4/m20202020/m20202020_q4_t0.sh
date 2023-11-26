#!/bin/bash

# Update package list and upgrade packages
sudo apt-get update
sudo apt-get -y upgrade

# Install pip if it's not already installed
sudo apt-get -y install python3-pip

# Install the required Python packages
pip3 install pymysql pandas direct-redis
