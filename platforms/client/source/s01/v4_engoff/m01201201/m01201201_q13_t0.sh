#!/bin/bash

# Update package list and install pip if it's not already installed
sudo apt update
sudo apt install python3-pip -y

# Install required Python packages
pip3 install pymysql
pip3 install pandas
pip3 install direct-redis
