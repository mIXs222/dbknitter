#!/bin/bash

# Update package list and install Python3 pip if it's not already installed
sudo apt-get update && sudo apt-get install -y python3-pip

# Installing necessary Python libraries
pip3 install pymysql pandas direct-redis
