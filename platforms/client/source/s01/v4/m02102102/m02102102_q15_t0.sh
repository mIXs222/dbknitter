#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python and pip if they are not installed
sudo apt-get install python3 python3-pip -y

# Install the required Python libraries
pip3 install pymysql pandas direct-redis
