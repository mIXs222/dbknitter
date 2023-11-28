#!/bin/bash

# Update repositories and install pip if it's not installed
sudo apt update
sudo apt install python3-pip -y

# Install required Python libraries
pip3 install pymysql pandas redis direct_redis
