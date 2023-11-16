#!/bin/bash

# Update and install Python3 and pip if not present
sudo apt-get update
sudo apt-get install python3 python3-pip -y

# Install required Python libraries
pip3 install pymysql pandas redis
