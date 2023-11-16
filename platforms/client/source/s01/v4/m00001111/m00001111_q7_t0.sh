#!/bin/bash

# Update repositories and install Python and pip if not already installed
sudo apt update
sudo apt install python3 python3-pip -y

# Install required Python libraries
pip3 install pymysql pymongo
