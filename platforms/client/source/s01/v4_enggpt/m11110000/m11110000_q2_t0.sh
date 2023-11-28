#!/bin/bash

# Install Python and Pip if they are not installed
sudo apt-get update
sudo apt-get install python3
sudo apt-get install python3-pip

# Install the required Python libraries
pip3 install pymysql
pip3 install pymongo
