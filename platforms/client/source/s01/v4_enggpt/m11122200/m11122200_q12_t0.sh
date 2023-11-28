#!/bin/bash
# Update package list and upgrade existing packages
sudo apt-get update && sudo apt-get upgrade -y

# Install python3 and pip if they are not installed
sudo apt-get install python3 python3-pip -y

# Install PyMySQL using pip
pip3 install pymysql
