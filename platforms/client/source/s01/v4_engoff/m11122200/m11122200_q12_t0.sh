#!/bin/bash

# Install Python and pip if they are not already installed
sudo apt update
sudo apt install -y python3
sudo apt install -y python3-pip

# Install pymysql library
pip3 install pymysql
