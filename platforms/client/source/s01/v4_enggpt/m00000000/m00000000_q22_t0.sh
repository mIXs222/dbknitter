#!/bin/bash

# Update package list
apt-get update

# Install pip if not available
apt-get install -y python3-pip

# Install pymysql using pip
pip3 install pymysql
