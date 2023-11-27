#!/bin/bash

# Update package manager
apt-get update

# Install pip if not already installed
apt-get install -y python3-pip

# Install pymysql
pip3 install pymysql
