#!/bin/bash

# Update package list and install pip if it's not already installed
apt-get update
apt-get install -y python3-pip

# Install python libraries
pip3 install pymysql pandas direct-redis
