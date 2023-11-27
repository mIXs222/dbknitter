#!/bin/bash

# Install python and pip if not already installed
apt-get update
apt-get install -y python3 python3-pip

# Install necessary Python libraries
pip3 install pymysql pandas direct-redis
