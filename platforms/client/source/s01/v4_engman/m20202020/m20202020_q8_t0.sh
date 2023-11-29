#!/bin/bash

# Update and install pip if not available
apt-get update
apt-get install -y python3-pip

# Install pymysql and pandas
pip3 install pymysql pandas redis direct-redis
