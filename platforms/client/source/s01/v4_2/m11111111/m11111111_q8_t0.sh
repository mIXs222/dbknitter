#!/bin/bash

# Updating and upgrading
apt-get update
apt-get upgrade -y

# Installing pip for python3
apt-get install python3-pip -y 

# Installing necessary python packages
pip3 install pymongo
pip3 install pandas

# Create a non-root user
adduser --disabled-password --gecos "" mongodb_user

# Adding permissions
chown mongodb_user:mongodb_user /home/mongodb_user
chmod 755 /home/mongodb_user
