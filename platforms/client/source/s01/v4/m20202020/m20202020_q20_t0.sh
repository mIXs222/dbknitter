#!/bin/bash

# Updating the package lists for upgrades for packages that need upgrading, as well as new package installations.
sudo apt update && sudo apt upgrade -y

# Installing python3 and pip if they are not installed
sudo apt install python3 python3-pip -y

# Upgrade pip to the latest version
pip3 install --upgrade pip

# Installing required Python libraries
pip3 install pymysql pandas redis direct_redis
