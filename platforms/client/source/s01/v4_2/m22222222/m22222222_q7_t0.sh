#!/bin/bash

# Update the system
apt-get update -y
apt-get upgrade -y

# Install Python and Pip
apt-get install python3.8 -y
apt-get install python3-pip -y

# Install necessary libraries
pip3 install pandas
pip3 install redis
pip3 install python-dateutil
