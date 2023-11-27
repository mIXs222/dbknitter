#!/bin/bash

# to install python 3
sudo apt-get update
sudo apt-get -y upgrade
sudo apt-get install -y python3-pip

# to install pandas
pip3 install pandas 

# to install direct_redis
pip3 install direct_redis

# to install dateutil for pandas datetime
pip3 install python-dateutil

# to install csv module
pip3 install python-csv
