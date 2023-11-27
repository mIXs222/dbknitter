#!/bin/bash
# update system package list and upgrade all packages
sudo apt-get update
sudo apt-get upgrade

# install pip if not installed
sudo apt-get install python3-pip

# install pandas
pip install pandas

# install direct_redis
pip install git+https://github.com/seomoz/qds.git
pip install git+https://github.com/seomoz/direct_redis.git

# run python script
python script.py
