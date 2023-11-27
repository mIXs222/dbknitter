#!/bin/bash

echo 'Updating system packages'
sudo apt-get update

echo 'Installing python3 pip'
sudo apt-get install python3-pip -y

echo 'Installing python MySQL client'
sudo pip3 install mysql-connector-python

echo 'Installing python MongoDB client'
sudo pip3 install pymongo

echo 'Installing pandas'
sudo pip3 install pandas

echo 'Finished installing dependencies'
